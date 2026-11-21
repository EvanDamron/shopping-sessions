//! Per-thread partitioned buffers in one huge-page-backed region: partition p owns
//! slots [p * cap, (p + 1) * cap); anything past `cap` spills into a per-partition Vec.
//! One region means few page faults (2 MB pages) and few TLB entries while scattering.

pub struct Parts<T: Copy> {
    base: *mut T,
    bytes: usize,
    cap: usize, // a multiple of 8
    lens: Vec<usize>,
    over: Vec<Vec<T>>,
    // Software write-combining: 8 items per partition are staged, then written to the
    // region as whole cache lines with non-temporal stores (no read-for-ownership).
    stage: Vec<Stage<T>>,
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Stage<T: Copy>([T; 8]);

unsafe impl<T: Copy + Send> Send for Parts<T> {}
unsafe impl<T: Copy + Sync> Sync for Parts<T> {}

const HUGE: usize = 2 << 20;

impl<T: Copy> Parts<T> {
    pub fn new(nparts: usize, cap: usize) -> Self {
        assert!(std::mem::size_of::<T>() * 8 % 64 == 0, "item size must tile cache lines in groups of 8");
        let cap = cap.max(16).div_ceil(8) * 8;
        let bytes = (nparts * cap * std::mem::size_of::<T>()).div_ceil(HUGE) * HUGE;
        let base = alloc(bytes) as *mut T;
        let stage = vec![Stage(unsafe { std::mem::zeroed::<[T; 8]>() }); nparts];
        Parts { base, bytes, cap, lens: vec![0; nparts], over: (0..nparts).map(|_| Vec::new()).collect(), stage }
    }

    /// Appends to partition p and returns its index within the partition.
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) -> usize {
        let l = self.lens[p];
        self.lens[p] = l + 1;
        if l < self.cap {
            let k = l & 7;
            let st = &mut self.stage[p];
            st.0[k] = x;
            if k == 7 {
                unsafe { flush8(self.base.add(p * self.cap + l - 7) as *mut u8, st.0.as_ptr() as *const u8, std::mem::size_of::<T>() * 8) };
            }
        } else {
            self.over[p].push(x);
        }
        l
    }

    /// Writes the staged remainder of every partition. Call once after the last push,
    /// before anyone reads (pushing threads must have run `sfence` first).
    pub fn finish(&mut self) {
        for p in 0..self.lens.len() {
            let l = self.lens[p].min(self.cap);
            let k = l & 7;
            if k > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(self.stage[p].0.as_ptr(), self.base.add(p * self.cap + l - k), k);
                }
            }
        }
        sfence();
    }

    #[inline]
    pub fn len(&self, p: usize) -> usize {
        self.lens[p]
    }

    /// The partition's items: the in-region part and the spill.
    #[inline]
    pub fn slices(&self, p: usize) -> (&[T], &[T]) {
        let n = self.lens[p].min(self.cap);
        (unsafe { std::slice::from_raw_parts(self.base.add(p * self.cap), n) }, &self.over[p])
    }

    /// Raw pointer to item i of partition p (for cross-thread marking after all pushes).
    #[inline]
    pub fn ptr(&self, p: usize, i: usize) -> *mut T {
        if i < self.cap {
            unsafe { self.base.add(p * self.cap + i) }
        } else {
            unsafe { self.over[p].as_ptr().add(i - self.cap) as *mut T }
        }
    }
}

impl<T: Copy> Drop for Parts<T> {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, self.bytes);
        }
    }
}

/// Copies `n` bytes (a multiple of 64; both 64-byte aligned) with non-temporal stores on x86.
#[inline(always)]
unsafe fn flush8(dst: *mut u8, src: *const u8, n: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use core::arch::x86_64::*;
        let mut i = 0;
        while i < n {
            let v = _mm_load_si128(src.add(i) as *const __m128i);
            _mm_stream_si128(dst.add(i) as *mut __m128i, v);
            i += 16;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    std::ptr::copy_nonoverlapping(src, dst, n);
}

/// Orders earlier non-temporal stores before later stores (x86); no-op elsewhere.
#[inline]
pub fn sfence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence()
    };
}

/// Anonymous, 2 MB-aligned, lazily backed memory with huge pages requested.
pub fn alloc(bytes: usize) -> *mut u8 {
    unsafe {
        let len = bytes + HUGE;
        let p = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap of {len} bytes failed");
        let start = (p as usize).div_ceil(HUGE) * HUGE;
        #[cfg(target_os = "linux")]
        libc::madvise(start as *mut libc::c_void, bytes, libc::MADV_HUGEPAGE);
        start as *mut u8
    }
}
