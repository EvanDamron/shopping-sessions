//! Per-thread partitioned buffers in one huge-page-backed region: partition p owns
//! slots [p * cap, (p + 1) * cap); anything past `cap` spills into a per-partition Vec.
//! One region means few page faults (2 MB pages) and few TLB entries while scattering.

pub struct Parts<T: Copy> {
    base: *mut T,
    bytes: usize,
    cap: usize,
    lens: Vec<usize>,
    over: Vec<Vec<T>>,
}

unsafe impl<T: Copy + Send> Send for Parts<T> {}
unsafe impl<T: Copy + Sync> Sync for Parts<T> {}

const HUGE: usize = 2 << 20;

impl<T: Copy> Parts<T> {
    pub fn new(nparts: usize, cap: usize) -> Self {
        let cap = cap.max(16);
        let bytes = (nparts * cap * std::mem::size_of::<T>()).div_ceil(HUGE) * HUGE;
        let base = alloc(bytes) as *mut T;
        Parts { base, bytes, cap, lens: vec![0; nparts], over: (0..nparts).map(|_| Vec::new()).collect() }
    }

    /// Appends to partition p and returns its index within the partition.
    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) -> usize {
        let l = self.lens[p];
        self.lens[p] = l + 1;
        if l < self.cap {
            unsafe { self.base.add(p * self.cap + l).write(x) };
        } else {
            self.over[p].push(x);
        }
        l
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

/// Anonymous, 2 MB-aligned, lazily backed memory with huge pages requested.
fn alloc(bytes: usize) -> *mut u8 {
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
