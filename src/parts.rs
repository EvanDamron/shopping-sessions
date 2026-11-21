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

/// Every large mapping (arenas, link table, input files), so they can be released in
/// parallel before exit instead of by the kernel's single-threaded teardown.
static REGIONS: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Vec::new());

pub fn register(ptr: *const u8, len: usize) {
    REGIONS.lock().unwrap().push((ptr as usize, len));
}

/// Drops the pages of every registered mapping, split across `nthreads` threads.
pub fn release_all(nthreads: usize) {
    #[cfg(target_os = "linux")]
    {
        const PIECE: usize = 8 << 20;
        let mut pieces = Vec::new();
        for &(p, l) in REGIONS.lock().unwrap().iter() {
            let start = p & !4095;
            let end = p + l;
            let mut a = start;
            while a < end {
                let b = (a + PIECE).min(end);
                pieces.push((a, b - a));
                a = b;
            }
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|sc| {
            for _ in 0..nthreads {
                sc.spawn(|| loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(&(a, l)) = pieces.get(i) else { break };
                    unsafe { libc::madvise(a as *mut libc::c_void, l, libc::MADV_DONTNEED) };
                });
            }
        });
    }
    #[cfg(not(target_os = "linux"))]
    let _ = nthreads;
}

/// Whether to ask for transparent huge pages. With little free contiguous memory the
/// kernel would compact synchronously on every fault, which costs far more than it saves.
static USE_HUGE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Free memory in blocks of 2 MB or larger, from /proc/buddyinfo (0 if unreadable).
fn free_huge_bytes() -> usize {
    let Ok(s) = std::fs::read_to_string("/proc/buddyinfo") else { return 0 };
    let mut total = 0usize;
    for line in s.lines() {
        // "Node 0, zone   Normal  c0 c1 ... c10": counts of free blocks of order 0..
        let counts: Vec<usize> = line.split_whitespace().skip(4).filter_map(|x| x.parse().ok()).collect();
        for (order, &c) in counts.iter().enumerate().skip(9) {
            total += c << (order + 12);
        }
    }
    total
}

/// Decides once, before the big allocations, whether huge pages are worth requesting.
pub fn plan_huge(need_bytes: usize) -> bool {
    let free = free_huge_bytes();
    let ok = std::env::var_os("NO_HUGE").is_none() && free >= need_bytes + need_bytes / 4;
    USE_HUGE.store(ok, std::sync::atomic::Ordering::Relaxed);
    ok
}

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
        register(start as *const u8, bytes);
        #[cfg(target_os = "linux")]
        if USE_HUGE.load(std::sync::atomic::Ordering::Relaxed) {
            libc::madvise(start as *mut libc::c_void, bytes, libc::MADV_HUGEPAGE);
        }
        start as *mut u8
    }
}
