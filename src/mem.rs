//! Anonymous memory: lazily backed regions and a concurrent bump allocator over one.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Zeroed, page-aligned, lazily backed memory. Never freed by drop.
pub fn map(bytes: usize) -> *mut u8 {
    let len = bytes.max(1).div_ceil(4096) * 4096;
    #[cfg(unix)]
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap of {len} bytes failed");
        p as *mut u8
    }
    #[cfg(not(unix))]
    {
        let v = vec![0u8; len];
        Box::leak(v.into_boxed_slice()).as_mut_ptr()
    }
}

/// Asks for huge pages on a range (Linux, opt-in).
pub fn hugepages(p: *mut u8, bytes: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(p as *mut libc::c_void, bytes, libc::MADV_HUGEPAGE);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (p, bytes);
}

/// Returns a range's pages to the kernel now (so exit does not have to, serially).
pub fn release(p: *mut u8, bytes: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(p as *mut libc::c_void, bytes, libc::MADV_DONTNEED);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (p, bytes);
}

/// Gives the whole pages of a vector that is never touched again back to the kernel
/// and leaks it, instead of freeing it: threads that free big blocks at once queue
/// on the address-space lock (munmap takes it to write), while madvise only reads it.
pub fn release_vec<T>(v: Vec<T>) {
    let a = v.as_ptr() as usize;
    let b = a + v.capacity() * std::mem::size_of::<T>();
    let (a, b) = ((a + 4095) & !4095, b & !4095);
    if b > a {
        release(a as *mut u8, b - a);
    }
    std::mem::forget(v);
}

/// Faults a range in with one call instead of one trap per page (Linux 5.14+;
/// elsewhere, and on older kernels, the pages are simply faulted on first touch).
pub fn populate(p: *mut u8, bytes: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(p as *mut libc::c_void, bytes, 23 /* MADV_POPULATE_WRITE */);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (p, bytes);
}

/// Copies `lines` cache lines from `src` to `dst` (both 64-byte aligned) with
/// streaming stores: the destination is written without first being read.
#[inline(always)]
pub unsafe fn stream(dst: *mut u8, src: *const u8, lines: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    {
        use core::arch::x86_64::*;
        for i in 0..lines * 2 {
            _mm256_stream_si256(dst.add(32 * i) as *mut __m256i, _mm256_load_si256(src.add(32 * i) as *const __m256i));
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
    std::ptr::copy_nonoverlapping(src, dst, lines * 64);
}

/// Makes streaming stores visible to other threads' loads.
#[inline(always)]
pub fn stream_done() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence()
    }
}

#[repr(align(64))]
struct Padded(AtomicUsize);

/// Bytes of free memory the kernel holds in blocks of 2 MB or more (Linux), read
/// from /proc/buddyinfo; None if that cannot be read. Huge pages are only worth
/// asking for when they are there for the taking: when they are not, each huge
/// fault stalls in compaction for far longer than the small faults it replaces.
pub fn free_huge_bytes() -> Option<usize> {
    let s = std::fs::read_to_string("/proc/buddyinfo").ok()?;
    let mut total = 0usize;
    for line in s.lines() {
        // "Node 0, zone   Normal  c0 c1 ... c10": counts of free blocks of 4 KB << order.
        let counts: Vec<usize> = line.split_whitespace().skip(4).filter_map(|x| x.parse().ok()).collect();
        for (order, c) in counts.iter().enumerate().skip(9) {
            total += c * (4096usize << order);
        }
    }
    Some(total)
}

/// A bump allocator that hands out fixed-size chunks to many threads. The first
/// region is sized from an estimate; if it runs out, further regions are mapped.
/// With `ahead` set, memory is faulted in a block at a time, a block ahead of use.
#[repr(align(64))]
pub struct Arena {
    base: *mut u8,
    cap: usize,
    ahead: usize, // 0 = off
    next: Padded,
    extra: std::sync::Mutex<(*mut u8, usize, usize)>, // overflow region: base, cap, used
}
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    pub fn new(bytes: usize, huge: bool, ahead: usize) -> Arena {
        let cap = bytes.max(1 << 16).div_ceil(1 << 21) << 21;
        let base = map(cap);
        if huge {
            hugepages(base, cap);
        }
        if ahead > 0 {
            populate(base, ahead.min(cap));
        }
        Arena { base, cap, ahead, next: Padded(AtomicUsize::new(0)), extra: std::sync::Mutex::new((std::ptr::null_mut(), 0, 0)) }
    }
    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.base
    }
    /// Returns part `k` of `n` of the used memory to the kernel. Threads call this
    /// in parallel once the records are dead, so that process exit, which tears
    /// memory down on one thread while the clock runs, has little left to do.
    pub fn release_part(&self, k: usize, n: usize) {
        let used = (self.next.0.load(Relaxed).min(self.cap) + self.ahead * 2).min(self.cap).div_ceil(4096) * 4096;
        let lo = (used / n * k) & !4095;
        let hi = if k + 1 == n { used } else { (used / n * (k + 1)) & !4095 };
        if hi > lo {
            release(unsafe { self.base.add(lo) }, hi - lo);
        }
    }
    #[inline(always)]
    fn bump(&self, bytes: usize) -> usize {
        let o = self.next.0.fetch_add(bytes, Relaxed);
        if self.ahead > 0 && (o + bytes) / self.ahead != o / self.ahead {
            // Crossed into a new block: fault in the one after it.
            let at = ((o + bytes) / self.ahead + 1) * self.ahead;
            if at < self.cap {
                populate(unsafe { self.base.add(at) }, self.ahead.min(self.cap - at));
            }
        }
        o
    }
    /// A chunk of `bytes` (64-byte aligned when `bytes` is a multiple of 64).
    #[inline]
    pub fn alloc(&self, bytes: usize) -> *mut u8 {
        let o = self.bump(bytes);
        if o + bytes <= self.cap {
            return unsafe { self.base.add(o) };
        }
        self.alloc_slow(bytes)
    }
    /// A chunk from the first region only, as an offset; panics if the region is full.
    #[inline]
    pub fn alloc_offset(&self, bytes: usize) -> usize {
        let o = self.bump(bytes);
        assert!(o + bytes <= self.cap, "arena overflow");
        o
    }
    #[cold]
    fn alloc_slow(&self, bytes: usize) -> *mut u8 {
        let mut g = self.extra.lock().unwrap();
        if g.0.is_null() || g.2 + bytes > g.1 {
            let cap = (self.cap / 2).max(bytes).max(64 << 20).div_ceil(1 << 21) << 21;
            *g = (map(cap), cap, 0);
        }
        let p = unsafe { g.0.add(g.2) };
        g.2 += bytes;
        p
    }
}
