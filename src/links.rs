//! Linked IDs (rules.md "Linked IDs"): every ID that appears in a link line, mapped
//! to its person's name, the byte-wise smallest ID of its connected component.
//!
//! One pass over the link files, in parallel: each thread interns both IDs of a line
//! in a shared open-addressing table and joins them in a lock-free union-find whose
//! roots are always the smallest name of their set. A second pass writes each entry's
//! person next to its ID, so a lookup from an event touches one cache line.

use crate::id::Id;
use crate::mem;
use crate::scan::{unquote, Reader};
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::*};

#[repr(C, align(32))]
struct Entry {
    lo: AtomicU64,
    hi: AtomicU64, // 0 = empty, BUSY = being written
    plo: AtomicU64,
    phi: AtomicU64,
}

/// No ID has this high word: its length byte would be 0xFE.
const BUSY: u64 = 0xFE << 56;

pub struct Links {
    tab: *const Entry,
    bytes: usize,
    mask: usize,
    shift: u32,
    pub count: usize,
}
unsafe impl Send for Links {}
unsafe impl Sync for Links {}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}

impl Links {
    /// Returns part `k` of `n` of the table to the kernel (see `Arena::release_part`).
    pub fn release_part(&self, k: usize, n: usize) {
        let lo = (self.bytes / n * k) & !4095;
        let hi = if k + 1 == n { self.bytes } else { (self.bytes / n * (k + 1)) & !4095 };
        if hi > lo {
            mem::release(unsafe { (self.tab as *mut u8).add(lo) }, hi - lo);
        }
    }

    /// Starts loading the cache line a lookup of an ID with hash `h` will read.
    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        prefetch(unsafe { self.tab.add((h >> self.shift) as usize) });
    }

    /// The person an ID belongs to: its component's name, or the ID itself if no
    /// link line mentions it.
    #[inline(always)]
    pub fn person(&self, id: Id, h: u64) -> Id {
        let mut i = (h >> self.shift) as usize;
        loop {
            let e = unsafe { &*self.tab.add(i) };
            let hi = e.hi.load(Relaxed);
            if hi == 0 {
                return id;
            }
            if hi == id.hi && e.lo.load(Relaxed) == id.lo {
                return Id { lo: e.plo.load(Relaxed), hi: e.phi.load(Relaxed) };
            }
            i = (i + 1) & self.mask;
        }
    }
}

struct Build {
    tab: *const Entry,
    parent: Vec<AtomicU32>,
    mask: usize,
    shift: u32,
    count: AtomicUsize,
    limit: usize,
    full: AtomicBool,
}
unsafe impl Sync for Build {}

impl Build {
    #[inline(always)]
    fn entry(&self, i: usize) -> &Entry {
        unsafe { &*self.tab.add(i) }
    }

    /// The slot of `id`, inserting it if absent.
    #[inline(always)]
    fn intern(&self, id: Id, h: u64, added: &mut usize) -> u32 {
        let mut i = (h >> self.shift) as usize;
        loop {
            let e = self.entry(i);
            let mut hi = e.hi.load(Acquire);
            if hi == 0 {
                match e.hi.compare_exchange(0, BUSY, AcqRel, Acquire) {
                    Ok(_) => {
                        e.lo.store(id.lo, Relaxed);
                        e.hi.store(id.hi, Release);
                        *added += 1;
                        return i as u32;
                    }
                    Err(cur) => hi = cur,
                }
            }
            while hi == BUSY {
                std::hint::spin_loop();
                hi = e.hi.load(Acquire);
            }
            if hi == id.hi && e.lo.load(Relaxed) == id.lo {
                return i as u32;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn id(&self, i: u32) -> Id {
        let e = self.entry(i as usize);
        Id { lo: e.lo.load(Relaxed), hi: e.hi.load(Relaxed) }
    }

    /// A slot's parent is stored plus one, so fresh (zero) memory means "a root".
    #[inline(always)]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let pe = self.parent[x as usize].load(Relaxed);
            if pe == 0 {
                return x;
            }
            let p = pe - 1;
            let gpe = self.parent[p as usize].load(Relaxed);
            if gpe == 0 {
                return p;
            }
            let _ = self.parent[x as usize].compare_exchange_weak(pe, gpe, Relaxed, Relaxed);
            x = gpe - 1;
        }
    }

    /// Joins two sets; the root with the smaller name stays the root.
    #[inline(always)]
    fn union(&self, mut a: u32, mut b: u32) {
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            if self.id(a).less(&self.id(b)) {
                std::mem::swap(&mut a, &mut b);
            }
            // a has the larger name: hang it under b, if it is still a root.
            if self.parent[a as usize].compare_exchange(0, b + 1, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    }
}

const BATCH: usize = 16;

/// `tasks` are (file, start, end) byte ranges of the link files.
pub fn build(files: &[File], tasks: &[(usize, u64, u64)], bytes: usize, nthreads: usize, buf: usize, huge: bool) -> Links {
    // About 15 bytes of link file per distinct ID in practice; sized for 8, and
    // rebuilt larger in the (pathological) case that the table fills up.
    let mut slots = (bytes / 8).max(1 << 16).next_power_of_two();
    loop {
        if let Some(l) = try_build(files, tasks, slots, nthreads, buf, huge) {
            return l;
        }
        slots *= 4;
    }
}

fn try_build(files: &[File], tasks: &[(usize, u64, u64)], slots: usize, nthreads: usize, buf: usize, huge: bool) -> Option<Links> {
    let tab = mem::map(slots * std::mem::size_of::<Entry>() + (2 << 20));
    let tab = ((tab as usize + (2 << 20) - 1) & !((2 << 20) - 1)) as *mut u8;
    if huge {
        mem::hugepages(tab, slots * std::mem::size_of::<Entry>());
    }
    let tab = tab as *const Entry;
    let pbytes = slots * 4;
    let parent = unsafe { Vec::from_raw_parts(mem::map(pbytes) as *mut AtomicU32, slots, slots) };
    let b = Build {
        tab,
        parent,
        mask: slots - 1,
        shift: 64 - slots.trailing_zeros(),
        count: AtomicUsize::new(0),
        limit: slots / 4 * 3,
        full: AtomicBool::new(false),
    };
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for _ in 0..nthreads.min(tasks.len().max(1)) {
            let (b, next) = (&b, &next);
            sc.spawn(move || {
                let mut rd = Reader::new(buf);
                let mut ids = [(Id::EMPTY, Id::EMPTY, 0u64, 0u64); BATCH];
                let mut n = 0usize;
                let mut added = 0usize;
                let mut dead = false;
                // Report insertions often enough that the table cannot fill between checks.
                let every = (b.mask + 1) / (8 * nthreads).max(1);
                let flush = |ids: &[(Id, Id, u64, u64)], added: &mut usize, dead: &mut bool| {
                    let mut s = [(0u32, 0u32); BATCH];
                    for (k, &(ia, ib, ha, hb)) in ids.iter().enumerate() {
                        let a = b.intern(ia, ha, added);
                        let c = b.intern(ib, hb, added);
                        s[k] = (a, c);
                        prefetch(&b.parent[a as usize]);
                        prefetch(&b.parent[c as usize]);
                    }
                    for &(a, c) in &s[..ids.len()] {
                        b.union(a, c);
                    }
                    if *added >= every {
                        if b.count.fetch_add(*added, Relaxed) + *added > b.limit {
                            b.full.store(true, Relaxed);
                        }
                        *added = 0;
                        *dead = b.full.load(Relaxed);
                    }
                };
                loop {
                    let t = next.fetch_add(1, Relaxed);
                    if t >= tasks.len() || dead || b.full.load(Relaxed) {
                        break;
                    }
                    let (fi, lo, hi) = tasks[t];
                    rd.run(&files[fi], lo, hi, |rd| unsafe {
                        let d = rd.data();
                        rd.lines::<2>(|ip, ls| {
                            if dead {
                                return;
                            }
                            let p0 = *ip as usize;
                            let (s0, e0) = unquote(d, ls, p0);
                            let (s1, e1) = unquote(d, p0 + 1, *ip.add(1) as usize);
                            if s0 == e0 || s1 == e1 {
                                return;
                            }
                            let (ia, ib) = (Id::field(d, s0, e0), Id::field(d, s1, e1));
                            let (ha, hb) = (ia.hash(), ib.hash());
                            prefetch(b.tab.add((ha >> b.shift) as usize));
                            prefetch(b.tab.add((hb >> b.shift) as usize));
                            ids[n] = (ia, ib, ha, hb);
                            n += 1;
                            if n == BATCH {
                                flush(&ids, &mut added, &mut dead);
                                n = 0;
                            }
                        });
                    });
                }
                if !dead {
                    flush(&ids[..n], &mut added, &mut dead);
                }
                if b.count.fetch_add(added, Relaxed) + added > b.limit {
                    b.full.store(true, Relaxed);
                }
            });
        }
    });
    if b.full.load(Relaxed) || b.count.load(Relaxed) > b.limit {
        unsafe {
            libc_unmap(tab as *mut u8, slots * std::mem::size_of::<Entry>());
            libc_unmap(b.parent.as_ptr() as *mut u8, pbytes);
        }
        std::mem::forget(b.parent);
        return None;
    }
    // Write every entry's person beside it.
    let next = AtomicUsize::new(0);
    const STEP: usize = 1 << 14;
    std::thread::scope(|sc| {
        for _ in 0..nthreads {
            let (b, next) = (&b, &next);
            sc.spawn(move || loop {
                let lo = next.fetch_add(STEP, Relaxed);
                if lo >= slots {
                    break;
                }
                let hi = (lo + STEP).min(slots);
                let mut i = lo;
                let mut pend = [(0u32, 0u32); BATCH];
                while i < hi {
                    let mut n = 0;
                    while i < hi && n < BATCH {
                        if b.entry(i).hi.load(Relaxed) != 0 {
                            let r = b.find(i as u32);
                            prefetch(b.entry(r as usize) as *const Entry);
                            pend[n] = (i as u32, r);
                            n += 1;
                        }
                        i += 1;
                    }
                    for &(k, r) in &pend[..n] {
                        let p = b.id(r);
                        let e = b.entry(k as usize);
                        e.plo.store(p.lo, Relaxed);
                        e.phi.store(p.hi, Relaxed);
                    }
                }
            });
        }
    });
    let count = b.count.load(Relaxed);
    // The parent array is dead: give its pages back in bulk.
    mem::release(b.parent.as_ptr() as *mut u8, pbytes);
    std::mem::forget(b.parent);
    Some(Links { tab, bytes: slots * std::mem::size_of::<Entry>(), mask: slots - 1, shift: 64 - slots.trailing_zeros(), count })
}

unsafe fn libc_unmap(p: *mut u8, bytes: usize) {
    #[cfg(unix)]
    libc::munmap(p as *mut libc::c_void, bytes.max(1).div_ceil(4096) * 4096);
    #[cfg(not(unix))]
    let _ = (p, bytes);
}
