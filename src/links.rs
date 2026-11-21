//! Linked IDs (rules.md "Linked IDs") and person numbers.
//!
//! One shared open-addressing table holds every user ID. A person is numbered by
//! the table slot of their name: the byte-wise smallest ID of their connected
//! component, or the ID itself if no link line mentions it.
//!
//! The link files are read in one parallel pass: each thread interns both IDs of a
//! line and joins them in a lock-free union-find whose roots are always the
//! smallest name of their set. A second pass writes each entry's person beside its
//! ID, so a lookup from an event touches one cache line. IDs that no link mentions
//! are added when an event or click first uses them.

use crate::id::Id;
use crate::mem;
use crate::scan::{unquote, Reader};
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::*};

#[repr(C, align(32))]
struct Entry {
    lo: AtomicU64,
    hi: AtomicU64, // 0 = empty, BUSY = being written
    person: AtomicU32,
    _pad: [u32; 3],
}

/// No ID has this high word: its length byte would be 0xFE.
const BUSY: u64 = 0xFE << 56;

pub struct Links {
    tab: *const Entry,
    bytes: usize,
    mask: usize,
    shift: u32,
    /// IDs that appear in link lines.
    pub linked: usize,
    /// Whether huge pages were asked for.
    pub huge: bool,
    count: AtomicUsize,
    /// Entries past which the table counts as full.
    pub limit: usize,
    /// Set when the table is too full to go on; the run is then redone with a larger one.
    pub full: AtomicBool,
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
    pub fn slots(&self) -> usize {
        self.mask + 1
    }

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

    /// Starts loading the cache line of person `n`'s name.
    #[inline(always)]
    pub fn prefetch_name(&self, n: u32) {
        prefetch(unsafe { self.tab.add(n as usize) });
    }

    /// The ID in slot `i` and its person, if the slot is in use.
    #[inline(always)]
    pub fn linked_at(&self, i: usize) -> Option<(Id, u32)> {
        let e = unsafe { &*self.tab.add(i) };
        let hi = e.hi.load(Relaxed);
        (hi != 0 && hi != BUSY).then(|| (Id { lo: e.lo.load(Relaxed), hi }, e.person.load(Relaxed)))
    }

    /// The name of person `n`.
    #[inline(always)]
    pub fn name(&self, n: u32) -> Id {
        let e = unsafe { &*self.tab.add(n as usize) };
        Id { lo: e.lo.load(Relaxed), hi: e.hi.load(Relaxed) }
    }

    /// The person an ID belongs to. An ID no link line mentions is its own person
    /// and is added to the table here, by whichever thread sees it first.
    #[inline(always)]
    pub fn person(&self, id: Id, h: u64, added: &mut u32) -> u32 {
        let mut i = (h >> self.shift) as usize;
        loop {
            let e = unsafe { &*self.tab.add(i) };
            let mut hi = e.hi.load(Acquire);
            loop {
                if hi == id.hi {
                    if e.lo.load(Relaxed) == id.lo {
                        return e.person.load(Relaxed);
                    }
                    break;
                }
                if hi == 0 {
                    match e.hi.compare_exchange(0, BUSY, AcqRel, Acquire) {
                        Ok(_) => {
                            e.lo.store(id.lo, Relaxed);
                            e.person.store(i as u32, Relaxed);
                            e.hi.store(id.hi, Release);
                            *added += 1;
                            return i as u32;
                        }
                        Err(cur) => hi = cur,
                    }
                } else if hi == BUSY {
                    std::hint::spin_loop();
                    hi = e.hi.load(Acquire);
                } else {
                    break;
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Reports IDs a thread has added; flags the table as full past its limit.
    #[inline]
    pub fn added(&self, n: &mut u32) {
        if self.count.fetch_add(*n as usize, Relaxed) + *n as usize > self.limit {
            self.full.store(true, Relaxed);
        }
        *n = 0;
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

/// `tasks` are (file, start, end) byte ranges of the link files. `users` estimates
/// the IDs that events and clicks will add; `floor` is the smallest safe table.
#[allow(clippy::too_many_arguments)]
pub fn build(
    files: &[&File],
    tasks: &[(usize, u64, u64)],
    bytes: usize,
    users: usize,
    floor: usize,
    nthreads: usize,
    buf: usize,
    huge_free: usize,
) -> Links {
    // About 15 bytes of link file per distinct ID in practice. Sized to end up at
    // most ~70% full, and rebuilt larger if the link files alone overfill it.
    // (`users == 0` with a `floor` is a test hook: exactly `floor` slots to start with.)
    let mut slots = if users == 0 { floor } else { floor.max(bytes / 8).max((bytes / 15 + users) / 7 * 10) }.next_power_of_two();
    loop {
        if let Some(l) = try_build(files, tasks, slots, nthreads, buf, huge_free) {
            return l;
        }
        slots *= 4;
    }
}

fn try_build(files: &[&File], tasks: &[(usize, u64, u64)], slots: usize, nthreads: usize, buf: usize, huge_free: usize) -> Option<Links> {
    let t0 = std::time::Instant::now();
    // The table is read at random by every event and click: on huge pages those
    // reads stop missing the TLB and it is built without a fault per 4 KB. The
    // caller passes how much memory may be assumed free in 2 MB blocks.
    let huge = huge_free >= 2 * slots * (std::mem::size_of::<Entry>() + 4) + (256 << 20);
    let tab = mem::map(slots * std::mem::size_of::<Entry>() + (2 << 20));
    let tab = ((tab as usize + (2 << 20) - 1) & !((2 << 20) - 1)) as *mut u8;
    if huge {
        mem::hugepages(tab, slots * std::mem::size_of::<Entry>());
    }
    let tab = tab as *const Entry;
    let pbytes = slots * 4;
    let pmem = mem::map(pbytes + (2 << 20));
    let pmem = ((pmem as usize + (2 << 20) - 1) & !((2 << 20) - 1)) as *mut u8;
    if huge {
        mem::hugepages(pmem, pbytes);
    }
    let parent = unsafe { Vec::from_raw_parts(pmem as *mut AtomicU32, slots, slots) };
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
                    // Find both roots and start loading their entries (the union compares
                    // the roots' names), then join: three passes, so the cache misses of a
                    // batch overlap instead of queueing.
                    for x in s[..ids.len()].iter_mut() {
                        *x = (b.find(x.0), b.find(x.1));
                        prefetch(b.entry(x.0 as usize) as *const Entry);
                        prefetch(b.entry(x.1 as usize) as *const Entry);
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
                    rd.run(files[fi], lo, hi, |rd| unsafe {
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
    if std::env::var_os("SS_TIME").is_some() {
        eprintln!("  links: interned and joined at {:?}", t0.elapsed());
    }
    // Write every entry's person (the slot of its set's root) beside it.
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
                            pend[n] = (i as u32, r);
                            n += 1;
                        }
                        i += 1;
                    }
                    for &(k, r) in &pend[..n] {
                        b.entry(k as usize).person.store(r, Relaxed);
                    }
                }
            });
        }
    });
    if std::env::var_os("SS_TIME").is_some() {
        eprintln!("  links: persons written at {:?}", t0.elapsed());
    }
    let count = b.count.load(Relaxed);
    // The parent array is dead: give its pages back in bulk.
    mem::release(b.parent.as_ptr() as *mut u8, pbytes);
    std::mem::forget(b.parent);
    Some(Links {
        tab,
        bytes: slots * std::mem::size_of::<Entry>(),
        mask: slots - 1,
        shift: 64 - slots.trailing_zeros(),
        linked: count,
        huge,
        count: AtomicUsize::new(count),
        limit: slots / 20 * 17,
        full: AtomicBool::new(false),
    })
}

unsafe fn libc_unmap(p: *mut u8, bytes: usize) {
    #[cfg(unix)]
    libc::munmap(p as *mut libc::c_void, bytes.max(1).div_ceil(4096) * 4096);
    #[cfg(not(unix))]
    let _ = (p, bytes);
}
