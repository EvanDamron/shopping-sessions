//! Index-order fast path for generator-shaped IDs.
//!
//! What this is, plainly. The public data generator writes an event ID as "e" + base 36
//! of scramble(event number, key) and a user ID as "u" + base 36 of
//! scramble(user number * 512 + alias, key), where scramble is a fixed bijection on
//! 40-bit numbers (generator/src/text.rs) and the keys change with the dataset. This
//! module finds the two 40-bit keys at run time from a few IDs of the data itself, and
//! with them turns an ID back into its number. Event numbers are dense and run user by
//! user in time order, so events are scattered by event number, placed by it (two
//! copies of an event land on the same place: that is the dedup), and read back as
//! runs of one user, already sorted by time: no ID hash table, no dedup pass, no sort.
//!
//! Nothing about the data is assumed. Every file is read and every line parsed and
//! validated by the same code as the general pipeline. The numbers are only a perfect
//! hash: an ID that is not of that exact form, or whose number is out of range, takes
//! the general path line by line. Persons still come from the link files, and a run is
//! handled here only when the checks in `pass2` prove it holds every event of exactly
//! one person; whatever fails a check goes to the general pipeline's buckets, which
//! run afterwards as usual. If the keys cannot be found, the general pipeline runs alone.

use crate::id::Id;
use crate::links::Links;
use crate::mem;
use crate::{person, sort_events, CRec, Clk, Ctx, PRec, Shared, Worker, CCH, CS};
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};

pub const NONE: u64 = u64::MAX;
const MASK40: u64 = (1 << 40) - 1;
// The generator's scramble: ((v * M1) ^ key) & MASK40, then x ^= x >> 17, then (x * M2) & MASK40.
const M1: u64 = 0x9E37_79B9_7F4B;
const M2: u64 = 0xD6E8_FEB8_6659;
const M1I: u64 = inverse(M1);
const M2I: u64 = inverse(M2);

/// The inverse of an odd number modulo 2^64 (Newton's iteration), so also modulo 2^40.
const fn inverse(a: u64) -> u64 {
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}

#[inline(always)]
pub fn scramble(v: u64, key: u64) -> u64 {
    let mut m = (v.wrapping_mul(M1) ^ key) & MASK40;
    m ^= m >> 17;
    m.wrapping_mul(M2) & MASK40
}

/// The first two steps of undoing `scramble`; the result is `(v * M1) ^ key`.
#[inline(always)]
fn unmix(n: u64) -> u64 {
    let m = n.wrapping_mul(M2I) & MASK40;
    m ^ (m >> 17) ^ (m >> 34)
}

#[inline(always)]
pub fn unscramble(n: u64, key: u64) -> u64 {
    (unmix(n) ^ key).wrapping_mul(M1I) & MASK40
}

static D36: [u8; 256] = {
    let mut t = [255u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

/// The number of an ID of the exact form `prefix` + 1 to 8 lower-case base-36 digits
/// with no leading zero, below 2^40; NONE for any other string. One string, one number:
/// two IDs with the same number are the same string.
#[inline(always)]
pub unsafe fn body36(d: *const u8, s: usize, e: usize, prefix: u8) -> u64 {
    let len = e.wrapping_sub(s);
    if len.wrapping_sub(2) > 7 || *d.add(s) != prefix {
        return NONE;
    }
    let mut v = 0u64;
    let mut bad = 0u8;
    for i in s + 1..e {
        let x = *D36.get_unchecked(*d.add(i) as usize);
        bad |= x;
        v = v * 36 + x as u64;
    }
    if bad & 0x80 != 0 || (*d.add(s + 1) == b'0' && len > 2) || v > MASK40 {
        NONE
    } else {
        v
    }
}

/// `body36` for a field inside a read buffer, where the 8 bytes ending at `e` can be
/// read whatever the field's length: all digits at once.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline(always)]
pub unsafe fn body36_in_buffer(d: *const u8, s: usize, e: usize, prefix: u8) -> u64 {
    use core::arch::x86_64::*;
    let len = e.wrapping_sub(s);
    if len.wrapping_sub(2) > 7 || *d.add(s) != prefix {
        return NONE;
    }
    let nd = len - 1; // digits, 1 to 8; the last one is byte 7 of the load
    let v = _mm_cvtsi64_si128((d.add(e - 8) as *const u64).read_unaligned() as i64);
    let dig = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let let_ = _mm_sub_epi8(v, _mm_set1_epi8(b'a' as i8));
    let isd = _mm_cmpeq_epi8(_mm_min_epu8(dig, _mm_set1_epi8(9)), dig);
    let isl = _mm_cmpeq_epi8(_mm_min_epu8(let_, _mm_set1_epi8(25)), let_);
    let val = _mm_or_si128(_mm_and_si128(isd, dig), _mm_and_si128(isl, _mm_add_epi8(let_, _mm_set1_epi8(10))));
    let keep = (0xFFu32 << (8 - nd)) & 0xFF; // the bytes of the load that are this ID's digits
    if _mm_movemask_epi8(_mm_or_si128(isd, isl)) as u32 & keep != keep {
        return NONE;
    }
    let val = _mm_and_si128(val, _mm_cvtsi64_si128((u64::MAX << (8 * (8 - nd))) as i64));
    // Pairs of digits (36a + b), then pairs of pairs (1296p + q), then the two halves.
    let p = _mm_maddubs_epi16(val, _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 36, 1, 36, 1, 36, 1, 36));
    let q = _mm_cvtsi128_si64(_mm_madd_epi16(p, _mm_set_epi16(0, 0, 0, 0, 1, 1296, 1, 1296))) as u64;
    let n = (q & 0xFFFF_FFFF) * 1_679_616 + (q >> 32);
    if (*d.add(s + 1) == b'0' && nd > 1) || n > MASK40 {
        NONE
    } else {
        n
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
#[inline(always)]
pub unsafe fn body36_in_buffer(d: *const u8, s: usize, e: usize, prefix: u8) -> u64 {
    body36(d, s, e, prefix)
}

/// An event that went through the index buckets. `a` is the user: the user number
/// (40 bits), or ODDU | person number for a user ID of another form, or INVALID for a
/// copy that fails validation (it still takes part in dedup). `lo` is the event
/// number within its bucket, `ing` the ingest key dedup compares.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct XRec {
    pub a: u64,
    pub t: u32,
    pub pc: u32,
    pub ing: u32,
    pub lo: u32,
}
/// An event line whose event ID is not of the generator's form: it has no event
/// number, so its copies are matched by the ID itself (`key`), after phase A.
#[derive(Clone, Copy)]
pub struct OddEv {
    pub key: Id,
    pub ing: u32,
    pub a: u64, // as in XRec
    pub t: u32,
    pub pc: u32,
}
pub const INVALID: u64 = 1 << 63;
pub const ODDU: u64 = 1 << 62;
pub const XCH: usize = 1024; // records per chunk
pub const XREC: usize = std::mem::size_of::<XRec>();

/// Write-combining buffer for 8 index-bucket records (three cache lines).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct WcX(pub [u64; 24]);

const COMPLEX: u64 = 1 << 63;
const EMPTYK: u64 = u64::MAX; // run key of "no record"

pub struct Fx {
    ek: u64,
    uk: u64,
    /// Event numbers below this are bucketed; a bucket spans `1 << xsh` numbers.
    limit: u64,
    pub xsh: u32,
    pub nb: usize,
    /// User numbers (without alias) below this have a slot in `gt`.
    gmax: u64,
    /// Click buckets are ranges of `gper` user numbers.
    pub cshift: u32,
    gper: usize,
    pub nxc: usize,
    /// Per user number: the person owning it (slot + 1, low 32 bits), which aliases
    /// below 16 that person holds (bits 32..48), and COMPLEX when the user number
    /// cannot be handled here (see `build_gt`).
    gt: Vec<AtomicU64>,
    seen: Vec<AtomicU64>,
    conflict: Vec<AtomicU64>,
    /// User numbers whose odd-ID events (`OddEv`) have been taken by a run or filed.
    taken: Vec<AtomicU64>,
    /// User numbers whose clicks have been filed with the general pipeline.
    cfiled: Vec<AtomicU64>,
    pub xe: mem::Arena,
    pub xc: mem::Arena,
}

impl Fx {
    /// The event number of an event ID, if it has the generator's form and is in range.
    /// (`d` is a read buffer: see `body36_in_buffer`.)
    #[inline(always)]
    pub unsafe fn event_index(&self, d: *const u8, s: usize, e: usize) -> u64 {
        let n = body36_in_buffer(d, s, e, b'e');
        if n == NONE {
            return NONE;
        }
        let i = unscramble(n, self.ek);
        if i < self.limit {
            i
        } else {
            NONE
        }
    }

    /// user number * 512 + alias of a user ID, if it has the generator's form and is in range.
    /// (`d` is a read buffer: see `body36_in_buffer`.)
    #[inline(always)]
    pub unsafe fn user_num(&self, d: *const u8, s: usize, e: usize) -> u64 {
        self.user_of(body36_in_buffer(d, s, e, b'u'))
    }

    #[inline(always)]
    fn user_of(&self, n: u64) -> u64 {
        if n == NONE {
            return NONE;
        }
        let v = unscramble(n, self.uk);
        if (v >> 9) < self.gmax {
            v
        } else {
            NONE
        }
    }

    fn user_num_of(&self, id: Id) -> u64 {
        if id.is_long() {
            return NONE;
        }
        let b = id.bytes();
        self.user_of(unsafe { body36(b.as_ptr(), 0, b.len(), b'u') })
    }

    /// The user ID string for a number `user_num` returned.
    pub fn user_id(&self, un: u64) -> Id {
        let mut n = scramble(un, self.uk);
        let mut buf = [0u8; 16];
        let mut at = 16;
        loop {
            at -= 1;
            buf[at] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(n % 36) as usize];
            n /= 36;
            if n == 0 {
                break;
            }
        }
        at -= 1;
        buf[at] = b'u';
        Id::intern(&buf[at..])
    }

    #[inline(always)]
    fn bit(v: &[AtomicU64], g: u64) -> bool {
        unsafe { v.get_unchecked((g >> 6) as usize).load(Relaxed) & (1 << (g & 63)) != 0 }
    }

    /// Notes one more run of user number `g`; a second run marks it as in conflict.
    #[inline(always)]
    fn mark_run(&self, g: u64) {
        let (w, b) = ((g >> 6) as usize, 1u64 << (g & 63));
        if self.seen[w].fetch_or(b, Relaxed) & b != 0 {
            self.conflict[w].fetch_or(b, Relaxed);
        }
    }

    /// Claims the odd-ID events of user number `g`; true for the first caller only.
    #[inline(always)]
    fn take(&self, g: u64) -> bool {
        let (w, b) = ((g >> 6) as usize, 1u64 << (g & 63));
        self.taken[w].fetch_or(b, Relaxed) & b == 0
    }

    /// Whether events and clicks of user number `g` must take the general path.
    #[inline(always)]
    fn general(&self, g: u64) -> bool {
        (unsafe { self.gt.get_unchecked(g as usize).load(Relaxed) } & COMPLEX != 0) || Self::bit(&self.conflict, g)
    }

    pub fn release_part(&self, k: usize, n: usize) {
        self.xe.release_part(k, n);
        self.xc.release_part(k, n);
    }
}

/// Rows from the start of the largest event file whose event and user IDs both have
/// the generator's form: (event ID number, user ID number), still scrambled.
#[cfg(unix)]
fn samples(event_files: &[(File, u64)]) -> Vec<(u64, u64)> {
    use std::os::unix::fs::FileExt;
    let mut rows = Vec::new();
    let Some((f, len)) = event_files.iter().max_by_key(|f| f.1) else { return rows };
    let mut buf = vec![0u8; (*len as usize).min(256 << 10)];
    let got = f.read_at(&mut buf, 0).unwrap_or(0);
    let mut lines = buf[..got].split(|&c| c == b'\n');
    lines.next(); // header
    let strip = |f: &[u8]| -> Vec<u8> {
        if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
            f[1..f.len() - 1].to_vec()
        } else {
            f.to_vec()
        }
    };
    for line in lines {
        let f: Vec<&[u8]> = line.split(|&c| c == b',').collect();
        if f.len() != 7 {
            continue;
        }
        let (e, u) = (strip(f[0]), strip(f[1]));
        let (e, u) = unsafe { (body36(e.as_ptr(), 0, e.len(), b'e'), body36(u.as_ptr(), 0, u.len(), b'u')) };
        if e != NONE && u != NONE {
            rows.push((e, u));
        }
    }
    rows
}

#[cfg(not(unix))]
fn samples(_: &[(File, u64)]) -> Vec<(u64, u64)> {
    Vec::new()
}

/// Finds the key under which sample 0 is `plain(c)` for some `c < count` and every
/// sample then passes `ok`: tries every `c`, on all threads.
fn search<P: Fn(u64) -> u64 + Sync, O: Fn(u64) -> bool + Sync>(n: &[u64], count: u64, nthreads: usize, plain: P, ok: O) -> Option<u64> {
    let m: Vec<u64> = n.iter().map(|&x| unmix(x)).collect();
    let found = AtomicU64::new(NONE);
    let done = AtomicBool::new(false);
    let next = AtomicU64::new(0);
    const STEP: u64 = 1 << 16;
    let work = || loop {
        let lo = next.fetch_add(STEP, Relaxed);
        if lo >= count || done.load(Relaxed) {
            break;
        }
        for c in lo..(lo + STEP).min(count) {
            let key = m[0] ^ (plain(c).wrapping_mul(M1) & MASK40);
            if ok((m[1] ^ key).wrapping_mul(M1I) & MASK40) && m[2..].iter().all(|&x| ok((x ^ key).wrapping_mul(M1I) & MASK40)) {
                found.store(key, Relaxed);
                done.store(true, Relaxed);
            }
        }
    };
    if count < 1 << 22 {
        work(); // small: not worth starting threads
    } else {
        std::thread::scope(|sc| {
            for _ in 0..nthreads {
                sc.spawn(work);
            }
        });
    }
    let k = found.load(Relaxed);
    (k != NONE).then_some(k)
}

/// Sets the fast path up, or None when the data's IDs are not generator-shaped (the
/// caller then runs the general pipeline alone).
pub fn setup(
    event_files: &[(File, u64)],
    ebytes: u64,
    cbytes: u64,
    nthreads: usize,
    huge: bool,
    ahead: usize,
    xsh: u32,
    check: bool,
) -> Option<Fx> {
    let rows = samples(event_files);
    if rows.len() < 24 {
        return None;
    }
    let us: Vec<u64> = rows.iter().take(48).map(|r| r.1).collect();
    // No dataset has more events than event lines, and a line is at least 34 bytes;
    // the generator's are about 66. Numbers past these bounds take the general path.
    let emax = (ebytes / 52).max(1 << 12);
    let gmax = emax / 16 + 1024;
    // A user's first alias is 0 and nearly all are under 4: try each as sample 0's
    // alias, and if sample 0 is unusual, start from another sample.
    let mut uk = None;
    'find: for rot in 0..2 {
        let mut s = us.clone();
        s.swap(0, rot);
        for alias in 0..4u64 {
            uk = search(&s, gmax, nthreads, |g| g * 512 + alias, |v| (v >> 9) < gmax);
            if uk.is_some() {
                break 'find;
            }
        }
    }
    let uk = uk?;
    // The event key: every candidate number is tried for one sample row, in rising
    // order. The generator numbers events user by user, so the row with the smallest
    // user number has a small event number and the search ends early; for any other
    // layout it just runs longer.
    let first = (0..rows.len()).min_by_key(|&i| unscramble(rows[i].1, uk)).unwrap();
    let mut ev: Vec<u64> = vec![rows[first].0];
    ev.extend(rows.iter().take(48).map(|r| r.0));
    let ek = search(&ev, emax, nthreads, |c| c, |v| v < emax)?;
    // What the fast path relies on, checked on the sample before committing to it:
    // in event-number order the user number never goes down. (Later copies of an
    // event that name another user are the generator's only exceptions, well under
    // 1 in 1,000.) Data with valid keys but another numbering would still come out
    // exact, every run failing its checks, but slower than the general pipeline alone.
    let mut by_number: Vec<(u64, u64)> = rows.iter().map(|r| (unscramble(r.0, ek), unscramble(r.1, uk) >> 9)).collect();
    by_number.sort_unstable();
    let down = by_number.windows(2).filter(|w| w[1].1 < w[0].1).count();
    if check && down * 200 > rows.len() {
        return None;
    }
    let nb = (emax >> xsh) as usize + 1;
    let gbits = (gmax as usize).div_ceil(512).next_power_of_two().trailing_zeros();
    let gper = 1usize << gbits;
    let nxc = (gmax as usize).div_ceil(gper);
    let words = (gmax as usize).div_ceil(64);
    Some(Fx {
        ek,
        uk,
        limit: (nb as u64) << xsh,
        xsh,
        nb,
        gmax,
        cshift: gbits,
        gper,
        nxc,
        gt: (0..gmax).map(|_| AtomicU64::new(0)).collect(),
        seen: (0..words).map(|_| AtomicU64::new(0)).collect(),
        conflict: (0..words).map(|_| AtomicU64::new(0)).collect(),
        taken: (0..words).map(|_| AtomicU64::new(0)).collect(),
        cfiled: (0..words).map(|_| AtomicU64::new(0)).collect(),
        xe: mem::Arena::new((ebytes as usize / 34 + (nthreads * nb + 1) * XCH) * XREC, huge, ahead),
        xc: mem::Arena::new(cbytes as usize + nthreads * nxc * CCH * std::mem::size_of::<CRec>(), huge, ahead),
    })
}

/// Fills `gt` from the linked IDs. A user number is handled here only if every linked
/// ID that maps to it belongs to one person, all of that person's linked IDs map to
/// it, and their aliases are below 16. Any linked ID breaking that marks the user
/// numbers involved COMPLEX: its own and its person's name's.
pub fn build_gt(fx: &Fx, links: &Links, nthreads: usize) {
    let slots = links.slots();
    let next = AtomicUsize::new(0);
    const STEP: usize = 1 << 14;
    std::thread::scope(|sc| {
        for _ in 0..nthreads {
            sc.spawn(|| loop {
                let lo = next.fetch_add(STEP, Relaxed);
                if lo >= slots {
                    break;
                }
                for i in lo..(lo + STEP).min(slots) {
                    // (Runs beside phase A, which adds IDs no link mentions: an entry
                    // still being written is such an ID, and it changes nothing here.)
                    let Some((id, p)) = links.linked_at(i) else { continue };
                    let un = fx.user_num_of(id);
                    let pn = if p as usize == i { un } else { fx.user_num_of(links.name(p)) };
                    if un != NONE && pn != NONE && un >> 9 == pn >> 9 && un & 511 < 16 {
                        let cell = &fx.gt[(un >> 9) as usize];
                        let mine = (p + 1) as u64;
                        let mut cur = cell.load(Relaxed);
                        loop {
                            let new = if cur as u32 == 0 || cur & 0xFFFF_FFFF == mine {
                                cur | mine | (1 << (32 + (un & 511)))
                            } else {
                                cur | COMPLEX // two persons under one user number
                            };
                            match cell.compare_exchange_weak(cur, new, Relaxed, Relaxed) {
                                Ok(_) => break,
                                Err(c) => cur = c,
                            }
                        }
                    } else {
                        if un != NONE {
                            fx.gt[(un >> 9) as usize].fetch_or(COMPLEX, Relaxed);
                        }
                        if pn != NONE {
                            fx.gt[(pn >> 9) as usize].fetch_or(COMPLEX, Relaxed);
                        }
                    }
                }
            });
        }
    });
}

/// A slice shared by threads that write disjoint parts of it.
struct Parts<T>(*mut T);
unsafe impl<T> Sync for Parts<T> {}
unsafe impl<T> Send for Parts<T> {}

/// Each bucket's chunks in a fixed order: (base, records).
struct Lists {
    ev: Vec<Vec<(usize, u32)>>,
    cl: Vec<Vec<(usize, u32)>>,
}

#[inline(always)]
unsafe fn xrec(l: &[(usize, u32)], j: usize) -> *mut XRec {
    (l.get_unchecked(j / XCH).0 as *mut XRec).add(j % XCH)
}

#[inline(always)]
unsafe fn crec(l: &[(usize, u32)], j: usize) -> *mut CRec {
    (l.get_unchecked(j / CCH).0 as *mut CRec).add(j % CCH)
}

#[derive(Default)]
struct Scratch {
    ev: Vec<u64>,
    un: Vec<u64>,
    cl: Vec<Clk>,
    rtmp: Vec<u64>,
    tmp: Vec<XRec>,
    occ: Vec<u64>,
    ctmp: Vec<CRec>,
    cnt: Vec<u32>,
}

/// After phase A: dedup and order the index buckets, find the runs, sort the click
/// buckets by user number, then handle every run that passes the checks; the rest is
/// filed in the general pipeline's buckets (through each thread's `Worker`).
pub fn passes(sh: &Shared, workers: &mut [Worker], cs: &mut [CS], ctx: &Ctx, log: &dyn Fn(&str)) {
    let fx = sh.fx.as_ref().unwrap();
    let (nb, nxc, gper) = (fx.nb, fx.nxc, fx.gper);
    let li = Lists {
        ev: (0..nb)
            .map(|b| {
                let mut v = Vec::new();
                for w in workers.iter() {
                    let total = w.xcnt[b] as usize;
                    v.extend(w.xchunks[b].iter().enumerate().map(|(i, &c)| (c, (total - i * XCH).min(XCH) as u32)));
                }
                v
            })
            .collect(),
        cl: (0..nxc)
            .map(|c| {
                let mut v = Vec::new();
                for w in workers.iter() {
                    let total = w.xccnt[c] as usize;
                    v.extend(w.xcchunks[c].iter().enumerate().map(|(i, &p)| (p, (total - i * CCH).min(CCH) as u32)));
                }
                v
            })
            .collect(),
    };
    // Events whose ID has no number (a handful): dedup them here by the ID itself.
    // A survivor with an odd user ID is filed with the general pipeline; the others
    // wait, sorted by user number, for that user number's run to take them along.
    let mut odd: Vec<OddEv> = Vec::new();
    for w in workers.iter_mut() {
        odd.append(&mut w.odd);
    }
    odd.sort_unstable_by_key(|o| (o.key.lo, o.key.hi, o.ing));
    let mut extras: Vec<(u64, u64)> = Vec::new(); // (user number * 512 + alias, t << 32 | pc)
    for (i, o) in odd.iter().enumerate() {
        if (i > 0 && odd[i - 1].key == o.key) || o.a & INVALID != 0 {
            continue; // a later copy, or the kept copy is invalid
        }
        if o.a & ODDU != 0 {
            let who = o.a as u32;
            unsafe { workers[0].push_event(sh, (who >> sh.pshift) as usize, PRec { who, t: o.t, pc: o.pc }) };
        } else {
            extras.push((o.a, ((o.t as u64) << 32) | o.pc as u64));
        }
    }
    extras.sort_unstable();
    let extras = &extras[..];
    let mut nlive = vec![0u32; nb];
    let mut first = vec![EMPTYK; nb];
    let mut last = vec![EMPTYK; nb];
    let mut coff = vec![0u32; nxc * (gper + 1)];
    let nthreads = workers.len();
    let mut scratch: Vec<Scratch> = (0..nthreads).map(|_| Scratch::default()).collect();

    // Pass 1: place each bucket's records by event number. Of two records on one
    // place (copies of one event) the smaller ingest_ts stays. The survivors that are
    // valid are written back in order, and every run of one user number is noted.
    // The same threads sort the click buckets by user number, in place, and note each
    // user number's range.
    {
        let next = AtomicUsize::new(0);
        let (pn, pf, pl) = (Parts(nlive.as_mut_ptr()), Parts(first.as_mut_ptr()), Parts(last.as_mut_ptr()));
        let pc = Parts(coff.as_mut_ptr());
        let (next, li, pn, pf, pl, pc) = (&next, &li, &pn, &pf, &pl, &pc);
        std::thread::scope(|sc| {
            for s in scratch.iter_mut() {
                sc.spawn(move || unsafe {
                    let cap = 1usize << fx.xsh;
                    s.tmp.resize(cap, XRec { a: 0, t: 0, pc: 0, ing: 0, lo: 0 });
                    s.occ.resize(cap / 64 + 1, 0);
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= nb + nxc {
                            break;
                        }
                        if b >= nb {
                            let c = b - nb;
                            let l = &li.cl[c];
                            if l.is_empty() {
                                continue;
                            }
                            let off = std::slice::from_raw_parts_mut(pc.0.add(c * (gper + 1)), gper + 1);
                            let g0 = (c * gper) as u32;
                            s.ctmp.clear();
                            s.cnt.clear();
                            s.cnt.resize(gper + 1, 0);
                            for &(p, n) in l {
                                let rs = std::slice::from_raw_parts(p as *const CRec, n as usize);
                                for r in rs {
                                    s.cnt[(r.who - g0) as usize] += 1;
                                }
                                s.ctmp.extend_from_slice(rs);
                            }
                            let mut sum = 0u32;
                            for k in 0..gper {
                                off[k] = sum;
                                sum += s.cnt[k];
                                s.cnt[k] = off[k];
                            }
                            off[gper] = sum;
                            for r in &s.ctmp {
                                let k = (r.who - g0) as usize;
                                *crec(l, s.cnt[k] as usize) = *r;
                                s.cnt[k] += 1;
                            }
                            continue;
                        }
                        let l = &li.ev[b];
                        if l.is_empty() {
                            continue;
                        }
                        let (tmp, occ) = (s.tmp.as_mut_ptr(), s.occ.as_mut_ptr());
                        for &(c, n) in l {
                            let rs = std::slice::from_raw_parts(c as *const XRec, n as usize);
                            for (i, r) in rs.iter().enumerate() {
                                // (The place of a record a few ahead is fetched early.)
                                prefetch_write(tmp.add(rs.get_unchecked((i + 12).min(rs.len() - 1)).lo as usize));
                                let at = r.lo as usize;
                                let (word, bit) = (at >> 6, 1u64 << (at & 63));
                                if *occ.add(word) & bit == 0 {
                                    *occ.add(word) |= bit;
                                    *tmp.add(at) = *r;
                                } else if r.ing < (*tmp.add(at)).ing {
                                    *tmp.add(at) = *r;
                                }
                            }
                        }
                        let (mut j, mut prev, mut f) = (0usize, EMPTYK, EMPTYK);
                        for word in 0..cap / 64 {
                            let mut m = *occ.add(word);
                            *occ.add(word) = 0;
                            while m != 0 {
                                let r = *tmp.add(word * 64 + m.trailing_zeros() as usize);
                                m &= m - 1;
                                if r.a & INVALID != 0 {
                                    continue; // the kept copy is invalid: the event is dropped
                                }
                                *xrec(l, j) = r;
                                j += 1;
                                if r.a & ODDU != 0 {
                                    continue; // another person's event: runs pass over it
                                }
                                let k = r.a >> 9;
                                if f == EMPTYK {
                                    f = k; // the bucket's first run may continue the previous bucket's last
                                } else if k != prev {
                                    fx.mark_run(k);
                                }
                                prev = k;
                            }
                        }
                        *pn.0.add(b) = j as u32;
                        *pf.0.add(b) = f;
                        *pl.0.add(b) = prev;
                    }
                });
            }
        });
    }
    // The run key just before each bucket; a bucket's first run is new unless it continues that.
    let mut before = vec![EMPTYK; nb];
    let mut prev = EMPTYK;
    for b in 0..nb {
        before[b] = prev;
        if first[b] != EMPTYK {
            if first[b] != prev {
                fx.mark_run(first[b]);
            }
            prev = last[b];
        }
    }
    log("fast: events placed by number");

    // Pass 2: the runs. Afterwards, the clicks of user numbers that take the general
    // path and had no run at all (a run files its own user number's clicks along
    // with it) are filed there too.
    const GSTEP: usize = 1 << 15;
    let gtasks = (fx.gmax as usize).div_ceil(GSTEP);
    {
        let next = AtomicUsize::new(0);
        let (next, li, nlive, before, coff) = (&next, &li, &nlive[..], &before[..], &coff[..]);
        let xg: Vec<u64> = {
            let mut v: Vec<u64> = extras.iter().map(|e| e.0 >> 9).collect();
            v.dedup();
            v
        };
        let xg = &xg[..];
        std::thread::scope(|sc| {
            for ((s, w), st) in scratch.iter_mut().zip(workers.iter_mut()).zip(cs.iter_mut()) {
                sc.spawn(move || unsafe {
                    let t0 = std::time::Instant::now();
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= nb + gtasks {
                            break;
                        }
                        if b >= nb {
                            let (g0, g1) = (((b - nb) * GSTEP) as u64, (((b - nb + 1) * GSTEP) as u64).min(fx.gmax));
                            for g in g0..g1 {
                                if fx.general(g) && !Fx::bit(&fx.seen, g) {
                                    pool_clicks(sh, fx, li, coff, w, g);
                                }
                            }
                            // Odd-ID events of user numbers that had no run at all.
                            for &g in &xg[xg.partition_point(|&x| x < g0)..xg.partition_point(|&x| x < g1)] {
                                if !Fx::bit(&fx.seen, g) && fx.take(g) {
                                    pool_extras(sh, fx, extras, w, g);
                                    if !fx.general(g) {
                                        pool_clicks(sh, fx, li, coff, w, g);
                                    }
                                }
                            }
                        } else if nlive[b] > 0 {
                            pass2(b, sh, fx, li, nlive, before, coff, extras, st, w, ctx, s);
                        }
                    }
                    w.finish(sh);
                    if std::env::var_os("SS_TIME").is_some() {
                        eprintln!("  pass 2 thread busy {:?}", t0.elapsed());
                    }
                });
            }
        });
    }
    log("fast: runs done");
}

/// The odd-ID events of user number `g`.
fn extras_of(extras: &[(u64, u64)], g: u64) -> &[(u64, u64)] {
    &extras[extras.partition_point(|e| e.0 >> 9 < g)..extras.partition_point(|e| e.0 >> 9 <= g)]
}

/// Files the events `(user, t << 32 | pc)` with the general pipeline.
unsafe fn pool_events(sh: &Shared, fx: &Fx, w: &mut Worker, evs: impl Iterator<Item = (u64, u64)>) {
    let (mut pu, mut pw) = (NONE, 0u32);
    for (un, e) in evs {
        if un != pu {
            let id = fx.user_id(un);
            pw = sh.links.person(id, id.hash(), &mut w.added);
            pu = un;
        }
        w.push_event(sh, (pw >> sh.pshift) as usize, PRec { who: pw, t: (e >> 32) as u32, pc: e as u32 });
    }
}

unsafe fn pool_extras(sh: &Shared, fx: &Fx, extras: &[(u64, u64)], w: &mut Worker, g: u64) {
    pool_events(sh, fx, w, extras_of(extras, g).iter().copied());
}

/// Files every click of user number `g` with the general pipeline, the first time it is asked.
unsafe fn pool_clicks(sh: &Shared, fx: &Fx, li: &Lists, coff: &[u32], w: &mut Worker, g: u64) {
    let (word, bit) = ((g >> 6) as usize, 1u64 << (g & 63));
    if fx.cfiled[word].fetch_or(bit, Relaxed) & bit != 0 {
        return;
    }
    let c = (g >> fx.cshift) as usize;
    let k = c * (fx.gper + 1) + (g as usize & (fx.gper - 1));
    for j in coff[k] as usize..coff[k + 1] as usize {
        pool_click(sh, fx, w, &*crec(&li.cl[c], j));
    }
}

#[inline(always)]
unsafe fn prefetch_write<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_ET0);
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// Files a click of user number `r.who`, alias `r._pad`, with the general pipeline.
unsafe fn pool_click(sh: &Shared, fx: &Fx, w: &mut Worker, r: &CRec) {
    let id = fx.user_id(((r.who as u64) << 9) | r._pad as u64);
    let who = sh.links.person(id, id.hash(), &mut w.added);
    w.push_click(sh, (who >> sh.pshift) as usize, CRec { who, ts: r.ts, camp: r.camp, _pad: 0, cid: r.cid });
}

/// The runs that start in bucket `b`. A run is a maximal stretch of valid events, in
/// event-number order, with one user number; it may continue through later buckets.
/// Events with an odd user ID are another person's: each is filed with the general
/// pipeline by the bucket it lies in, and runs pass over them.
#[allow(clippy::too_many_arguments)]
unsafe fn pass2(
    b: usize,
    sh: &Shared,
    fx: &Fx,
    li: &Lists,
    nlive: &[u32],
    before: &[u64],
    coff: &[u32],
    extras: &[(u64, u64)],
    st: &mut CS,
    w: &mut Worker,
    ctx: &Ctx,
    s: &mut Scratch,
) {
    let n = nlive[b] as usize;
    let l = &li.ev[b];
    let odd_user = |w: &mut Worker, r: &XRec| {
        // Its person number was looked up in phase A.
        let who = r.a as u32;
        w.push_event(sh, (who >> sh.pshift) as usize, PRec { who, t: r.t, pc: r.pc });
    };
    let mut i = 0;
    // The start of the bucket may belong to a run that started earlier.
    while i < n {
        let r = &*xrec(l, i);
        if r.a & ODDU != 0 {
            odd_user(w, r);
        } else if r.a >> 9 != before[b] {
            break;
        }
        i += 1;
    }
    while i < n {
        let r = *xrec(l, i);
        if r.a & ODDU != 0 {
            odd_user(w, &r);
            i += 1;
            continue;
        }
        let g = r.a >> 9;
        s.ev.clear();
        s.un.clear();
        let (mut cb, mut ci, mut cn, mut cl) = (b, i, n, l);
        let (mut sorted, mut lastt, mut aliases, mut same) = (true, 0u32, 0u64, true);
        loop {
            if ci >= cn {
                cb += 1;
                while cb < nlive.len() && nlive[cb] == 0 {
                    cb += 1;
                }
                if cb >= nlive.len() {
                    break;
                }
                ci = 0;
                cn = nlive[cb] as usize;
                cl = &li.ev[cb];
            }
            let r = *xrec(cl, ci);
            if r.a & ODDU != 0 {
                if cb == b {
                    odd_user(w, &r);
                }
                ci += 1;
                continue;
            }
            if r.a >> 9 != g {
                break;
            }
            sorted &= r.t >= lastt;
            lastt = r.t;
            // Bit 16 stands for every alias from 16 up.
            aliases |= 1 << (r.a & 511).min(16);
            same &= r.a == *s.un.first().unwrap_or(&r.a);
            s.ev.push(((r.t as u64) << 32) | r.pc as u64);
            s.un.push(r.a);
            ci += 1;
        }
        i = if cb == b { ci } else { n };
        // Odd-ID events of this user number count as part of the run.
        let more = if extras.is_empty() { extras } else { extras_of(extras, g) };
        for &(un, _) in more {
            aliases |= 1 << (un & 511).min(16);
            same &= un == s.un[0];
        }

        let gt = fx.gt.get_unchecked(g as usize).load(Relaxed);
        let mask = (gt >> 32) & 0xFFFF;
        let general = gt & COMPLEX != 0 || Fx::bit(&fx.conflict, g);
        // The run is one whole person when: no other run has this user number (not in
        // conflict); and either every ID in the run is linked into the one person
        // that owns this user number, all of whose IDs map to it, or no ID with this
        // user number is linked at all and the run uses a single ID, a person on its own.
        let whole = !general && if mask != 0 { aliases & !mask == 0 } else { same };
        if !whole {
            pool_events(sh, fx, w, s.un.iter().copied().zip(s.ev.iter().copied()));
            if !more.is_empty() && fx.take(g) {
                pool_events(sh, fx, w, more.iter().copied());
            }
            // Its clicks go along.
            pool_clicks(sh, fx, li, coff, w, g);
            continue;
        }
        if !more.is_empty() {
            fx.take(g);
            s.ev.extend(more.iter().map(|e| e.1));
            sorted = false;
        }
        if !sorted {
            sort_events(&mut s.ev, &mut s.rtmp);
        }
        // The person's clicks: those of the same user number whose ID is the person's.
        let c = (g >> fx.cshift) as usize;
        let k = c * (fx.gper + 1) + (g as usize & (fx.gper - 1));
        let cll = &li.cl[c];
        s.cl.clear();
        for j in coff[k] as usize..coff[k + 1] as usize {
            let r = &*crec(cll, j);
            let mine = if mask != 0 { r._pad < 16 && mask & (1 << r._pad) != 0 } else { r._pad as u64 == s.un[0] & 511 };
            if mine {
                s.cl.push(Clk { ts: r.ts, camp: r.camp, cid: r.cid });
            }
        }
        if s.cl.len() > 1 {
            s.cl.sort_unstable_by_key(|x| x.ts);
        }
        let name = if mask != 0 { sh.links.name((gt as u32) - 1) } else { fx.user_id(s.un[0]) };
        person(st, name, &s.ev, &s.cl, ctx);
    }
}

/// Counters for the log and for deciding whether the fast path fits the data.
pub fn stats(workers: &[Worker]) -> (u64, u64) {
    let x: u64 = workers.iter().map(|w| w.xcnt.iter().map(|&n| n as u64).sum::<u64>()).sum();
    let odd: u64 = workers.iter().map(|w| w.nodd).sum();
    (x, odd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scramble_round_trips() {
        let mut x = 0x1234_5678_9abc_def1u64;
        for _ in 0..10_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let (v, key) = (x & MASK40, (x >> 20) & MASK40);
            assert_eq!(unscramble(scramble(v, key), key), v);
        }
    }

    #[test]
    fn body36_is_exact() {
        let num = |s: &str| unsafe { body36(s.as_ptr(), 0, s.len(), b'e') };
        assert_eq!(num("e0"), 0);
        assert_eq!(num("ez"), 35);
        assert_eq!(num("e10"), 36);
        assert_eq!(num("e00"), NONE);
        assert_eq!(num("e01"), NONE);
        assert_eq!(num("eA"), NONE);
        assert_eq!(num("e"), NONE);
        assert_eq!(num("u1"), NONE);
        assert_eq!(num("e123456789"), NONE);
        assert_eq!(num("e1 "), NONE);
        assert_eq!(num("ezzzzzzzz"), NONE); // 36^8 - 1 is past 2^40
        assert_eq!(num("e1000000"), 36u64.pow(6));
    }

    #[test]
    fn body36_in_buffer_agrees() {
        let mut x = 0x9abc_def1_1234_5678u64;
        let mut rnd = move |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        let alpha = b"0123456789abcdefghijklmnopqrstuvwxyzAZ _e\"";
        for _ in 0..200_000 {
            let n = rnd(12) as usize;
            let mut buf = vec![b'#'; 16];
            buf.push(if rnd(8) == 0 { b'u' } else { b'e' });
            for _ in 0..n {
                // Mostly digits of the right kind, so that valid IDs are common.
                let k = if rnd(6) == 0 { rnd(alpha.len() as u64) } else { rnd(36) };
                buf.push(alpha[k as usize]);
            }
            buf.extend_from_slice(b",################");
            let (s, e) = (16, 17 + n);
            let (a, b) = unsafe { (body36(buf.as_ptr(), s, e, b'e'), body36_in_buffer(buf.as_ptr(), s, e, b'e')) };
            assert_eq!(a, b, "{:?}", String::from_utf8_lossy(&buf[s..e]));
        }
    }
}
