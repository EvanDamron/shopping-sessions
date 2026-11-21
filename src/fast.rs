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
//! handled by the event stage (stage.rs) only when its checks prove it holds every
//! event of exactly one person; whatever fails a check goes to the general pipeline's
//! buckets, which run afterwards as usual. If the keys cannot be found, the general
//! pipeline runs alone.
//!
//! This file: the ID arithmetic, finding the keys, the records phase A files, and the
//! table of user numbers built from the link files. What happens to the records after
//! phase A is in stage.rs.

use crate::id::Id;
use crate::links::Links;
use crate::mem;
use crate::{CRec, Worker, CCH};
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

pub const COMPLEX: u64 = 1 << 63;
/// In `Fx::gt`: the user number has valid events with an odd event ID (set by the
/// event stage before it starts).
pub const EXTRA: u64 = 1 << 62;

pub struct Fx {
    ek: u64,
    pub uk: u64,
    /// Event numbers below this are bucketed; a bucket spans `1 << xsh` numbers.
    limit: u64,
    pub xsh: u32,
    pub nb: usize,
    /// User numbers (without alias) below this have a slot in `gt`.
    gmax: u64,
    /// Click buckets are ranges of `gper` user numbers.
    pub cshift: u32,
    pub gper: usize,
    pub nxc: usize,
    /// Per user number: the person owning it (slot + 1, low 32 bits), which aliases
    /// below 16 that person holds (bits 32..48), and COMPLEX when the user number
    /// cannot be handled here (see `build_gt`).
    pub gt: Vec<AtomicU64>,
    /// Whether the records' memory asks for huge pages.
    pub huge: bool,
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
    if gmax >= 1 << 27 {
        return None; // the event stage keeps a user number in 27 bits
    }
    let nb = (emax >> xsh) as usize + 1;
    let gbits = (gmax as usize).div_ceil(512).next_power_of_two().trailing_zeros();
    let gper = 1usize << gbits;
    let nxc = (gmax as usize).div_ceil(gper);
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
        huge,
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
