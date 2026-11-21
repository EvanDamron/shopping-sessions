//! Per-line decoder for event lines: no separator index pass.
//!
//! A line is looked at once. Three 32-byte loads from its start give bit masks of
//! its commas, quotes and newline; the six commas come out of the masks by bit
//! operations, in two 64-bit windows (fields 0 to 2 from the line start, fields 3 to
//! 6 from the start of `ingest_ts`). The quote mask is checked against the one mask
//! it has to be if every field is either bare or wrapped in exactly one pair of
//! quotes. Then the fields are converted with straight-line code and a few small
//! tables keyed by a field's length and quoting:
//!
//!   both IDs      one 256-bit lane each: prefix letter, base-36 digits, value
//!   both times    one 256-bit lane each: `ts` in any of its three forms next to
//!                 `ingest_ts`, separators and digit ranges checked, digits folded
//!                 by three multiply-adds; the date part by a small table of the
//!                 days that can matter for this dataset's window
//!   event type    word compares
//!   product, price  only looked at for a purchase
//!
//! The rows gather in a batch (struct of arrays); the batch turns ID values into
//! event and user numbers with the dataset's keys, applies the window and lateness
//! rules and files the records exactly as `events_x` does.
//!
//! This is an acceptor of the common shapes only. Whatever it is not sure about, at
//! any step (a line longer than 96 bytes, a field count other than seven, a quote
//! out of place, an ID that is not `e`/`u` + base 36 or whose number is out of
//! range, a timestamp of another length or with any character out of place, a date
//! outside the table, an empty product), is handed whole to `cold`, which runs the
//! general per-line code on that line. So the result is that code's result on every
//! line; the decoder only decides how fast it is reached.

use crate::fast::{self, unscramble, Fx, XRec};
use crate::{event_x_line, parse_price, Shared, Worker, CART, DAY, PURCHASE, VIEW, WEEK};
use crate::id::Id;
use core::arch::x86_64::*;

/// Rows per batch.
const B: usize = 64;

/// How one form of `ts` is read: lane 0 of a 256-bit vector holds 16 bytes of the
/// `ts` field, lane 1 always holds `THH:MM:SSZ` of `ingest_ts`.
#[repr(C, align(64))]
struct Form {
    lo: [u8; 32],    // subtracted from each byte ...
    mk: [u8; 32],    // ... masked ...
    rg: [u8; 32],    // ... and then at most this: digits 0..9, separators exact
    shuf: [u8; 32],  // digits into pairs
    wlim: [i16; 16], // the most each pair may be
    w2: [i16; 16],
    w3: [i16; 16],
    off: isize,      // lane 0 starts this far from the field's first character
    k0: i64,         // time = c + k0 * first number + kb * second number
    kb: [i64; 2],    // (indexed by bit 1 of the character after the seconds)
    dmask: u64,      // all ones if the field starts with a date
    c0: i64,
    bad: u64,        // nonzero: no such form
}

/// One day of the date table.
#[repr(C)]
#[derive(Clone, Copy)]
struct Day {
    w0: u64,   // "YYYY-MM-"
    w1: u64,   // "YY-MM-DD"
    tms: i64,  // the day's start, ms since the click window's start
    isec: i64, // the day's start, seconds since the click window's start
}

/// Bits of "YY-MM-DD" that tell the days of two neighbouring months apart: the
/// month's last digit (bit 0), the day's digits (2 and 4 bits).
const DAYBITS: u64 = (1 << 32) | (3 << 48) | (0xF << 56);

#[repr(C, align(64))]
pub struct Dec {
    forms: [Form; 4],
    /// Form of a `ts` field by 2 * raw length + quoted.
    tsform: [u8; 128],
    /// Shuffle for an ID field by 2 * raw length + quoted, applied to the 16 bytes
    /// that end where the field ends: digits right-aligned in bytes 8..16, the
    /// prefix letter in byte 7, the first digit again in byte 6.
    idsh: [[u8; 16]; 128],
    days: [Day; 128],
    idt: [u8; 32],
    idlow: [u8; 32],
    idrng: [u8; 32],
    idw1: [u8; 32],
    idw2: [i16; 16],
    idw3: [u32; 8],
    base: i64,
    base_s: i64,
    evspan: u64,
}

fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

fn daykey(w1: u64) -> usize {
    (((w1 >> 32) & 1) | (((w1 >> 48) & 3) << 1) | (((w1 >> 56) & 0xF) << 3)) as usize
}

impl Dec {
    /// `base` is the start of the click window, `end` the window's end (ms), `evspan`
    /// the length of the event window.
    pub fn new(base: i64, end: i64, evspan: u64) -> Box<Dec> {
        let base_s = base.div_euclid(1000);
        let mut dec: Box<Dec> = unsafe { Box::new(std::mem::zeroed()) };
        dec.base = base;
        dec.base_s = base_s;
        dec.evspan = evspan;

        // ---- The three forms of `ts`.
        // Lane 1, `ingest_ts` from its 'T': T HH : MM : SS Z.
        let f1 = |f: &mut Form, lane: usize, with_offset: bool| {
            let o = 16 * lane;
            let set = |f: &mut Form, i: usize, lo: u8, mk: u8, rg: u8| {
                f.lo[o + i] = lo;
                f.mk[o + i] = mk;
                f.rg[o + i] = rg;
            };
            set(f, 0, b'T', 0xFF, 0);
            set(f, 3, b':', 0xFF, 0);
            set(f, 6, b':', 0xFF, 0);
            for i in [1, 2, 4, 5, 7, 8] {
                set(f, i, b'0', 0xFF, 9);
            }
            for i in 0..16 {
                f.shuf[o + i] = 0x80;
            }
            for (k, src) in [1u8, 2, 4, 5, 7, 8].into_iter().enumerate() {
                f.shuf[o + k] = src;
            }
            let w = 8 * lane;
            for i in 0..8 {
                f.wlim[w + i] = i16::MAX;
            }
            (f.wlim[w], f.wlim[w + 1], f.wlim[w + 2]) = (23, 59, 59);
            (f.w2[w], f.w2[w + 1], f.w2[w + 2]) = (60, 1, 1);
            (f.w3[w], f.w3[w + 1]) = (60, 1);
            if with_offset {
                // ... then a sign and HH:MM. '+' and '-' differ in bits 1 and 2 only
                // as 0x2B and 0x2D: (c - '+') & 0xFD is zero for exactly those two.
                set(f, 9, b'+', 0xFD, 0);
                set(f, 12, b':', 0xFF, 0);
                for i in [10, 11, 13, 14] {
                    set(f, i, b'0', 0xFF, 9);
                }
                for (k, src) in [10u8, 11, 13, 14].into_iter().enumerate() {
                    f.shuf[o + 8 + k] = src;
                }
                (f.wlim[w + 4], f.wlim[w + 5]) = (23, 59);
                (f.w2[w + 4], f.w2[w + 5]) = (60, 1);
                f.w3[w + 2] = 60;
            } else {
                set(f, 9, b'Z', 0xFF, 0);
            }
        };
        for k in 0..4 {
            let f = &mut dec.forms[k];
            f1(f, 1, false);
            match k {
                0 | 1 => {
                    f1(f, 0, k == 1);
                    f.off = 10;
                    f.k0 = 1000;
                    // local time minus the offset: '+' has bit 1 set, '-' has not
                    f.kb = if k == 1 { [1000, -1000] } else { [0, 0] };
                    f.dmask = u64::MAX;
                    f.c0 = 0;
                }
                _ => {
                    // 13 digits ending where the field ends: the lane starts 3 bytes
                    // before the field.
                    for i in 0..16 {
                        f.shuf[i] = i as u8;
                        if i >= 3 {
                            (f.lo[i], f.mk[i], f.rg[i]) = (b'0', 0xFF, 9);
                        }
                    }
                    for i in 0..8 {
                        f.wlim[i] = i16::MAX;
                    }
                    for i in 0..4 {
                        (f.w2[2 * i], f.w2[2 * i + 1]) = (100, 1);
                    }
                    (f.w3[0], f.w3[1], f.w3[2], f.w3[3]) = (10_000, 1, 10_000, 1);
                    f.off = -3;
                    f.k0 = 100_000_000;
                    f.kb = [1, 1];
                    f.dmask = 0;
                    f.c0 = base.wrapping_neg();
                    f.bad = (k == 3) as u64;
                }
            }
        }
        for i in 0..128 {
            dec.tsform[i] = 3;
        }
        for (len, form) in [(20, 0u8), (25, 1), (13, 2)] {
            dec.tsform[2 * len] = form;
            dec.tsform[2 * (len + 2) + 1] = form;
        }

        // ---- IDs.
        for i in 0..128 {
            let (raw, q) = (i / 2, i % 2);
            let sh = &mut dec.idsh[i];
            *sh = [0x80; 16];
            if raw < 2 * q + 2 || raw > 2 * q + 9 {
                continue; // not a letter and 1 to 8 digits
            }
            let nd = raw - 2 * q - 1;
            for j in 0..nd {
                sh[15 - j] = (15 - q - j) as u8;
            }
            sh[7] = (15 - q - nd) as u8;
            sh[6] = if nd >= 2 { (16 - q - nd) as u8 } else { (15 - q - nd) as u8 };
        }
        for lane in 0..2 {
            let o = 16 * lane;
            for i in 0..8 {
                dec.idt[o + i] = 127;
                dec.idt[o + 8 + i] = 9;
                dec.idrng[o + i] = 255;
                dec.idrng[o + 8 + i] = 25;
                dec.idw1[o + 8 + i] = if i % 2 == 0 { 36 } else { 1 };
            }
            dec.idlow[o + 6] = 1;
            dec.idrng[o + 6] = 254;
            dec.idlow[o + 7] = [b'e', b'u'][lane] - b'0';
            dec.idrng[o + 7] = 0;
            let w = 8 * lane;
            (dec.idw2[w + 4], dec.idw2[w + 5], dec.idw2[w + 6], dec.idw2[w + 7]) = (1296, 1, 1296, 1);
            dec.idw3[4 * lane + 2] = 1_679_616;
        }

        // ---- Dates. A `ts` in the click window has a local date within a day of it;
        // an `ingest_ts` is usually within a week after. A day that is not here (or
        // that would share a key with one that is) is simply left to `cold`.
        for i in 0..128 {
            dec.days[i] = Day { w0: 0, w1: if i == 0 { u64::MAX } else { 0 }, tms: 0, isec: 0 };
        }
        let first = base.div_euclid(DAY) - 1;
        let last = (end - 1).div_euclid(DAY) + 8;
        for day in first..=last {
            let (y, m, d) = civil(day);
            if !(0..=9999).contains(&y) {
                continue;
            }
            let text = format!("{y:04}-{m:02}-{d:02}");
            let t = text.as_bytes();
            let w0 = u64::from_le_bytes(t[0..8].try_into().unwrap());
            let w1 = u64::from_le_bytes(t[2..10].try_into().unwrap());
            let e = &mut dec.days[daykey(w1)];
            if e.w0 == 0 {
                *e = Day { w0, w1, tms: day * DAY - base, isec: day * 86_400 - base_s };
            }
        }
        dec
    }
}

#[repr(C, align(64))]
struct Batch {
    ids: [[u64; 4]; B], // [1] event ID value, [3] user ID value
    t: [i64; B],        // ts, ms since the click window's start
    ing: [i64; B],      // ingest_ts, seconds since the click window's start
    pc: [u32; B],
    line: [u32; B],
    kind: [u8; B], // 1: a known type (and, for a purchase, a good price)
}

#[inline(always)]
unsafe fn ld(p: *const u8) -> __m256i {
    _mm256_loadu_si256(p as *const __m256i)
}

#[inline(always)]
unsafe fn ld2(a: *const u8, b: *const u8) -> __m256i {
    _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128(a as *const __m128i)), _mm_loadu_si128(b as *const __m128i), 1)
}

#[inline(always)]
unsafe fn load8(p: *const u8) -> u64 {
    (p as *const u64).read_unaligned()
}

/// Bit masks of the commas, newlines and quotes in the 96 bytes at `p`: the first
/// 64 bytes and the last 32 of each.
#[inline(always)]
unsafe fn masks(p: *const u8) -> [u64; 6] {
    let (v0, v1, v2) = (ld(p), ld(p.add(32)), ld(p.add(64)));
    let m = |ch: u8| -> (u64, u64) {
        let x = _mm256_set1_epi8(ch as i8);
        let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, x)) as u32 as u64;
        let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, x)) as u32 as u64;
        let c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, x)) as u32 as u64;
        (a | (b << 32), c)
    };
    let (c, n, q) = (m(b','), m(b'\n'), m(b'"'));
    [c.0, c.1, n.0, n.1, q.0, q.1]
}

/// The general per-line code on the line `[ls, le)` (`le` is its newline).
#[cold]
#[inline(never)]
unsafe fn cold(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) {
    w.ncold += 1;
    event_x_line(sh, w, d, ls, le);
}

/// The line starting at `ls`, wherever it ends.
#[cold]
#[inline(never)]
unsafe fn cold_from(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize) -> usize {
    let mut le = ls;
    while *d.add(le) != b'\n' {
        le += 1;
    }
    w.ncold += 1;
    event_x_line(sh, w, d, ls, le);
    le + 1
}

/// Finishes a batch: numbers from ID values, the window and lateness rules, and the
/// records filed under their event numbers.
#[inline(never)]
unsafe fn flush(sh: &Shared, fx: &Fx, dec: &Dec, w: &mut Worker, d: *const u8, bt: &Batch, n: usize) {
    let (ek, uk, limit, gmax) = fx.keys();
    let mut a = [0u64; B];
    let mut ix = [0u64; B];
    for j in 0..n {
        let (ne, nu) = (bt.ids[j][1], bt.ids[j][3]);
        let idx = unscramble(ne, ek);
        let un = unscramble(nu, uk);
        let out = (((ne | nu) >> 40) != 0) | (idx >= limit) | ((un >> 9) >= gmax);
        let t = bt.t[j];
        let ing_ms = (bt.ing[j] + dec.base_s) * 1000;
        let ok = (bt.kind[j] != 0) & ((t.wrapping_sub(WEEK) as u64) < dec.evspan) & (ing_ms.wrapping_sub(t.wrapping_add(dec.base)) <= DAY);
        a[j] = if ok { un } else { fast::INVALID };
        ix[j] = if out { u64::MAX } else { idx };
    }
    let lomask = (1u64 << fx.xsh) - 1;
    if sh.deccheck {
        check(sh, fx, w, d, bt, n, &a, &ix);
    }
    for j in 0..n {
        let idx = ix[j];
        if idx == u64::MAX {
            cold_from(sh, w, d, bt.line[j] as usize);
            continue;
        }
        let r = XRec { a: a[j], t: bt.t[j] as u32, pc: bt.pc[j], ing: (bt.ing[j] as i32 as u32) ^ 0x8000_0000, lo: (idx & lomask) as u32 };
        w.push_x(fx, (idx >> fx.xsh) as usize, r);
    }
}

/// Test hook: every row of the batch against the plain reading of its line.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
unsafe fn check(sh: &Shared, fx: &Fx, w: &mut Worker, d: *const u8, bt: &Batch, n: usize, a: &[u64; B], ix: &[u64; B]) {
    for j in 0..n {
        if ix[j] == u64::MAX {
            continue;
        }
        let ls = bt.line[j] as usize;
        let mut le = ls;
        while *d.add(le) != b'\n' {
            le += 1;
        }
        let mine = (ix[j], a[j], (bt.ing[j] as i32 as u32) ^ 0x8000_0000, (ix[j] & ((1u64 << fx.xsh) - 1)) as u32);
        let same = match crate::event_x_plain(sh, w, d, ls, le) {
            Some((idx, r)) => {
                (idx, r.a, r.ing, r.lo) == mine && (r.a == fast::INVALID || (r.t == bt.t[j] as u32 && r.pc == bt.pc[j]))
            }
            None => false,
        };
        if !same {
            eprintln!("decoder differs from the plain reading on: {:?}", String::from_utf8_lossy(std::slice::from_raw_parts(d.add(ls), le - ls)));
            std::process::exit(3);
        }
    }
}

/// Event lines of `d[start..end]` (whole lines; at least 64 readable bytes before
/// `d + start` and 128 after `d + end`).
#[inline(never)]
pub unsafe fn events(sh: &Shared, w: &mut Worker, d: *const u8, start: usize, end: usize) {
    let fx = sh.fx.as_ref().unwrap_unchecked();
    let dec: &Dec = sh.dec.as_ref().unwrap_unchecked();
    let mut bt: Batch = std::mem::zeroed();
    let mut nb = 0usize;
    let mut pos = start;
    let zero = _mm256_set1_epi8(b'0' as i8);
    let (idt, idlow, idrng) = (ld(dec.idt.as_ptr()), ld(dec.idlow.as_ptr()), ld(dec.idrng.as_ptr()));
    let (idw1, idw2, idw3) = (ld(dec.idw1.as_ptr()), ld(dec.idw2.as_ptr() as *const u8), ld(dec.idw3.as_ptr() as *const u8));
    let (c39, c10) = (_mm256_set1_epi8(39), _mm256_set1_epi8(10));
    let tens = _mm256_set1_epi16(0x010A); // bytes 10, 1
    while pos < end {
        let p = d.add(pos);
        let [cm, ch, nm, nh, qm, qh] = masks(p);
        if (nm | nh) == 0 {
            pos = cold_from(sh, w, d, pos); // a line longer than 96 bytes
            continue;
        }
        let nl = if nm != 0 { nm.trailing_zeros() as u64 } else { 64 + nh.trailing_zeros() as u64 };
        let next = pos + nl as usize + 1;

        // ---- Where the commas are. Fields 0..2 end in the first 64 bytes; the rest
        // of the line is looked at from the byte after the third comma.
        let m1 = _blsr_u64(cm);
        let m2 = _blsr_u64(m1);
        let m3 = _blsr_u64(m2);
        let (c1, c2, c3) = (_tzcnt_u64(cm), _tzcnt_u64(m1), _tzcnt_u64(m2));
        let c3s = (c3 & 63) as u32;
        let tsh = 63 - c3s;
        let t = ((cm >> c3s) >> 1) | (ch << tsh);
        let qt = ((qm >> c3s) >> 1) | (qh << tsh);
        let nlr = nl.wrapping_sub(c3).wrapping_sub(1);
        let tl = _bzhi_u64(t, nlr as u32);
        let t1 = _blsr_u64(tl);
        let t2 = _blsr_u64(t1);
        let t3 = _blsr_u64(t2);
        let (c4, c5, c6) = (_tzcnt_u64(tl), _tzcnt_u64(t1), _tzcnt_u64(t2));
        // Three commas in the first 64 bytes, three more and the newline in the next 64.
        if ((c3 | nlr | c6) >> 6) | t3 != 0 {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }

        // ---- Quotes: `o` has a bit per field that starts with one. Every quote must
        // be one of those or the last character of such a field, and each of those
        // must be there.
        let qa = _bzhi_u64(qm, c3 as u32);
        let sa = cm ^ m3;
        let fa = sa.wrapping_mul(2) | 1;
        let oa = _pext_u64(qa, fa);
        let mut bad = (qa & !fa) ^ _pdep_u64(oa, sa >> 1);
        let qb = _bzhi_u64(qt, nlr as u32);
        let sb = tl | (1u64 << nlr);
        let fb = sb.wrapping_mul(2) | 1;
        let ob = _pext_u64(qb, fb);
        bad |= (qb & !fb) ^ _pdep_u64(ob, sb >> 1);
        let (q0, q1, q2) = (oa & 1, (oa >> 1) & 1, oa >> 2);
        let (q3, q4, q5, q6) = (ob & 1, (ob >> 1) & 1, (ob >> 2) & 1, ob >> 3);
        let q = c3 + 1; // where `ingest_ts` starts

        // ---- Both IDs.
        let sh0 = dec.idsh.get_unchecked((2 * c1 + q0) as usize).as_ptr();
        let sh1 = dec.idsh.get_unchecked((2 * (c2 - c1 - 1) + q1) as usize).as_ptr();
        let v = ld2(p.add(c1 as usize).sub(16), p.add(c2 as usize).sub(16));
        let x = _mm256_shuffle_epi8(_mm256_sub_epi8(v, zero), ld2(sh0, sh1));
        // Letters (above 9 in a digit place) come down by 39 to 10..35; then every
        // byte must be in its range: a digit place 0..35 with letters not below 10,
        // the prefix place exactly its letter, the first-digit copy not '0'.
        let g = _mm256_cmpgt_epi8(x, idt);
        let val = _mm256_sub_epi8(x, _mm256_and_si256(g, c39));
        let chk = _mm256_sub_epi8(_mm256_sub_epi8(val, idlow), _mm256_and_si256(g, c10));
        let vbad = _mm256_subs_epu8(chk, idrng);
        let n2 = _mm256_madd_epi16(_mm256_maddubs_epi16(val, idw1), idw2);
        let idv = _mm256_add_epi64(_mm256_mul_epu32(n2, idw3), _mm256_srli_epi64(n2, 32));

        // ---- Both timestamps.
        let form = &*dec.forms.as_ptr().add(*dec.tsform.get_unchecked((2 * (c3 - c2 - 1) + q2) as usize) as usize);
        let tsp = p.add((c2 + 1 + q2) as usize);
        let inp = p.add((q + q3) as usize);
        bad |= form.bad | (c4.wrapping_sub(2 * q3) ^ 20);
        let v = ld2(tsp.offset(form.off), inp.add(10));
        let y = _mm256_and_si256(_mm256_sub_epi8(v, ld(form.lo.as_ptr())), ld(form.mk.as_ptr()));
        let vbad = _mm256_or_si256(vbad, _mm256_subs_epu8(y, ld(form.rg.as_ptr())));
        let pr = _mm256_maddubs_epi16(_mm256_shuffle_epi8(y, ld(form.shuf.as_ptr())), tens);
        let vbad = _mm256_or_si256(vbad, _mm256_cmpgt_epi16(pr, ld(form.wlim.as_ptr() as *const u8)));
        let s2 = _mm256_madd_epi16(pr, ld(form.w2.as_ptr() as *const u8));
        let s3 = _mm256_madd_epi16(_mm256_packus_epi32(s2, s2), ld(form.w3.as_ptr() as *const u8));
        let ab = _mm_cvtsi128_si64(_mm256_castsi256_si128(s3)) as u64;
        let isec = _mm_cvtsi128_si32(_mm256_extracti128_si256(s3, 1)) as u32 as i64;
        // Dates.
        let w1 = load8(tsp.add(2));
        let dt = &*dec.days.as_ptr().add(_pext_u64(w1, DAYBITS) as usize);
        bad |= ((load8(tsp) ^ dt.w0) | (w1 ^ dt.w1)) & form.dmask;
        let kb = *form.kb.get_unchecked(((*tsp.add(19) >> 1) & 1) as usize);
        let tms = (dt.tms & form.dmask as i64)
            .wrapping_add(form.c0)
            .wrapping_add((ab as u32 as i64).wrapping_mul(form.k0))
            .wrapping_add(((ab >> 32) as i64).wrapping_mul(kb));
        let w1 = load8(inp.add(2));
        let di = &*dec.days.as_ptr().add(_pext_u64(w1, DAYBITS) as usize);
        bad |= (load8(inp) ^ di.w0) | (w1 ^ di.w1);
        let ing = di.isec + isec;

        // ---- Type, product.
        let tw = load8(p.add((q + c4 + 1 + q4) as usize));
        let tl4 = (c5 - c4 - 1).wrapping_sub(2 * q4);
        let purchase = (tl4 == 8) & (tw == PURCHASE);
        let typed = purchase | ((tl4 == 4) & ((tw as u32 == VIEW) | (tw as u32 == CART)));
        bad |= ((c6 - c5 - 1) == 2 * q5) as u64; // an empty product

        if (bad != 0) | (_mm256_testz_si256(vbad, vbad) == 0) {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }
        let mut kind = typed as u8;
        let mut pc = 0u32;
        if purchase {
            let base = pos + q as usize;
            let (s5, e5) = (base + (c5 + 1 + q5) as usize, base + (c6 - q5) as usize);
            let (s6, e6) = (base + (c6 + 1 + q6) as usize, base + (nlr - q6) as usize);
            match parse_price(std::slice::from_raw_parts(d.add(s6), e6 - s6)) {
                Some(price) => pc = price | (sh.lk.products.get(Id::field(d, s5, e5)).unwrap_or(sh.lk.unknown_cat) << 24),
                None => kind = 0,
            }
        }
        _mm256_storeu_si256(bt.ids.as_mut_ptr().add(nb) as *mut __m256i, idv);
        *bt.t.get_unchecked_mut(nb) = tms;
        *bt.ing.get_unchecked_mut(nb) = ing;
        *bt.pc.get_unchecked_mut(nb) = pc;
        *bt.line.get_unchecked_mut(nb) = pos as u32;
        *bt.kind.get_unchecked_mut(nb) = kind;
        nb += 1;
        if nb == B {
            flush(sh, fx, dec, w, d, &bt, nb);
            nb = 0;
        }
        pos = next;
    }
    flush(sh, fx, dec, w, d, &bt, nb);
}
