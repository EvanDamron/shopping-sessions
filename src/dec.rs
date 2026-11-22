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
//!   event type    a table entry by first letter: the word and its length
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
//! outside the table, an unknown event type, an empty product), is handed whole to `cold`, which runs the
//! general per-line code on that line. So the result is that code's result on every
//! line; the decoder only decides how fast it is reached.

use crate::fast::{self, Fx, M1I, M2I};
use crate::{event_x_line, parse_price, Lookups, Shared, Worker, CART, DAY, PURCHASE, VIEW, WEEK};
use crate::id::Id;
use core::arch::x86_64::*;

/// Rows per batch.
const B: usize = 64;

/// How far ahead of the line being decoded the input is prefetched.
pub(crate) const AHEAD: usize = 1024;

/// How one form of `ts` is read: lane 0 of a 256-bit vector holds 16 bytes of the
/// `ts` field, lane 1 always holds `THH:MM:SSZ` of `ingest_ts`.
#[repr(C, align(64))]
pub(crate) struct Form {
    pub(crate) lo: [u8; 32],    // subtracted from each byte ...
    pub(crate) mk: [u8; 32],    // ... masked ...
    pub(crate) rg: [u8; 32],    // ... and then at most this: digits 0..9, separators exact
    pub(crate) shuf: [u8; 32],  // digits into pairs
    pub(crate) wlim: [i16; 16], // the most each pair may be
    pub(crate) w2: [i16; 16],
    pub(crate) w3: [i16; 16],
    pub(crate) off: isize,      // lane 0 starts this far from the field's first character
    pub(crate) k0: i64,         // time = c + k0 * first number + kb * second number
    pub(crate) kb: [i64; 2],    // (indexed by bit 1 of the character after the seconds)
    pub(crate) dmask: u64,      // all ones if the field starts with a date
    pub(crate) c0: i64,
    pub(crate) bad: u64,        // nonzero: no such form
    // For the forms chosen by the field's raw length (`Dec::forms2`): 1 if the field
    // is quoted, and which of the 16 bytes from one before its first character must
    // equal the day's text (the opening quote and the date, or just the date).
    q: u64,
    dm16: [u8; 16],
}

/// One day of the date table.
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Day {
    pub(crate) w0: u64,   // "YYYY-MM-"
    pub(crate) w1: u64,   // "YY-MM-DD"
    pub(crate) tms: i64,  // the day's start, ms since the click window's start
    isec: i64, // the day's start, seconds since the click window's start
    text: [u8; 16], // a quote and "YYYY-MM-DD"
    _pad: [u64; 2],
}

/// Bits of "YY-MM-DD" that tell the days of two neighbouring months apart: the
/// month's last digit (bit 0), the day's digits (2 and 4 bits).
pub(crate) const DAYBITS: u64 = (1 << 32) | (3 << 48) | (0xF << 56);

#[repr(C, align(64))]
pub struct Dec {
    // (All plain data: built zeroed and filled in by `new`.)
    pub(crate) forms: [Form; 4],
    /// Form of a `ts` field by 2 * raw length + quoted.
    pub(crate) tsform: [u8; 128],
    /// Shuffle for an ID field by 2 * raw length + quoted, applied to the 16 bytes
    /// that end where the field ends: digits right-aligned in bytes 8..16, the
    /// prefix letter in byte 7, the first digit again in byte 6.
    pub(crate) idsh: [[u8; 16]; 128],
    pub(crate) days: [Day; 128],
    idt: [u8; 32],
    idlow: [u8; 32],
    idrng: [u8; 32],
    idw1: [u8; 32],
    idw2: [i16; 16],
    idw3: [u32; 8],
    /// What the event type must be, by the low five bits of its first letter.
    ty: [Ty; 32],
    daybits: u64, // DAYBITS, where the hand-arranged loop can use it as an operand
    // For the hand-arranged loop, which takes the quoting of the IDs and of `ts`
    // from the field's raw length and checks the quotes with the field itself:
    /// The `ts` forms, bare and quoted (F1, F1 quoted, F2, F2 quoted, F3, F3 quoted, none).
    forms2: [Form; 7],
    /// Byte offset into `forms2` by the raw length of the `ts` field.
    tsrow: [u16; 64],
    /// By raw length, for an event ID (rows 0..64) and a user ID (rows 64..128): the
    /// shuffle of the 16 bytes that end where the field ends (digits to bytes
    /// 8..16, prefix to 7, first digit again to 6, the two quotes to 4 and 5), then
    /// what bytes 4..8 must be. Lengths 2..9 are read as bare, 10 and 11 as quoted.
    idr: [[u8; 32]; 128],
    /// The hand-arranged line loop is used (SS_ASM=0: the compiled one).
    pub asm: bool,
    /// Category by product number (`pcat_len` of them), for products named `p` +
    /// their number; NOCAT elsewhere. (The table lives as long as the process.)
    pcat: *const u8,
    pcat_len: usize,
    base: i64,
    base_s: i64,
    evspan: u64,
}

/// An event type: the first eight bytes of the field under `m` must be `w`, and its
/// length `len`.
#[repr(C)]
#[derive(Clone, Copy)]
struct Ty {
    w: u64,
    m: u64,
    len: u64,
    purchase: u64,
}

const MASK40: u64 = (1 << 40) - 1;
const NOCAT: u8 = 255;

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
    pub fn new(base: i64, end: i64, evspan: u64, lk: &Lookups) -> Box<Dec> {
        let base_s = base.div_euclid(1000);
        let mut dec: Box<Dec> = unsafe { Box::new(std::mem::zeroed()) };
        // ---- Products named `p` + a number without a leading zero (at most 7
        // digits), with a category that fits a byte: their category by that number.
        // A field of that form names exactly the product `p<its number>`.
        let mut pcat = Vec::new();
        for (id, cat) in lk.products.entries() {
            let b = id.bytes();
            if b.len() < 2 || b.len() > 8 || b[0] != b'p' || b[1] == b'0' || !b[1..].iter().all(u8::is_ascii_digit) || cat >= NOCAT as u32 {
                continue;
            }
            let n = b[1..].iter().fold(0usize, |n, &c| n * 10 + (c - b'0') as usize);
            if n >= pcat.len() {
                pcat.resize(n + 1, NOCAT);
            }
            pcat[n] = cat as u8;
        }
        let pcat: &'static [u8] = Box::leak(pcat.into_boxed_slice());
        (dec.pcat, dec.pcat_len) = (pcat.as_ptr(), pcat.len());
        dec.base = base;
        dec.base_s = base_s;
        dec.evspan = evspan;
        dec.daybits = DAYBITS;
        dec.asm = std::env::var_os("SS_ASM").is_none_or(|v| v != "0");

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

        // The same forms by raw length: bare as above, and quoted with the closing
        // quote checked in the lane (F1: after the Z; F2: after the offset; F3: the
        // lane starts one byte later, the opening quote before the digits and the
        // closing one after them). The opening quote of F1 and F2 is checked with
        // the date.
        for k in 0..7 {
            let src = &dec.forms[(k / 2).min(3)] as *const Form;
            unsafe { std::ptr::copy_nonoverlapping(src, &mut dec.forms2[k], 1) };
            let f = &mut dec.forms2[k];
            let quoted = k % 2 == 1 && k < 6;
            f.q = quoted as u64;
            if k < 4 {
                for i in 1..11 {
                    f.dm16[i] = 0xFF;
                }
                f.dm16[0] = if quoted { 0xFF } else { 0 };
                if quoted {
                    let at = if k == 1 { 10 } else { 15 };
                    (f.lo[at], f.mk[at], f.rg[at]) = (b'"', 0xFF, 0);
                }
            } else if k == 5 {
                f.off = -2;
                for i in 0..16 {
                    (f.lo[i], f.mk[i], f.rg[i]) = match i {
                        0 => (0, 0, 0),
                        1 | 15 => (b'"', 0xFF, 0),
                        _ => (b'0', 0xFF, 9),
                    };
                    f.shuf[i] = if i >= 3 { i as u8 - 1 } else { 0x80 };
                }
            }
        }
        for r in 0..64 {
            let k = match r {
                20 => 0,
                22 => 1,
                25 => 2,
                27 => 3,
                13 => 4,
                15 => 5,
                _ => 6,
            };
            dec.tsrow[r] = (k * std::mem::size_of::<Form>()) as u16;
        }

        // ---- IDs by raw length.
        for i in 0..128 {
            let (kind, raw) = (i / 64, i % 64);
            let row = &mut dec.idr[i];
            for b in row.iter_mut().take(16) {
                *b = 0x80;
            }
            row[16 + 6] = 1;
            row[16 + 7] = [b'e', b'u'][kind] - b'0';
            let (q, nd) = match raw {
                2..=9 => (0, raw - 1),
                10 | 11 => (1, raw - 3),
                _ => continue, // no letter and 1 to 8 digits: the prefix check fails
            };
            for j in 0..nd {
                row[15 - j] = (15 - q - j) as u8;
            }
            row[7] = (15 - q - nd) as u8;
            row[6] = if nd >= 2 { (16 - q - nd) as u8 } else { (15 - q - nd) as u8 };
            if q == 1 {
                (row[4], row[5]) = ((13 - nd) as u8, 15);
                (row[16 + 4], row[16 + 5]) = (b'"'.wrapping_sub(b'0'), b'"'.wrapping_sub(b'0'));
            }
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
                dec.idrng[o + i] = if i == 4 || i == 5 { 0 } else { 255 };
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

        // ---- Event types. (An entry no type uses asks for an impossible length.)
        for i in 0..32 {
            dec.ty[i] = Ty { w: 0, m: 0, len: u64::MAX, purchase: 0 };
        }
        dec.ty[(b'v' & 31) as usize] = Ty { w: VIEW as u64, m: 0xFFFF_FFFF, len: 4, purchase: 0 };
        dec.ty[(b'c' & 31) as usize] = Ty { w: CART as u64, m: 0xFFFF_FFFF, len: 4, purchase: 0 };
        dec.ty[(b'p' & 31) as usize] = Ty { w: PURCHASE, m: u64::MAX, len: 8, purchase: 1 };

        // ---- Dates. A `ts` in the click window has a local date within a day of it;
        // an `ingest_ts` is usually within a week after. A day that is not here (or
        // that would share a key with one that is) is simply left to `cold`.
        for i in 0..128 {
            // (An unused entry holds text that would have another key, so nothing
            // that is looked up here equals it.)
            let fill = if i == 0 { 0xFF } else { 0 };
            dec.days[i] = Day { w0: 0, w1: if i == 0 { u64::MAX } else { 0 }, tms: 0, isec: 0, text: [fill; 16], _pad: [0; 2] };
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
                let mut text = [0u8; 16];
                text[0] = b'"';
                text[1..11].copy_from_slice(&t[0..10]);
                *e = Day { w0, w1, tms: day * DAY - base, isec: day * 86_400 - base_s, text, _pad: [0; 2] };
            }
        }
        dec
    }
}

#[repr(C, align(64))]
struct Batch {
    ids: [[u64; 2]; B], // event ID value, user ID value
    t: [i64; B],        // ts, ms since the click window's start (i64::MIN: a purchase with a bad price)
    ing: [i64; B],      // ingest_ts, seconds since the click window's start
    pc: [u64; B],       // price and category of a purchase with a good price, else 0
    // Filled by `flush`:
    ix: [u64; B], // event number, u64::MAX if either ID's number is out of range
    un: [u64; B], // user number
    // The record's two words (fast::XRec): user word with the event number within
    // the bucket on top; time, BUY and ingest key. And the price and category for
    // the extension record of a valid purchase (else 0).
    a: [u64; B],
    w1: [u64; B],
    px: [u64; B],
    lp: [u64; B], // where the row's line starts (address)
    // For `lines_asm`: the line after each row's (address), which rows are
    // purchases, the lines queued for the plain reading, and a few words it keeps
    // to hand.
    nx: [u64; B],
    coldp: [u64; B],
    pur: [u8; B],
    nc: u64,
    scr: [u64; 5],
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

/// The general per-line code on the line `[ls, le)` (`le` is its newline).
#[cold]
#[inline(never)]
unsafe fn cold(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, _le: usize) {
    cold_from(sh, w, d, ls);
}

/// The seven fields of the line `d[ls..le]`, by the rules `split` applies (six
/// commas; a field has no quote, or exactly two that wrap it), found from masks
/// instead of a byte loop: Some(None) for a malformed line, None for a line of 96
/// bytes or more (left to `split`).
#[inline(always)]
unsafe fn fields7(d: *const u8, ls: usize, le: usize) -> Option<Option<[(usize, usize); 7]>> {
    let len = le - ls;
    if len >= 96 {
        return None;
    }
    let p = d.add(ls);
    let (v0, v1, v2) = (ld(p), ld(p.add(32)), ld(p.add(64)));
    let m = |c: u8| -> u128 {
        let x = _mm256_set1_epi8(c as i8);
        let f = |v: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, x)) as u32 as u128;
        (f(v0) | (f(v1) << 32) | (f(v2) << 64)) & ((1u128 << len) - 1)
    };
    let (mut c, q) = (m(b','), m(b'"'));
    if c.count_ones() != 6 {
        return Some(None);
    }
    let mut f = [(0usize, 0usize); 7];
    let mut s = 0usize;
    for g in f.iter_mut() {
        let e = if c != 0 { c.trailing_zeros() as usize } else { len };
        c &= c.wrapping_sub(1);
        let quotes = (q >> s) & ((1u128 << (e - s)) - 1);
        *g = if quotes == 0 {
            (ls + s, ls + e)
        } else if e - s >= 2 && quotes == 1 | (1u128 << (e - s - 1)) {
            (ls + s + 1, ls + e - 1)
        } else {
            return Some(None);
        };
        s = e + 1;
    }
    Some(Some(f))
}

/// The general per-line code on the line starting at `ls`, wherever it ends;
/// returns where the next line starts.
#[cold]
#[inline(never)]
unsafe fn cold_from(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize) -> usize {
    let mut le = ls;
    while *d.add(le) != b'\n' {
        le += 1;
    }
    w.ncold += 1;
    match fields7(d, ls, le) {
        Some(found) => {
            if sh.deccheck && found != crate::plain_fields::<7>(d, ls, le) {
                eprintln!("fields differ from the plain reading on: {:?}", String::from_utf8_lossy(std::slice::from_raw_parts(d.add(ls), le - ls)));
                std::process::exit(3);
            }
            if let Some(f) = found {
                crate::event_x_fields(sh, sh.fx.as_ref().unwrap_unchecked(), w, d, &f);
            }
        }
        None => event_x_line(sh, w, d, ls, le),
    }
    le + 1
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// The number written by the `len` (1 to 8) digits that end at `end`; None if one of
/// them is not a digit.
#[inline(always)]
unsafe fn digits(end: *const u8, len: usize) -> Option<u64> {
    // The digits are the top `len` bytes of the 8 that end there; '0's fill the rest.
    let keep = u64::MAX << (8 * (8 - len));
    let w = (load8(end.sub(8)) & keep) | (ZEROS & !keep);
    let x = w ^ ZEROS;
    if (x.wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    // Pairs, then fours, then all eight (the first digit is the lowest byte).
    let v = x.wrapping_mul(10).wrapping_add(x >> 8);
    let v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32)));
    Some(v >> 32)
}

/// Price and category of the purchase on the well-formed line that ends at `le`, as
/// `price | category << 24`; None for a price the rules do not accept. (About 3% of
/// lines: the two fields are found again here rather than kept in the loop.)
#[inline(never)]
unsafe fn purchase(sh: &Shared, dec: &Dec, d: *const u8, le: usize) -> Option<u32> {
    // The last two commas, from the 32 bytes that end with the newline (the line has
    // six commas; if the last two fields do not fit there, a byte loop finds them).
    let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(ld(d.add(le).sub(31)), _mm256_set1_epi8(b',' as i8))) as u32;
    let rest = cm & (0x7FFF_FFFFu32 >> cm.leading_zeros().min(31));
    let c = if rest != 0 {
        [le - cm.leading_zeros() as usize, le - rest.leading_zeros() as usize]
    } else {
        let (mut c, mut i) = ([0usize; 2], le);
        for k in 0..2 {
            i -= 1;
            while *d.add(i) != b',' {
                i -= 1;
            }
            c[k] = i;
        }
        c
    };
    // price_cents is after the last comma, product_id before it.
    let (mut s6, mut e6) = (c[0] + 1, le);
    if e6 > s6 && *d.add(s6) == b'"' {
        (s6, e6) = (s6 + 1, e6 - 1);
    }
    let (mut s5, mut e5) = (c[1] + 1, c[0]);
    if *d.add(s5) == b'"' {
        (s5, e5) = (s5 + 1, e5 - 1);
    }
    let price = if e6 - s6 - 1 < 8 {
        match digits(d.add(e6), e6 - s6) {
            Some(v) if v >= 1 && v <= 10_000_000 => v as u32,
            _ => return None,
        }
    } else {
        parse_price(std::slice::from_raw_parts(d.add(s6), e6 - s6))?
    };
    // The category: from the table by product number for `p` + a number that is
    // there, from the lookup map for anything else.
    let nd = e5 - s5 - 1;
    if nd.wrapping_sub(1) < 7 && *d.add(s5) == b'p' && *d.add(s5 + 1) != b'0' {
        if let Some(n) = digits(d.add(e5), nd) {
            let n = n as usize;
            if n < dec.pcat_len && *dec.pcat.add(n) != NOCAT {
                return Some(price | ((*dec.pcat.add(n) as u32) << 24));
            }
        }
    }
    Some(price | (sh.lk.products.get(Id::field(d, s5, e5)).unwrap_or(sh.lk.unknown_cat) << 24))
}

/// `a * m` modulo 2^40 in each 64-bit lane, for `a` below 2^40; `mlo` and `mhi` hold
/// the low 32 bits and the next 8 bits of `m`.
#[inline(always)]
unsafe fn mul40(a: __m256i, mlo: __m256i, mhi: __m256i, mask: __m256i) -> __m256i {
    let cross = _mm256_add_epi64(_mm256_mul_epu32(a, mhi), _mm256_mul_epu32(_mm256_srli_epi64(a, 32), mlo));
    _mm256_and_si256(_mm256_add_epi64(_mm256_mul_epu32(a, mlo), _mm256_slli_epi64(cross, 32)), mask)
}

/// Finishes a batch: numbers from ID values, the window and lateness rules, and the
/// records filed under their event numbers.
#[inline(never)]
unsafe fn flush(sh: &Shared, fx: &Fx, dec: &Dec, w: &mut Worker, d: *const u8, bt: &mut Batch, n: usize) {
    let (ek, uk, limit, gmax) = fx.keys();
    // ---- Two rows at a time: (event ID value, user ID value) -> (event number, user
    // number), as fast::unscramble does; a row with either number out of range gets
    // event number u64::MAX.
    let lane = |x: u64, y: u64| _mm256_set_epi64x(y as i64, x as i64, y as i64, x as i64);
    let mask = _mm256_set1_epi64x(MASK40 as i64);
    let (m2lo, m2hi) = (_mm256_set1_epi64x((M2I & 0xFFFF_FFFF) as i64), _mm256_set1_epi64x(((M2I >> 32) & 0xFF) as i64));
    let (m1lo, m1hi) = (_mm256_set1_epi64x((M1I & 0xFFFF_FFFF) as i64), _mm256_set1_epi64x(((M1I >> 32) & 0xFF) as i64));
    let keys = lane(ek, uk);
    let top = lane(limit.min(MASK40 + 1) - 1, (gmax << 9).min(MASK40 + 1) - 1);
    let mut j = 0;
    while j < n {
        let v = ld(bt.ids.as_ptr().add(j) as *const u8);
        let m = mul40(_mm256_and_si256(v, mask), m2lo, m2hi, mask);
        let m = _mm256_xor_si256(_mm256_xor_si256(m, _mm256_srli_epi64(m, 17)), _mm256_srli_epi64(m, 34));
        let x = mul40(_mm256_xor_si256(m, keys), m1lo, m1hi, mask);
        // (values are below 2^63, so the signed compares are the unsigned ones)
        let out = _mm256_or_si256(_mm256_cmpgt_epi64(v, mask), _mm256_cmpgt_epi64(x, top));
        let out = _mm256_or_si256(out, _mm256_shuffle_epi32(out, 0b01_00_11_10));
        let r = _mm256_permute4x64_epi64(_mm256_or_si256(x, _mm256_and_si256(out, lane(u64::MAX, 0))), 0b11_01_10_00);
        _mm_storeu_si128(bt.ix.as_mut_ptr().add(j) as *mut __m128i, _mm256_castsi256_si128(r));
        _mm_storeu_si128(bt.un.as_mut_ptr().add(j) as *mut __m128i, _mm256_extracti128_si256(r, 1));
        j += 2;
    }
    // ---- Four rows at a time: in the event window and not late -> the user number,
    // otherwise INVALID.
    const BIAS: i64 = 2 * 86_400; // ingest seconds are at least minus one day: made positive for the multiply
    let week = _mm256_set1_epi64x(WEEK);
    let span = _mm256_set1_epi64x(dec.evspan.min(i64::MAX as u64) as i64);
    let c = _mm256_set1_epi64x(dec.base_s * 1000 - dec.base - BIAS * 1000);
    let low32 = _mm256_set1_epi64x(0xFFFF_FFFF);
    let m31 = _mm256_set1_epi64x(0x7FFF_FFFF);
    let lomask = _mm256_set1_epi64x(((1u64 << fx.xsh) - 1) as i64);
    let mut j = 0;
    while j < n {
        let t = ld(bt.t.as_ptr().add(j) as *const u8);
        let tw = _mm256_sub_epi64(t, week);
        let inwin = _mm256_and_si256(_mm256_cmpgt_epi64(tw, _mm256_set1_epi64x(-1)), _mm256_cmpgt_epi64(span, tw));
        let ing0 = ld(bt.ing.as_ptr().add(j) as *const u8);
        let ing = _mm256_add_epi64(ing0, _mm256_set1_epi64x(BIAS));
        let ing_ms = _mm256_add_epi64(_mm256_mul_epu32(ing, _mm256_set1_epi64x(1000)), c);
        let late = _mm256_cmpgt_epi64(_mm256_sub_epi64(ing_ms, t), _mm256_set1_epi64x(DAY));
        let ok = _mm256_andnot_si256(late, inwin);
        let a = _mm256_blendv_epi8(_mm256_set1_epi64x(fast::INVALID as i64), ld(bt.un.as_ptr().add(j) as *const u8), ok);
        let lo = _mm256_slli_epi64(_mm256_and_si256(ld(bt.ix.as_ptr().add(j) as *const u8), lomask), fast::XLO as i32);
        _mm256_storeu_si256(bt.a.as_mut_ptr().add(j) as *mut __m256i, _mm256_or_si256(a, lo));
        // Price and category count for a valid row only; BUY says there is one.
        let px = _mm256_and_si256(ld(bt.pc.as_ptr().add(j) as *const u8), ok);
        _mm256_storeu_si256(bt.px.as_mut_ptr().add(j) as *mut __m256i, px);
        let buy = _mm256_andnot_si256(_mm256_cmpeq_epi64(px, _mm256_setzero_si256()), _mm256_set1_epi64x(fast::BUY as i64));
        let key = _mm256_slli_epi64(_mm256_xor_si256(_mm256_and_si256(ing0, low32), _mm256_set1_epi64x(0x8000_0000)), 32);
        let w1 = _mm256_or_si256(_mm256_or_si256(_mm256_and_si256(t, m31), buy), key);
        _mm256_storeu_si256(bt.w1.as_mut_ptr().add(j) as *mut __m256i, w1);
        j += 4;
    }
    if sh.deccheck {
        check(sh, w, d, bt, n);
    }
    // ---- The records, filed as `Worker::push_x` files them (the same buffers and
    // chunks; written out here so that nothing is re-read from the worker per row).
    let xsh = fx.xsh;
    let (cnt, cur, wc) = (w.xcnt.as_mut_ptr(), w.xcur.as_mut_ptr(), w.xwc.as_mut_ptr() as *mut u64);
    for j in 0..n {
        let idx = *bt.ix.get_unchecked(j);
        if idx == u64::MAX {
            cold_from(sh, w, d, (bt.lp[j] - d as u64) as usize);
            continue;
        }
        let b = (idx >> xsh) as usize;
        let (x0, x1) = (*bt.a.get_unchecked(j), *bt.w1.get_unchecked(j));
        let nrec = *cnt.add(b);
        let off = nrec as usize & (fast::XCH - 1);
        if off == 0 {
            new_chunk(fx, w, b);
        }
        *cnt.add(b) = nrec + 1;
        let k = (nrec & 7) as usize;
        let buf = wc.add(b * 16);
        *buf.add(k * 2) = x0;
        *buf.add(k * 2 + 1) = x1;
        if k == 7 {
            crate::mem::stream((*cur.add(b) as *mut u8).add((off - 7) * fast::XREC), buf as *const u8, 2);
        }
        // A valid purchase: its extension record next (same place and ingest key,
        // the price and category where the time was).
        let px = *bt.px.get_unchecked(j);
        if px != 0 {
            extension(fx, w, b, fast::EXT | (x0 & !fast::XA), (x1 & !0xFFFF_FFFF) | px);
        }
    }
}

/// Files the extension record of a purchase (3% of events) under bucket `b`.
#[inline(never)]
unsafe fn extension(fx: &Fx, w: &mut Worker, b: usize, x0: u64, x1: u64) {
    let nrec = *w.xcnt.get_unchecked(b);
    let off = nrec as usize & (fast::XCH - 1);
    if off == 0 {
        new_chunk(fx, w, b);
    }
    *w.xcnt.get_unchecked_mut(b) = nrec + 1;
    let k = (nrec & 7) as usize;
    let buf = (w.xwc.as_mut_ptr() as *mut u64).add(b * 16);
    *buf.add(k * 2) = x0;
    *buf.add(k * 2 + 1) = x1;
    if k == 7 {
        crate::mem::stream((*w.xcur.get_unchecked(b) as *mut u8).add((off - 7) * fast::XREC), buf as *const u8, 2);
    }
}

/// A new chunk for event-number bucket `b`.
#[cold]
#[inline(never)]
unsafe fn new_chunk(fx: &Fx, w: &mut Worker, b: usize) {
    let c = fx.xe.alloc(fast::XCH * fast::XREC) as usize;
    *w.xcur.get_unchecked_mut(b) = c;
    w.xchunks.get_unchecked_mut(b).push(c);
}

/// Test hook: every row of the batch against the plain reading of its line.
#[cold]
#[inline(never)]
unsafe fn check(sh: &Shared, w: &mut Worker, d: *const u8, bt: &Batch, n: usize) {
    for j in 0..n {
        if bt.ix[j] == u64::MAX {
            continue;
        }
        let ls = (bt.lp[j] - d as u64) as usize;
        let mut le = ls;
        while *d.add(le) != b'\n' {
            le += 1;
        }
        // (the words exactly as they are filed, against the plain reading packed the same way)
        let same = match crate::event_x_plain(sh, w, d, ls, le) {
            Some((idx, r)) => {
                let (x, ext) = r.pack();
                idx == bt.ix[j]
                    && (x.a, x.ing) == (bt.a[j], (bt.w1[j] >> 32) as u32)
                    && (r.a == fast::INVALID || x.t == bt.w1[j] as u32)
                    && ext.map_or(0, |e| e.t as u64) == bt.px[j]
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
    let (comma, newline, quote) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
    let (idt, idlow, idrng) = (ld(dec.idt.as_ptr()), ld(dec.idlow.as_ptr()), ld(dec.idrng.as_ptr()));
    let (idw1, idw2, idw3) = (ld(dec.idw1.as_ptr()), ld(dec.idw2.as_ptr() as *const u8), ld(dec.idw3.as_ptr() as *const u8));
    let (c39, c10) = (_mm256_set1_epi8(39), _mm256_set1_epi8(10));
    let tens = _mm256_set1_epi16(0x010A); // bytes 10, 1
    while pos < end {
        let p = d.add(pos);
        // (Read from a mapping, the lines come straight from the page cache: ask for
        // them a few lines early. From the read buffer this finds them in cache.)
        _mm_prefetch::<_MM_HINT_T0>(p.add(AHEAD) as *const i8);
        // ---- Masks of the commas, newlines and quotes of the 96 bytes at p: the
        // first 64 bytes and the last 32 of each.
        let (v0, v1, v2) = (ld(p), ld(p.add(32)), ld(p.add(64)));
        let m64 = |x: __m256i| -> u64 {
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, x)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, x)) as u32 as u64) << 32)
        };
        let m32 = |x: __m256i| -> u64 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, x)) as u32 as u64 };
        let (nm, nh) = (m64(newline), m32(newline));
        if (nm | nh) == 0 {
            pos = cold_from(sh, w, d, pos); // a line longer than 96 bytes
            continue;
        }
        let nl = if nm != 0 { _tzcnt_u64(nm) } else { 64 + _tzcnt_u64(nh) };
        let next = pos + nl as usize + 1;
        let (cm, ch, qm, qh) = (m64(comma), m32(comma), m64(quote), m32(quote));

        // ---- Where the commas are. Fields 0..2 end in the first 64 bytes; the rest
        // of the line is looked at from the byte after the third comma.
        let m1 = _blsr_u64(cm);
        let m2 = _blsr_u64(m1);
        let (c1, c2, c3) = (_tzcnt_u64(cm), _tzcnt_u64(m1), _tzcnt_u64(m2));
        let sa = cm ^ _blsr_u64(m2);
        let qa = _bzhi_u64(qm, c3 as u32);
        let c3s = (c3 & 63) as u32;
        let nlr = nl.wrapping_sub(c3).wrapping_sub(1);
        let tl = _bzhi_u64(((cm >> c3s) >> 1) | (ch << (63 - c3s)), nlr as u32);
        let qb = _bzhi_u64(((qm >> c3s) >> 1) | (qh << (63 - c3s)), nlr as u32);
        let t1 = _blsr_u64(tl);
        let t2 = _blsr_u64(t1);
        let (c4, c5, c6) = (_tzcnt_u64(tl), _tzcnt_u64(t1), _tzcnt_u64(t2));
        // Three commas in the first 64 bytes, three more and the newline in the next 64.
        if ((c3 | nlr | c6) >> 6) | _blsr_u64(t2) != 0 {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }

        // ---- The second half of the line: ingest_ts, type, product, price.
        // Quotes: `o` has a bit per field that starts with one. Every quote must be
        // one of those or the last character of such a field, and each of those must
        // be there.
        let sb = tl | (1u64 << nlr);
        let fb = sb.wrapping_mul(2) | 1;
        let ob = _pext_u64(qb, fb);
        let mut bad = (qb & !fb) ^ _pdep_u64(ob, sb >> 1);
        let (q3, q4, q5) = (ob & 1, (ob >> 1) & 1, (ob >> 2) & 1);
        let tail = p.add(c3 as usize + 1); // where `ingest_ts` starts
        let inp = tail.add(q3 as usize);
        bad |= c4.wrapping_sub(2 * q3) ^ 20;
        // Type: the entry for its first letter says what the word and its length must be.
        let tw = load8(tail.add((c4 + 1 + q4) as usize));
        let ty = &*dec.ty.as_ptr().add((tw & 31) as usize);
        bad |= ((tw ^ ty.w) & ty.m) | ((c5 - c4 - 1).wrapping_sub(2 * q4) ^ ty.len);
        let is_purchase = ty.purchase;
        bad |= ((c6 - c5 - 1) == 2 * q5) as u64; // an empty product

        // ---- The first half: both IDs and ts.
        let fa = sa.wrapping_mul(2) | 1;
        let oa = _pext_u64(qa, fa);
        bad |= (qa & !fa) ^ _pdep_u64(oa, sa >> 1);
        let (q0, q1, q2) = (oa & 1, (oa >> 1) & 1, oa >> 2);
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
        bad |= form.bad;
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
        let mut tms = (dt.tms & form.dmask as i64)
            .wrapping_add(form.c0)
            .wrapping_add((ab as u32 as i64).wrapping_mul(form.k0))
            .wrapping_add(((ab >> 32) as i64).wrapping_mul(kb));
        let w1 = load8(inp.add(2));
        let di = &*dec.days.as_ptr().add(_pext_u64(w1, DAYBITS) as usize);
        bad |= (load8(inp) ^ di.w0) | (w1 ^ di.w1);
        let ing = di.isec + isec;

        if bad != 0 {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }
        if _mm256_testz_si256(vbad, vbad) == 0 {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }
        let mut pc = 0u64;
        if is_purchase != 0 {
            match purchase(sh, dec, d, next - 1) {
                Some(x) => pc = x as u64,
                None => tms = i64::MIN, // outside every window: the copy is invalid
            }
        }
        _mm_storeu_si128(bt.ids.as_mut_ptr().add(nb) as *mut __m128i, _mm256_castsi256_si128(_mm256_permute4x64_epi64(idv, 0b11_01_11_01)));
        *bt.t.get_unchecked_mut(nb) = tms;
        *bt.ing.get_unchecked_mut(nb) = ing;
        *bt.pc.get_unchecked_mut(nb) = pc;
        *bt.lp.get_unchecked_mut(nb) = p as u64;
        nb += 1;
        if nb == B {
            flush(sh, fx, dec, w, d, &mut bt, nb);
            nb = 0;
        }
        pos = next;
    }
    flush(sh, fx, dec, w, d, &mut bt, nb);
}

/// Up to B lines from `p` decoded into the batch by the hand-arranged loop:
/// returns where it stopped, the rows it filled, and whether it stopped at a line
/// with no newline in its first 96 bytes. It does what `events`' line loop does (same
/// masks for commas and newlines, same lane arithmetic, same batch columns) with the
/// registers assigned by hand, because the compiled loop spends about a fifth of
/// its instructions moving values to and from the stack. Two things differ from the
/// compiled loop, both ways of doing less for the same answer:
///
///   - The quoting of the two IDs and of `ts` is not read from a quote mask. Each
///     field's raw length says what it must be (an ID of 2..9 bytes bare, of 10 or
///     11 quoted; `ts` of 20, 25, 13 bare, of 22, 27, 15 quoted), the table row for
///     that length routes the quote bytes into the lane, and the lane checks them
///     with every other byte of the field. A field that is something else (a
///     quoted ID with fewer than 7 digits, say) fails its check and the line goes
///     to the plain reading.
///   - The quotes of the last four fields come from one mask of the 64 bytes that
///     end with the newline, and are checked as in `events`.
///
/// Nothing is called from here: a line for the plain reading is queued in
/// `bt.coldp`, a purchase is marked in `bt.pur`, and the loop returns when the batch
/// or the queue is full.
///
/// Registers: rdi tables, rsi line, rdx end, r13 batch, r15 rows, r14 rows * 16;
/// rax rbx rcx rbp r8-r12 are scratch; ymm3-ymm15 hold constants, ymm0-ymm2 scratch.
#[inline(never)]
unsafe fn lines_asm(dec: &Dec, p: *const u8, pend: *const u8, bt: &mut Batch) -> (*const u8, usize, bool) {
    use core::mem::offset_of;
    let (stop, rows, long): (*const u8, usize, u64);
    core::arch::asm!(
        "push rbx",
        "push rbp",
        "xor r15d, r15d",
        "xor r14d, r14d",
        // ---- next line
        "2:",
        "cmp rsi, rdx",
        "jae 90f",
        "prefetcht0 byte ptr [rsi + {ahead}]",
        // newline: nl in rax (96 or more: none in the first 96 bytes)
        "vpcmpeqb ymm0, ymm14, ymmword ptr [rsi]",
        "vpcmpeqb ymm1, ymm14, ymmword ptr [rsi + 32]",
        "vpcmpeqb ymm2, ymm14, ymmword ptr [rsi + 64]",
        "vpmovmskb eax, ymm0",
        "vpmovmskb ecx, ymm1",
        "vpmovmskb r8d, ymm2",
        "shl rcx, 32",
        "or rax, rcx",
        "tzcnt r8, r8",
        "add r8, 64",
        "tzcnt rax, rax",
        "cmovc rax, r8",
        "cmp rax, 96",
        "jae 80f",
        // commas of the first 64 bytes: r8
        "vpcmpeqb ymm0, ymm15, ymmword ptr [rsi]",
        "vpcmpeqb ymm1, ymm15, ymmword ptr [rsi + 32]",
        "vpmovmskb r8d, ymm0",
        "vpmovmskb ecx, ymm1",
        "shl rcx, 32",
        "or r8, rcx",
        // c1 rcx, c2 rbx, c3 r12
        "blsr rbx, r8",
        "tzcnt rcx, r8",
        "blsr rbp, rbx",
        "tzcnt rbx, rbx",
        "tzcnt r12, rbp",
        "cmp r12, 64",
        "jae 70f",
        // ---- both IDs: their bytes, and by each field's length its shuffle and
        // what the prefix and quote places must hold
        "vmovdqu xmm0, xmmword ptr [rsi + rcx - 16]",
        "vinserti128 ymm0, ymm0, xmmword ptr [rsi + rbx - 16], 1",
        "lea rbp, [rcx*4]",
        "vmovdqu ymm1, ymmword ptr [rdi + rbp*8 + {idr}]",
        "mov rbp, rbx",
        "sub rbp, rcx",
        "shl rbp, 5",
        "vmovdqu ymm2, ymmword ptr [rdi + rbp + {idr} + 63*32]",
        "vperm2i128 ymm8, ymm1, ymm2, 0x31",
        "vperm2i128 ymm1, ymm1, ymm2, 0x20",
        "vpsubb ymm0, ymm0, ymm12",
        "vpshufb ymm0, ymm0, ymm1",
        "vpcmpgtb ymm1, ymm0, ymm11",
        "vpand ymm2, ymm1, ymm10",
        "vpsubb ymm0, ymm0, ymm2",
        "vpand ymm1, ymm1, ymm9",
        "vpsubb ymm2, ymm0, ymm8",
        "vpsubb ymm2, ymm2, ymm1",
        "vpsubusb ymm2, ymm2, ymm7",
        "vpmaddubsw ymm0, ymm0, ymm6",
        "vpmaddwd ymm0, ymm0, ymm5",
        "vpmuludq ymm1, ymm0, ymm4",
        "vpsrlq ymm0, ymm0, 32",
        "vpaddq ymm0, ymm0, ymm1",
        "vpermq ymm0, ymm0, 0xDD",
        "vmovdqu xmmword ptr [r13 + r14 + {ids}], xmm0",
        // ---- ts: its form by the field's length (rcx), where its text starts
        // (rbx), its 16 bytes (xmm1), its date, the constant part of the time and
        // the multiplier of the second number
        "mov rcx, r12",
        "sub rcx, rbx",
        "movzx ecx, word ptr [rdi + rcx*2 + {tsrow} - 2]",
        "lea rcx, [rdi + rcx + {forms2}]",
        "mov r11, qword ptr [rcx + {f_bad}]",
        "mov rbp, qword ptr [rcx + {f_q}]",
        "lea rbx, [rbx + rbp + 1]",
        "add rbx, rsi",
        "mov rbp, qword ptr [rcx + {f_off}]",
        "vmovdqu xmm1, xmmword ptr [rbx + rbp]",
        "mov rbp, qword ptr [rbx + 2]",
        "pext rbp, rbp, qword ptr [rdi + {daybits}]",
        "shl rbp, 6",
        "vmovdqu xmm0, xmmword ptr [rbx - 1]",
        "vpxor xmm0, xmm0, xmmword ptr [rdi + rbp + {d_text}]",
        "vpand xmm0, xmm0, xmmword ptr [rcx + {f_dm16}]",
        "vpor ymm2, ymm2, ymm0",
        "mov r10, qword ptr [rdi + rbp + {d_tms}]",
        "and r10, qword ptr [rcx + {f_dmask}]",
        "add r10, qword ptr [rcx + {f_c0}]",
        "mov qword ptr [r13 + {scr} + 16], r10",
        "movzx r10d, byte ptr [rbx + 19]",
        "and r10d, 2",
        "mov r10, qword ptr [rcx + r10*4 + {f_kb}]",
        "mov qword ptr [r13 + {scr} + 24], r10",
        "mov qword ptr [r13 + {scr} + 32], rcx",
        // ---- the rest of the line, from the 64 bytes that end with the newline:
        // its commas (r10) and quotes (r8) moved so that bit 0 is the byte after the
        // third comma; nlr = nl - c3 - 1 (r9) is where the newline lands
        "vpcmpeqb ymm0, ymm15, ymmword ptr [rsi + rax - 63]",
        "vpcmpeqb ymm8, ymm15, ymmword ptr [rsi + rax - 31]",
        "vpmovmskb r10d, ymm0",
        "vpmovmskb ebx, ymm8",
        "shl rbx, 32",
        "or r10, rbx",
        "vpcmpeqb ymm0, ymm13, ymmword ptr [rsi + rax - 63]",
        "vpcmpeqb ymm8, ymm13, ymmword ptr [rsi + rax - 31]",
        "vpmovmskb r8d, ymm0",
        "vpmovmskb ebx, ymm8",
        "shl rbx, 32",
        "or r8, rbx",
        "lea r9, [rax - 1]",
        "sub r9, r12",
        "mov ebx, 63",
        "sub rbx, r9",
        "shrx r10, r10, rbx",
        "shrx r8, r8, rbx",
        "lea rax, [rsi + rax + 1]",
        "mov qword ptr [r13 + {scr}], rax",
        // c4 rbx, c5 rbp, c6 rax; exactly three commas and the newline within 64 bytes
        "tzcnt rbx, r10",
        "blsr rax, r10",
        "tzcnt rbp, rax",
        "blsr rax, rax",
        "blsr rcx, rax",
        "or r11, rcx",
        "tzcnt rax, rax",
        "mov rcx, r9",
        "or rcx, rax",
        "shr rcx, 6",
        "or r11, rcx",
        // which of fields 3..6 are quoted: r9; a quote out of place: r11
        "bts r10, r9",
        "lea rcx, [r10 + r10 + 1]",
        "pext r9, r8, rcx",
        "andn rcx, rcx, r8",
        "shr r10, 1",
        "pdep r10, r9, r10",
        "xor rcx, r10",
        "or r11, rcx",
        // ingest_ts: 20 characters; where it starts in r8
        "mov r8d, r9d",
        "and r8d, 1",
        "lea r12, [rsi + r12 + 1]",
        "lea rcx, [r8 + r8]",
        "mov r10, rbx",
        "sub r10, rcx",
        "xor r10, 20",
        "or r11, r10",
        "add r8, r12",
        // type: the table entry for its first letter
        "mov ecx, r9d",
        "shr ecx, 1",
        "and ecx, 1",
        "lea r10, [r12 + rbx + 1]",
        "mov r10, qword ptr [r10 + rcx]",
        "mov r12, r10",
        "and r12d, 31",
        "shl r12, 5",
        "xor r10, qword ptr [rdi + r12 + {ty}]",
        "and r10, qword ptr [rdi + r12 + {ty} + 8]",
        "or r11, r10",
        "mov r10, rbp",
        "sub r10, rbx",
        "lea rcx, [rcx + rcx + 1]",
        "sub r10, rcx",
        "xor r10, qword ptr [rdi + r12 + {ty} + 16]",
        "or r11, r10",
        "mov r10, qword ptr [rdi + r12 + {ty} + 24]",
        "mov byte ptr [r13 + r15 + {pur}], r10b",
        // product: not empty
        "shr r9d, 2",
        "and r9d, 1",
        "lea rbp, [rbp + r9*2 + 1]",
        "xor ecx, ecx",
        "cmp rax, rbp",
        "sete cl",
        "or r11, rcx",
        // ingest date; the day's start in rbx
        "mov rbp, qword ptr [r8 + 2]",
        "pext r10, rbp, qword ptr [rdi + {daybits}]",
        "shl r10, 6",
        "xor rbp, qword ptr [rdi + r10 + {d_w1}]",
        "or r11, rbp",
        "mov rbp, qword ptr [r8]",
        "xor rbp, qword ptr [rdi + r10 + {d_w0}]",
        "or r11, rbp",
        "mov rbx, qword ptr [rdi + r10 + {d_isec}]",
        // ---- both timestamps
        "mov rcx, qword ptr [r13 + {scr} + 32]",
        "vinserti128 ymm1, ymm1, xmmword ptr [r8 + 10], 1",
        "vpsubb ymm1, ymm1, ymmword ptr [rcx + {f_lo}]",
        "vpand ymm1, ymm1, ymmword ptr [rcx + {f_mk}]",
        "vpsubusb ymm0, ymm1, ymmword ptr [rcx + {f_rg}]",
        "vpor ymm2, ymm2, ymm0",
        "vpshufb ymm1, ymm1, ymmword ptr [rcx + {f_shuf}]",
        "vpmaddubsw ymm1, ymm1, ymm3",
        "vpcmpgtw ymm0, ymm1, ymmword ptr [rcx + {f_wlim}]",
        "vpor ymm2, ymm2, ymm0",
        "vpmaddwd ymm1, ymm1, ymmword ptr [rcx + {f_w2}]",
        "vpackusdw ymm1, ymm1, ymm1",
        "vpmaddwd ymm1, ymm1, ymmword ptr [rcx + {f_w3}]",
        "vmovq rax, xmm1",
        "vextracti128 xmm0, ymm1, 1",
        "vmovd r8d, xmm0",
        "add rbx, r8",
        // ts = constant part + k0 * first number + kb * second number
        "mov r10d, eax",
        "shr rax, 32",
        "imul r10, qword ptr [rcx + {f_k0}]",
        "imul rax, qword ptr [r13 + {scr} + 24]",
        "add rax, r10",
        "add rax, qword ptr [r13 + {scr} + 16]",
        // ---- a row, or a line for the plain reading
        "vptest ymm2, ymm2",
        "jnz 60f",
        "test r11, r11",
        "jnz 60f",
        "mov qword ptr [r13 + r15*8 + {t}], rax",
        "mov qword ptr [r13 + r15*8 + {ing}], rbx",
        "mov qword ptr [r13 + r15*8 + {lp}], rsi",
        "mov rsi, qword ptr [r13 + {scr}]",
        "mov qword ptr [r13 + r15*8 + {nx}], rsi",
        "inc r15",
        "add r14, 16",
        "cmp r14, {b} * 16",
        "jb 2b",
        "jmp 90f",
        "60:",
        "mov rax, qword ptr [r13 + {scr}]",
        "jmp 71f",
        "70:",
        "lea rax, [rsi + rax + 1]",
        "71:",
        "mov rcx, qword ptr [r13 + {nc}]",
        "mov qword ptr [r13 + rcx*8 + {coldp}], rsi",
        "inc rcx",
        "mov qword ptr [r13 + {nc}], rcx",
        "mov rsi, rax",
        "cmp rcx, {b}",
        "jb 2b",
        "90:",
        "xor eax, eax",
        "jmp 99f",
        "80:",
        "mov eax, 1",
        "99:",
        "pop rbp",
        "pop rbx",
        b = const B,
        ahead = const AHEAD,
        idr = const offset_of!(Dec, idr),
        tsrow = const offset_of!(Dec, tsrow),
        forms2 = const offset_of!(Dec, forms2),
        d_w0 = const offset_of!(Dec, days) + offset_of!(Day, w0),
        d_w1 = const offset_of!(Dec, days) + offset_of!(Day, w1),
        d_tms = const offset_of!(Dec, days) + offset_of!(Day, tms),
        d_isec = const offset_of!(Dec, days) + offset_of!(Day, isec),
        d_text = const offset_of!(Dec, days) + offset_of!(Day, text),
        ty = const offset_of!(Dec, ty),
        daybits = const offset_of!(Dec, daybits),
        f_lo = const offset_of!(Form, lo),
        f_mk = const offset_of!(Form, mk),
        f_rg = const offset_of!(Form, rg),
        f_shuf = const offset_of!(Form, shuf),
        f_wlim = const offset_of!(Form, wlim),
        f_w2 = const offset_of!(Form, w2),
        f_w3 = const offset_of!(Form, w3),
        f_off = const offset_of!(Form, off),
        f_k0 = const offset_of!(Form, k0),
        f_kb = const offset_of!(Form, kb),
        f_dmask = const offset_of!(Form, dmask),
        f_c0 = const offset_of!(Form, c0),
        f_bad = const offset_of!(Form, bad),
        f_q = const offset_of!(Form, q),
        f_dm16 = const offset_of!(Form, dm16),
        ids = const offset_of!(Batch, ids),
        t = const offset_of!(Batch, t),
        ing = const offset_of!(Batch, ing),
        lp = const offset_of!(Batch, lp),
        nx = const offset_of!(Batch, nx),
        coldp = const offset_of!(Batch, coldp),
        pur = const offset_of!(Batch, pur),
        nc = const offset_of!(Batch, nc),
        scr = const offset_of!(Batch, scr),
        in("rdi") dec as *const Dec,
        inout("rsi") p => stop,
        in("rdx") pend,
        in("r13") bt as *mut Batch,
        out("r15") rows,
        out("rax") long,
        out("rcx") _, out("r8") _, out("r9") _, out("r10") _, out("r11") _, out("r12") _, out("r14") _,
        out("ymm0") _, out("ymm1") _, out("ymm2") _, out("ymm8") _,
        in("ymm3") _mm256_set1_epi16(0x010A),
        in("ymm4") ld(dec.idw3.as_ptr() as *const u8),
        in("ymm5") ld(dec.idw2.as_ptr() as *const u8),
        in("ymm6") ld(dec.idw1.as_ptr()),
        in("ymm7") ld(dec.idrng.as_ptr()),
        in("ymm9") _mm256_set1_epi8(10),
        in("ymm10") _mm256_set1_epi8(39),
        in("ymm11") ld(dec.idt.as_ptr()),
        in("ymm12") _mm256_set1_epi8(b'0' as i8),
        in("ymm13") _mm256_set1_epi8(b'"' as i8),
        in("ymm14") _mm256_set1_epi8(b'\n' as i8),
        in("ymm15") _mm256_set1_epi8(b',' as i8),
    );
    (stop, rows, long != 0)
}

/// `events` with the line loop of `lines_asm`: the same rows into the same batch
/// stage, the same lines to the plain reading.
#[inline(never)]
pub unsafe fn events_a(sh: &Shared, w: &mut Worker, d: *const u8, start: usize, end: usize) {
    let fx = sh.fx.as_ref().unwrap_unchecked();
    let dec: &Dec = sh.dec.as_ref().unwrap_unchecked();
    let mut bt: Batch = std::mem::zeroed();
    let (mut p, pend) = (d.add(start), d.add(end));
    while p < pend {
        bt.nc = 0;
        let (stop, rows, long) = lines_asm(dec, p, pend, &mut bt);
        // Which rows are purchases, as a bit each.
        let z = _mm256_setzero_si256();
        let marked = |i: usize| !(_mm256_movemask_epi8(_mm256_cmpeq_epi8(ld(bt.pur.as_ptr().add(32 * i)), z)) as u32) as u64;
        let purchases = (marked(0) | (marked(1) << 32)) & (u64::MAX >> ((B - rows) & 63)) & ((rows != 0) as u64).wrapping_neg();
        // The purchases' price and category. A price the rules do not accept puts
        // the row outside every window: the copy is invalid.
        let mut m = purchases;
        while m != 0 {
            let j = m.trailing_zeros() as usize;
            m &= m - 1;
            match purchase(sh, dec, d, (bt.nx[j] - d as u64) as usize - 1) {
                Some(x) => bt.pc[j] = x as u64,
                None => bt.t[j] = i64::MIN,
            }
        }
        flush(sh, fx, dec, w, d, &mut bt, rows);
        let mut m = purchases;
        while m != 0 {
            bt.pc[m.trailing_zeros() as usize] = 0;
            m &= m - 1;
        }
        for k in 0..bt.nc as usize {
            cold_from(sh, w, d, (bt.coldp[k] - d as u64) as usize);
        }
        p = stop;
        if long {
            p = d.add(cold_from(sh, w, d, p as usize - d as usize));
        }
    }
}
