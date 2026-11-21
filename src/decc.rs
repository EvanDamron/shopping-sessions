//! Per-line decoder for click lines: no separator index pass.
//!
//! The method is the event decoder's (dec.rs), on a shorter line. Two 32-byte loads
//! from the line start give bit masks of its commas, quotes and newline; a click line
//! is about 45 bytes, so the three commas and the newline are in that one 64-bit
//! window (a longer line goes to `cold`). The quote mask is checked against the one
//! mask it has to be if every field is either bare or wrapped in exactly one pair of
//! quotes. Then:
//!
//!   campaign, user ID   one 256-bit vector: lane 0 the campaign as `c_` + a decimal
//!                       number without a leading zero, lane 1 the user ID as `u` +
//!                       base-36 digits (the event decoder's lane); the campaign
//!                       number indexes a table built from campaigns.csv, any other
//!                       campaign text goes through the hash map
//!   ts                  one 128-bit lane with the event decoder's per-form
//!                       constants and its date table
//!   click ID            the 16 bytes at the field, masked to its length
//!
//! Rows gather in a batch (struct of arrays); the batch looks up the campaigns,
//! applies the window, turns user ID values into user numbers and files the clicks
//! exactly as `clicks_x` does.
//!
//! An acceptor of the common shapes only: a line longer than 64 bytes, a field count
//! other than four, a quote out of place, a click ID that is empty or longer than 15
//! bytes, a user ID that is not `u` + base 36 or whose number is out of range, a
//! timestamp of another length or with any character out of place, or a date outside
//! the table, and the whole line goes to `cold`, which runs the general per-line code
//! (`click_x_line`) on it. So the result is that code's result on every line.

use crate::dec::{Dec, Form, DAYBITS};
use crate::fast::{unscramble, Fx};
use crate::id::Id;
use crate::{click_x_line, click_x_plain, COut, CRec, Lookups, Shared, Worker};
use core::arch::x86_64::*;

/// 16 bytes from `KEEP[16 - n]` keep the first n bytes of a 16-byte load.
static KEEP: [u8; 32] = {
    let mut k = [0u8; 32];
    let mut i = 0;
    while i < 16 {
        k[i] = 0xFF;
        i += 1;
    }
    k
};

/// Rows per batch.
const B: usize = 64;

/// Campaign table entry of a number that is no campaign's.
const NOTFOUND: u32 = u32::MAX;

/// Campaign numbers at or above this: no table, every campaign through the hash map.
const CAP: u64 = 1 << 16;

#[repr(C, align(64))]
pub struct DecC {
    /// Shuffle for a campaign field by 2 * raw length + quoted, applied to the 16
    /// bytes that end where the field ends: digits right-aligned in bytes 8..16, `_`
    /// in byte 7, `c` in byte 6, the first digit again in byte 5.
    csh: [[u8; 16]; 128],
    // Per byte of the (campaign, user ID) vector, as `Dec`'s ID tables.
    idt: [u8; 32],
    idlow: [u8; 32],
    idrng: [u8; 32],
    idw1: [u8; 32],
    idw2: [i16; 16],
    idw3: [u32; 8],
    /// Campaign index by campaign number, NOTFOUND for no campaign; `ctab[clen]` is
    /// NOTFOUND, and so is every number at or above `clen`.
    ctab: Vec<u32>,
    clen: u64,
    /// Campaigns all go through the hash map (all ones) or the table where they can (0).
    notab: u64,
}

/// The number of `c_` + 1 to 8 decimal digits without a leading zero.
fn campaign_number(id: &[u8]) -> Option<u64> {
    let digits = id.strip_prefix(b"c_")?;
    if digits.is_empty() || digits.len() > 8 || digits[0] == b'0' || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(digits.iter().fold(0u64, |n, &c| n * 10 + (c - b'0') as u64))
}

impl DecC {
    pub fn new(lk: &Lookups) -> Box<DecC> {
        let mut dc: Box<DecC> = Box::new(DecC {
            csh: [[0x80; 16]; 128],
            idt: [0; 32],
            idlow: [0; 32],
            idrng: [0; 32],
            idw1: [0; 32],
            idw2: [0; 16],
            idw3: [0; 8],
            ctab: vec![NOTFOUND],
            clen: 0,
            notab: u64::MAX,
        });
        for i in 0..128 {
            let (raw, q) = (i / 2, i % 2);
            if raw < 2 * q + 3 || raw > 2 * q + 10 {
                continue; // not `c_` and 1 to 8 digits
            }
            let nd = raw - 2 * q - 2;
            let sh = &mut dc.csh[i];
            for j in 0..nd {
                sh[15 - j] = (15 - q - j) as u8;
            }
            sh[7] = (15 - q - nd) as u8;
            sh[6] = (14 - q - nd) as u8;
            sh[5] = (16 - q - nd) as u8;
        }
        // Lane 0: the campaign. No letters (nothing is above 127); digit places 0..9,
        // the first-digit copy 1..9, `c` and `_` exact, the rest zero.
        for i in 0..16 {
            dc.idt[i] = 127;
        }
        for i in 8..16 {
            dc.idrng[i] = 9;
            dc.idw1[i] = if i % 2 == 0 { 10 } else { 1 };
        }
        (dc.idlow[5], dc.idrng[5]) = (1, 8);
        dc.idlow[6] = b'c' - b'0';
        dc.idlow[7] = b'_' - b'0';
        (dc.idw2[4], dc.idw2[5], dc.idw2[6], dc.idw2[7]) = (100, 1, 100, 1);
        dc.idw3[2] = 10_000;
        // Lane 1: the user ID, as the event decoder's user lane.
        let o = 16;
        for i in 0..8 {
            dc.idt[o + i] = 127;
            dc.idt[o + 8 + i] = 9;
            dc.idrng[o + i] = 255;
            dc.idrng[o + 8 + i] = 25;
            dc.idw1[o + 8 + i] = if i % 2 == 0 { 36 } else { 1 };
        }
        dc.idlow[o + 6] = 1;
        dc.idrng[o + 6] = 254;
        dc.idlow[o + 7] = b'u' - b'0';
        dc.idrng[o + 7] = 0;
        (dc.idw2[12], dc.idw2[13], dc.idw2[14], dc.idw2[15]) = (1296, 1, 1296, 1);
        dc.idw3[6] = 1_679_616;

        // The table: every campaign whose ID has the `c_` + number form, by number. A
        // field of that form names exactly the campaign `c_<its number>`, so its entry
        // is that campaign or none at all.
        let nums: Vec<(u64, usize)> = lk.campaign_ids.iter().enumerate().filter_map(|(i, id)| campaign_number(id).map(|n| (n, i))).collect();
        let max = nums.iter().map(|x| x.0).max().unwrap_or(0);
        if max < CAP {
            let mut ctab = vec![NOTFOUND; max as usize + 2];
            let mut ok = true;
            for &(n, i) in &nums {
                ok &= format!("c_{n}").as_bytes() == &lk.campaign_ids[i][..]
                    && lk.campaigns.get(Id::intern(&lk.campaign_ids[i])) == Some(i as u32)
                    && ctab[n as usize] == NOTFOUND;
                ctab[n as usize] = i as u32;
            }
            if ok {
                dc.clen = max + 1;
                dc.ctab = ctab;
                dc.notab = 0;
            }
        }
        dc
    }
}

#[repr(C, align(64))]
struct Batch {
    ids: [[u64; 4]; B], // [1] campaign number (or 1 << 63 | campaign index), [3] user ID value
    cid: [[u64; 2]; B], // the click ID as an Id's two words
    t: [i64; B], // ts, ms since the click window's start
    line: [u32; B],
}

#[inline(always)]
unsafe fn ld(p: *const u8) -> __m256i {
    _mm256_loadu_si256(p as *const __m256i)
}

#[inline(always)]
unsafe fn ld16(p: *const u8) -> __m128i {
    _mm_loadu_si128(p as *const __m128i)
}

#[inline(always)]
unsafe fn ld2(a: *const u8, b: *const u8) -> __m256i {
    _mm256_inserti128_si256(_mm256_castsi128_si256(ld16(a)), ld16(b), 1)
}

#[inline(always)]
unsafe fn load8(p: *const u8) -> u64 {
    (p as *const u64).read_unaligned()
}

/// The general per-line code on the line `[ls, le)` (`le` is its newline).
#[cold]
#[inline(never)]
unsafe fn cold(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) {
    w.nccold += 1;
    click_x_line(sh, w, d, ls, le);
}

/// The line starting at `ls`, wherever it ends.
#[cold]
#[inline(never)]
unsafe fn cold_from(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize) -> usize {
    let mut le = ls;
    while *d.add(le) != b'\n' {
        le += 1;
    }
    cold(sh, w, d, ls, le);
    le + 1
}

/// A campaign field of another form than `c_` + number: the hash map, as `clicks_x`.
#[inline(never)]
unsafe fn campaign_slow(sh: &Shared, d: *const u8, s: usize, e: usize) -> u32 {
    sh.lk.campaigns.get(Id::field(d, s, e)).unwrap_or(NOTFOUND)
}

/// What becomes of row `j`: (0 dropped, 1 filed here, 2 to the plain reading; user
/// number; campaign index).
#[inline(always)]
unsafe fn row(fx: &Fx, dc: &DecC, span: u64, bt: &Batch, j: usize) -> (u8, u64, u32) {
    let (_, uk, _, gmax) = fx.keys();
    let c = bt.ids[j][1];
    let cp = if c >> 63 != 0 { c as u32 } else { *dc.ctab.get_unchecked(c.min(dc.clen) as usize) };
    let nu = bt.ids[j][3];
    let u = unscramble(nu, uk);
    let out = ((nu >> 40) != 0) | ((u >> 9) >= gmax);
    let keep = (cp != NOTFOUND) & ((bt.t[j] as u64) < span);
    (keep as u8 * (1 + out as u8), u, cp)
}

/// Finishes a batch: campaigns, the window, user numbers from user ID values, and the
/// clicks filed under their user numbers.
#[inline(never)]
unsafe fn flush(sh: &Shared, fx: &Fx, dc: &DecC, w: &mut Worker, d: *const u8, bt: &Batch, n: usize) {
    let span = sh.win.span as u64;
    if sh.deccheck {
        check(sh, fx, dc, w, d, bt, n);
    }
    for j in 0..n {
        let (st, u, camp) = row(fx, dc, span, bt, j);
        if st == 1 {
            let g = u >> 9;
            let r = CRec { who: g as u32, ts: bt.t[j] as u32, camp, _pad: (u & 511) as u32, cid: Id { lo: bt.cid[j][0], hi: bt.cid[j][1] } };
            w.push_xclick(fx, (g >> fx.cshift) as usize, r);
        } else if st == 2 {
            cold_from(sh, w, d, bt.line[j] as usize);
        }
    }
}

/// Test hook: every row of the batch against the plain reading of its line.
#[cold]
#[inline(never)]
unsafe fn check(sh: &Shared, fx: &Fx, dc: &DecC, w: &mut Worker, d: *const u8, bt: &Batch, n: usize) {
    for j in 0..n {
        let (st, u, camp) = row(fx, dc, sh.win.span as u64, bt, j);
        if st == 2 {
            continue;
        }
        let ls = bt.line[j] as usize;
        let mut le = ls;
        while *d.add(le) != b'\n' {
            le += 1;
        }
        let same = match (st, click_x_plain(sh, w, d, ls, le)) {
            (0, COut::Drop) => true,
            (1, COut::X(c, r)) => {
                let g = u >> 9;
                c == (g >> fx.cshift) as usize && (r.who, r.ts, r.camp, r._pad, r.cid) == (g as u32, bt.t[j] as u32, camp, (u & 511) as u32, Id { lo: bt.cid[j][0], hi: bt.cid[j][1] })
            }
            _ => false,
        };
        if !same {
            eprintln!("decoder differs from the plain reading on: {:?}", String::from_utf8_lossy(std::slice::from_raw_parts(d.add(ls), le - ls)));
            std::process::exit(3);
        }
    }
}

/// Click lines of `d[start..end]` (whole lines; at least 64 readable bytes before
/// `d + start` and 128 after `d + end`).
#[inline(never)]
pub unsafe fn clicks(sh: &Shared, w: &mut Worker, d: *const u8, start: usize, end: usize) {
    let fx = sh.fx.as_ref().unwrap_unchecked();
    let dec: &Dec = sh.dec.as_ref().unwrap_unchecked();
    let dc: &DecC = sh.decc.as_ref().unwrap_unchecked();
    let mut bt: Batch = std::mem::zeroed();
    let mut nb = 0usize;
    let mut pos = start;
    let zero = _mm256_set1_epi8(b'0' as i8);
    let (comma, newline, quote) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
    let (idt, idlow, idrng) = (ld(dc.idt.as_ptr()), ld(dc.idlow.as_ptr()), ld(dc.idrng.as_ptr()));
    let (idw1, idw2, idw3) = (ld(dc.idw1.as_ptr()), ld(dc.idw2.as_ptr() as *const u8), ld(dc.idw3.as_ptr() as *const u8));
    let (c39, c10) = (_mm256_set1_epi8(39), _mm256_set1_epi8(10));
    let tens = _mm_set1_epi16(0x010A); // bytes 10, 1
    while pos < end {
        let p = d.add(pos);
        let (v0, v1) = (ld(p), ld(p.add(32)));
        let m = |x: __m256i| -> u64 {
            let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, x)) as u32 as u64;
            let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, x)) as u32 as u64;
            a | (b << 32)
        };
        let (cm, nm, qm) = (m(comma), m(newline), m(quote));
        if nm == 0 {
            pos = cold_from(sh, w, d, pos); // a line longer than 64 bytes
            continue;
        }
        let nl = _tzcnt_u64(nm);
        let next = pos + nl as usize + 1;

        // ---- Where the commas are: exactly three before the newline.
        let cl = _bzhi_u64(cm, nl as u32);
        let m1 = _blsr_u64(cl);
        let m2 = _blsr_u64(m1);
        let (c1, c2, c3) = (_tzcnt_u64(cl), _tzcnt_u64(m1), _tzcnt_u64(m2));
        if (c3 >> 6) | _blsr_u64(m2) != 0 {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }

        // ---- Quotes: `o` has a bit per field that starts with one. Every quote must
        // be one of those or the last character of such a field, and each of those
        // must be there. (With an empty first field the field ends are off by one;
        // such a line fails the click ID's length check below.)
        let qa = _bzhi_u64(qm, nl as u32);
        let s = cl | (1u64 << nl);
        let f = s.wrapping_mul(2) | 1;
        let o = _pext_u64(qa, f);
        let mut bad = (qa & !f) ^ _pdep_u64(o, s >> 1);
        let (q0, q1, q2, q3) = (o & 1, (o >> 1) & 1, (o >> 2) & 1, (o >> 3) & 1);

        // ---- Click ID: 1 to 15 bytes.
        let l0 = c1.wrapping_sub(2 * q0);
        bad |= (l0.wrapping_sub(1) > 14) as u64;

        // ---- Campaign and user ID.
        let shc = dc.csh.get_unchecked((2 * (c3 - c2 - 1) + q2) as usize).as_ptr();
        let shu = dec.idsh.get_unchecked((2 * (c2 - c1 - 1) + q1) as usize).as_ptr();
        let v = ld2(p.add(c3 as usize).sub(16), p.add(c2 as usize).sub(16));
        let x = _mm256_shuffle_epi8(_mm256_sub_epi8(v, zero), ld2(shc, shu));
        // Letters (above 9 in a user ID digit place) come down by 39 to 10..35; then
        // every byte must be in its range.
        let g = _mm256_cmpgt_epi8(x, idt);
        let val = _mm256_sub_epi8(x, _mm256_and_si256(g, c39));
        let chk = _mm256_sub_epi8(_mm256_sub_epi8(val, idlow), _mm256_and_si256(g, c10));
        let vbad = _mm256_subs_epu8(chk, idrng);
        let n2 = _mm256_madd_epi16(_mm256_maddubs_epi16(val, idw1), idw2);
        let idv = _mm256_add_epi64(_mm256_mul_epu32(n2, idw3), _mm256_srli_epi64(n2, 32));
        let vok = _mm256_movemask_epi8(_mm256_cmpeq_epi8(vbad, _mm256_setzero_si256())) as u32;
        bad |= (!vok >> 16) as u64; // the user ID
        let cslow = (!vok & 0xFFFF) as u64 | dc.notab; // the campaign

        // ---- Timestamp.
        let form: &Form = &*dec.forms.as_ptr().add(*dec.tsform.get_unchecked((2 * (nl - c3 - 1) + q3) as usize) as usize);
        let tsp = p.add((c3 + 1 + q3) as usize);
        bad |= form.bad;
        let y = _mm_and_si128(_mm_sub_epi8(ld16(tsp.offset(form.off)), ld16(form.lo.as_ptr())), ld16(form.mk.as_ptr()));
        let tbad = _mm_subs_epu8(y, ld16(form.rg.as_ptr()));
        let pr = _mm_maddubs_epi16(_mm_shuffle_epi8(y, ld16(form.shuf.as_ptr())), tens);
        let tbad = _mm_or_si128(tbad, _mm_cmpgt_epi16(pr, ld16(form.wlim.as_ptr() as *const u8)));
        let s2 = _mm_madd_epi16(pr, ld16(form.w2.as_ptr() as *const u8));
        let s3 = _mm_madd_epi16(_mm_packus_epi32(s2, s2), ld16(form.w3.as_ptr() as *const u8));
        let ab = _mm_cvtsi128_si64(s3) as u64;
        let w1 = load8(tsp.add(2));
        let dt = &*dec.days.as_ptr().add(_pext_u64(w1, DAYBITS) as usize);
        bad |= ((load8(tsp) ^ dt.w0) | (w1 ^ dt.w1)) & form.dmask;
        let kb = *form.kb.get_unchecked(((*tsp.add(19) >> 1) & 1) as usize);
        let tms = (dt.tms & form.dmask as i64)
            .wrapping_add(form.c0)
            .wrapping_add((ab as u32 as i64).wrapping_mul(form.k0))
            .wrapping_add(((ab >> 32) as i64).wrapping_mul(kb));

        if (bad != 0) | (_mm_testz_si128(tbad, tbad) == 0) {
            cold(sh, w, d, pos, next - 1);
            pos = next;
            continue;
        }
        _mm256_storeu_si256(bt.ids.as_mut_ptr().add(nb) as *mut __m256i, idv);
        if cslow != 0 {
            let cs = pos + (c2 + 1 + q2) as usize;
            bt.ids.get_unchecked_mut(nb)[1] = (1 << 63) | campaign_slow(sh, d, cs, pos + (c3 - q2) as usize) as u64;
        }
        // (Id::short: the bytes masked to the length, the length in byte 15)
        let cid = _mm_and_si128(ld16(p.add(q0 as usize)), ld16(KEEP.as_ptr().add(16 - l0 as usize)));
        _mm_storeu_si128(bt.cid.as_mut_ptr().add(nb) as *mut __m128i, _mm_insert_epi8(cid, l0 as i32, 15));
        *bt.t.get_unchecked_mut(nb) = tms;
        *bt.line.get_unchecked_mut(nb) = pos as u32;
        nb += 1;
        if nb == B {
            flush(sh, fx, dc, w, d, &bt, nb);
            nb = 0;
        }
        pos = next;
    }
    flush(sh, fx, dc, w, d, &bt, nb);
}
