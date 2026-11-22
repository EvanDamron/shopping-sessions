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
//! Rows gather in a batch (struct of arrays); the batch drops the clicks with no
//! campaign or outside the window, turns user ID values into user numbers and files
//! the clicks exactly as `clicks_x` does.
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
    // For the hand-arranged loop: `ctab`'s data, the length masks (16 bytes from
    // `keep[16 - n]` keep the first n bytes), and whether it is used (SS_ASM=0, or
    // no campaign table: the compiled loop).
    ctabp: *const u32,
    keep: [u8; 32],
    pub asm: bool,
}
unsafe impl Send for DecC {}
unsafe impl Sync for DecC {}

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
            ctabp: std::ptr::null(),
            keep: KEEP,
            asm: false,
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
        dc.ctabp = dc.ctab.as_ptr();
        dc.asm = dc.notab == 0 && std::env::var_os("SS_ASM").is_none_or(|v| v != "0");
        dc
    }
}

#[repr(C, align(64))]
struct Batch {
    ids: [[u64; 4]; B], // [3] user ID value
    camp: [u32; B],     // campaign index, NOTFOUND for none
    cid: [[u64; 2]; B], // the click ID as an Id's two words
    t: [i64; B],        // ts, ms since the click window's start
    lp: [u64; B],       // where the row's line starts (address)
    // For `lines_asm`: the lines queued for the plain reading, and where the next
    // line starts.
    coldp: [u64; B],
    nc: u64,
    scr: [u64; 2],
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
unsafe fn row(fx: &Fx, span: u64, bt: &Batch, j: usize) -> (u8, u64, u32) {
    let (_, uk, _, gmax) = fx.keys();
    let cp = bt.camp[j];
    let nu = bt.ids[j][3];
    let u = unscramble(nu, uk);
    let out = ((nu >> 40) != 0) | ((u >> 9) >= gmax);
    let keep = (cp != NOTFOUND) & ((bt.t[j] as u64) < span);
    (keep as u8 * (1 + out as u8), u, cp)
}

/// Finishes a batch: the campaign and window checks, user numbers from user ID
/// values, and the clicks filed under their user numbers.
#[inline(never)]
unsafe fn flush(sh: &Shared, fx: &Fx, w: &mut Worker, d: *const u8, bt: &Batch, n: usize) {
    let span = sh.win.span as u64;
    if sh.deccheck {
        check(sh, fx, w, d, bt, n);
    }
    for j in 0..n {
        let (st, u, camp) = row(fx, span, bt, j);
        if st == 1 {
            let g = u >> 9;
            let r = CRec { who: g as u32, ts: bt.t[j] as u32, camp, _pad: (u & 511) as u32, cid: Id { lo: bt.cid[j][0], hi: bt.cid[j][1] } };
            w.push_xclick(fx, (g >> fx.cshift) as usize, r);
        } else if st == 2 {
            cold_from(sh, w, d, (bt.lp[j] - d as u64) as usize);
        }
    }
}

/// Test hook: every row of the batch against the plain reading of its line.
#[cold]
#[inline(never)]
unsafe fn check(sh: &Shared, fx: &Fx, w: &mut Worker, d: *const u8, bt: &Batch, n: usize) {
    for j in 0..n {
        let (st, u, camp) = row(fx, sh.win.span as u64, bt, j);
        if st == 2 {
            continue;
        }
        let ls = (bt.lp[j] - d as u64) as usize;
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
        _mm_prefetch::<_MM_HINT_T0>(p.add(crate::dec::AHEAD) as *const i8); // (see dec::events)
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
        let cn = _mm_extract_epi64::<1>(_mm256_castsi256_si128(idv)) as u64;
        *bt.camp.get_unchecked_mut(nb) = *dc.ctab.get_unchecked(cn.min(dc.clen) as usize);
        if cslow != 0 {
            let cs = pos + (c2 + 1 + q2) as usize;
            *bt.camp.get_unchecked_mut(nb) = campaign_slow(sh, d, cs, pos + (c3 - q2) as usize);
        }
        // (Id::short: the bytes masked to the length, the length in byte 15)
        let cid = _mm_and_si128(ld16(p.add(q0 as usize)), ld16(KEEP.as_ptr().add(16 - l0 as usize)));
        _mm_storeu_si128(bt.cid.as_mut_ptr().add(nb) as *mut __m128i, _mm_insert_epi8(cid, l0 as i32, 15));
        *bt.t.get_unchecked_mut(nb) = tms;
        *bt.lp.get_unchecked_mut(nb) = p as u64;
        nb += 1;
        if nb == B {
            flush(sh, fx, w, d, &bt, nb);
            nb = 0;
        }
        pos = next;
    }
    flush(sh, fx, w, d, &bt, nb);
}

/// Up to B click lines from `p` decoded into the batch with the registers assigned
/// by hand (see `dec::lines_asm` for why): returns where it stopped, the rows it
/// filled, and whether it stopped at a line with no newline in its first 64 bytes.
/// It does what `clicks`' loop does, with one difference: a campaign field that is
/// not `c_` + a number goes with the line to the plain reading (queued in
/// `bt.coldp`) instead of to the lookup map from inside the loop. Nothing is
/// called from here.
///
/// Registers: rdi the event decoder's tables, r14 the click tables, rsi line, rdx
/// end, r13 batch, r15 rows; rax rbx rcx rbp r8-r12 scratch; ymm3 and ymm7-ymm15
/// constants, ymm0-ymm2 and ymm4 scratch.
#[inline(never)]
unsafe fn lines_asm(dec: &Dec, dc: &DecC, p: *const u8, pend: *const u8, bt: &mut Batch) -> (*const u8, usize, bool) {
    use crate::dec::Day;
    use core::mem::{offset_of, size_of};
    let (stop, rows, long): (*const u8, usize, u64);
    core::arch::asm!(
        "push rbx",
        "push rbp",
        "xor r15d, r15d",
        // ---- next line
        "2:",
        "cmp rsi, rdx",
        "jae 90f",
        "prefetcht0 byte ptr [rsi + {ahead}]",
        // newline: nl in rax
        "vpcmpeqb ymm0, ymm14, ymmword ptr [rsi]",
        "vpcmpeqb ymm1, ymm14, ymmword ptr [rsi + 32]",
        "vpmovmskb eax, ymm0",
        "vpmovmskb ecx, ymm1",
        "shl rcx, 32",
        "or rax, rcx",
        "tzcnt rax, rax",
        "jc 80f",
        // commas (r8) and quotes (r9) of the line
        "vpcmpeqb ymm0, ymm15, ymmword ptr [rsi]",
        "vpcmpeqb ymm1, ymm15, ymmword ptr [rsi + 32]",
        "vpmovmskb r8d, ymm0",
        "vpmovmskb ecx, ymm1",
        "shl rcx, 32",
        "or r8, rcx",
        "bzhi r8, r8, rax",
        "vpcmpeqb ymm0, ymm13, ymmword ptr [rsi]",
        "vpcmpeqb ymm1, ymm13, ymmword ptr [rsi + 32]",
        "vpmovmskb r9d, ymm0",
        "vpmovmskb ecx, ymm1",
        "shl rcx, 32",
        "or r9, rcx",
        "bzhi r9, r9, rax",
        // c1 rcx, c2 rbx, c3 r12; exactly three commas
        "blsr rbx, r8",
        "tzcnt rcx, r8",
        "blsr rbp, rbx",
        "tzcnt rbx, rbx",
        "tzcnt r12, rbp",
        "blsr rbp, rbp",
        "cmp r12, 64",
        "jae 70f",
        "test rbp, rbp",
        "jnz 70f",
        // quotes: which fields are quoted in r11, anything out of place in r10
        "bts r8, rax",
        "lea r10, [r8 + r8 + 1]",
        "pext r11, r9, r10",
        "andn r10, r10, r9",
        "shr r8, 1",
        "pdep r8, r11, r8",
        "xor r10, r8",
        "lea r8, [rsi + rax + 1]",
        "mov qword ptr [r13 + {scr}], r8",
        // ---- ts: its form (rax), where its text starts (r8), its lane, its date,
        // the time in rbp
        "mov r8d, r11d",
        "shr r8d, 3",
        "sub rax, r12",
        "lea rax, [r8 + rax*2]",
        "movzx eax, byte ptr [rdi + rax + {tsform} - 2]",
        "imul eax, eax, {fsize}",
        "lea rax, [rdi + rax + {forms}]",
        "or r10, qword ptr [rax + {f_bad}]",
        "lea r8, [r12 + r8 + 1]",
        "add r8, rsi",
        "mov rbp, qword ptr [rax + {f_off}]",
        "vmovdqu xmm1, xmmword ptr [r8 + rbp]",
        "vpsubb xmm1, xmm1, xmmword ptr [rax + {f_lo}]",
        "vpand xmm1, xmm1, xmmword ptr [rax + {f_mk}]",
        "vpsubusb xmm2, xmm1, xmmword ptr [rax + {f_rg}]",
        "vpshufb xmm1, xmm1, xmmword ptr [rax + {f_shuf}]",
        "vpmaddubsw xmm1, xmm1, xmm3",
        "vpcmpgtw xmm0, xmm1, xmmword ptr [rax + {f_wlim}]",
        "vpor xmm2, xmm2, xmm0",
        "vpmaddwd xmm1, xmm1, xmmword ptr [rax + {f_w2}]",
        "vpackusdw xmm1, xmm1, xmm1",
        "vpmaddwd xmm1, xmm1, xmmword ptr [rax + {f_w3}]",
        "mov rbp, qword ptr [r8 + 2]",
        "pext r9, rbp, qword ptr [rdi + {daybits}]",
        "shl r9, 6",
        "xor rbp, qword ptr [rdi + r9 + {d_w1}]",
        "and rbp, qword ptr [rax + {f_dmask}]",
        "or r10, rbp",
        "mov rbp, qword ptr [r8]",
        "xor rbp, qword ptr [rdi + r9 + {d_w0}]",
        "and rbp, qword ptr [rax + {f_dmask}]",
        "or r10, rbp",
        "mov rbp, qword ptr [rdi + r9 + {d_tms}]",
        "and rbp, qword ptr [rax + {f_dmask}]",
        "add rbp, qword ptr [rax + {f_c0}]",
        "movzx r9d, byte ptr [r8 + 19]",
        "and r9d, 2",
        "mov r9, qword ptr [rax + r9*4 + {f_kb}]",
        "vmovd r8d, xmm1",
        "imul r8, qword ptr [rax + {f_k0}]",
        "add rbp, r8",
        "vpextrd r8d, xmm1, 1",
        "imul r8, r9",
        "add rbp, r8",
        // ---- campaign (lane 0) and user ID (lane 1): shuffles by raw length and
        // quoting, the lane arithmetic, the checks
        "mov rax, r12",
        "sub rax, rbx",
        "mov r8d, r11d",
        "shr r8d, 2",
        "and r8d, 1",
        "lea rax, [r8 + rax*2]",
        "shl rax, 4",
        "vmovdqu xmm0, xmmword ptr [r14 + rax + {csh} - 32]",
        "mov rax, rbx",
        "sub rax, rcx",
        "mov r8d, r11d",
        "shr r8d, 1",
        "and r8d, 1",
        "lea rax, [r8 + rax*2]",
        "shl rax, 4",
        "vinserti128 ymm0, ymm0, xmmword ptr [rdi + rax + {idsh} - 32], 1",
        "vmovdqu xmm1, xmmword ptr [rsi + r12 - 16]",
        "vinserti128 ymm1, ymm1, xmmword ptr [rsi + rbx - 16], 1",
        "vpsubb ymm1, ymm1, ymm12",
        "vpshufb ymm1, ymm1, ymm0",
        "vpcmpgtb ymm0, ymm1, ymm11",
        "vpand ymm4, ymm0, ymm10",
        "vpsubb ymm1, ymm1, ymm4",
        "vpand ymm0, ymm0, ymm9",
        "vpsubb ymm4, ymm1, ymm8",
        "vpsubb ymm4, ymm4, ymm0",
        "vpsubusb ymm4, ymm4, ymm7",
        "vpor ymm4, ymm4, ymm2",
        "vpmaddubsw ymm1, ymm1, ymmword ptr [r14 + {idw1}]",
        "vpmaddwd ymm1, ymm1, ymmword ptr [r14 + {idw2}]",
        "vpmuludq ymm0, ymm1, ymmword ptr [r14 + {idw3}]",
        "vpsrlq ymm1, ymm1, 32",
        "vpaddq ymm1, ymm1, ymm0",
        // the campaign's index by its number (eax)
        "vpextrq rax, xmm1, 1",
        "cmp rax, qword ptr [r14 + {clen}]",
        "cmovae rax, qword ptr [r14 + {clen}]",
        "mov r8, qword ptr [r14 + {ctabp}]",
        "mov eax, dword ptr [r8 + rax*4]",
        // ---- click ID: 1 to 15 bytes, masked to its length, the length in byte 15
        "mov r8d, r11d",
        "and r8d, 1",
        "lea r9, [r8 + r8]",
        "sub rcx, r9",
        "lea r9, [rcx - 1]",
        "cmp r9, 14",
        "ja 60f",
        "vmovdqu xmm0, xmmword ptr [rsi + r8]",
        "mov r9d, 16",
        "sub r9, rcx",
        "vpand xmm0, xmm0, xmmword ptr [r14 + r9 + {keep}]",
        "vpinsrb xmm0, xmm0, ecx, 15",
        // ---- a row, or a line for the plain reading
        "vptest ymm4, ymm4",
        "jnz 60f",
        "test r10, r10",
        "jnz 60f",
        "mov r8, r15",
        "shl r8, 4",
        "vmovdqu xmmword ptr [r13 + r8 + {cid}], xmm0",
        "vmovdqu ymmword ptr [r13 + r8*2 + {ids}], ymm1",
        "mov dword ptr [r13 + r15*4 + {camp}], eax",
        "mov qword ptr [r13 + r15*8 + {t}], rbp",
        "mov qword ptr [r13 + r15*8 + {lp}], rsi",
        "mov rsi, qword ptr [r13 + {scr}]",
        "inc r15",
        "cmp r15, {b}",
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
        ahead = const crate::dec::AHEAD,
        tsform = const offset_of!(Dec, tsform),
        forms = const offset_of!(Dec, forms),
        idsh = const offset_of!(Dec, idsh),
        daybits = const offset_of!(Dec, daybits),
        d_w0 = const offset_of!(Dec, days) + offset_of!(Day, w0),
        d_w1 = const offset_of!(Dec, days) + offset_of!(Day, w1),
        d_tms = const offset_of!(Dec, days) + offset_of!(Day, tms),
        fsize = const size_of::<Form>(),
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
        csh = const offset_of!(DecC, csh),
        idw1 = const offset_of!(DecC, idw1),
        idw2 = const offset_of!(DecC, idw2),
        idw3 = const offset_of!(DecC, idw3),
        clen = const offset_of!(DecC, clen),
        ctabp = const offset_of!(DecC, ctabp),
        keep = const offset_of!(DecC, keep),
        ids = const offset_of!(Batch, ids),
        camp = const offset_of!(Batch, camp),
        cid = const offset_of!(Batch, cid),
        t = const offset_of!(Batch, t),
        lp = const offset_of!(Batch, lp),
        coldp = const offset_of!(Batch, coldp),
        nc = const offset_of!(Batch, nc),
        scr = const offset_of!(Batch, scr),
        in("rdi") dec as *const Dec,
        in("r14") dc as *const DecC,
        inout("rsi") p => stop,
        in("rdx") pend,
        in("r13") bt as *mut Batch,
        out("r15") rows,
        out("rax") long,
        out("rcx") _, out("r8") _, out("r9") _, out("r10") _, out("r11") _, out("r12") _,
        out("ymm0") _, out("ymm1") _, out("ymm2") _, out("ymm4") _,
        in("ymm3") _mm256_set1_epi16(0x010A),
        in("ymm7") ld(dc.idrng.as_ptr()),
        in("ymm8") ld(dc.idlow.as_ptr()),
        in("ymm9") _mm256_set1_epi8(10),
        in("ymm10") _mm256_set1_epi8(39),
        in("ymm11") ld(dc.idt.as_ptr()),
        in("ymm12") _mm256_set1_epi8(b'0' as i8),
        in("ymm13") _mm256_set1_epi8(b'"' as i8),
        in("ymm14") _mm256_set1_epi8(b'\n' as i8),
        in("ymm15") _mm256_set1_epi8(b',' as i8),
    );
    (stop, rows, long != 0)
}

/// `clicks` with the line loop of `lines_asm`: the same rows into the same batch
/// stage, the same lines to the plain reading.
#[inline(never)]
pub unsafe fn clicks_a(sh: &Shared, w: &mut Worker, d: *const u8, start: usize, end: usize) {
    let fx = sh.fx.as_ref().unwrap_unchecked();
    let dec: &Dec = sh.dec.as_ref().unwrap_unchecked();
    let dc: &DecC = sh.decc.as_ref().unwrap_unchecked();
    let mut bt: Batch = std::mem::zeroed();
    let (mut p, pend) = (d.add(start), d.add(end));
    while p < pend {
        bt.nc = 0;
        let (stop, rows, long) = lines_asm(dec, dc, p, pend, &mut bt);
        flush(sh, fx, w, d, &bt, rows);
        for k in 0..bt.nc as usize {
            cold_from(sh, w, d, (bt.coldp[k] - d as u64) as usize);
        }
        p = stop;
        if long {
            p = d.add(cold_from(sh, w, d, p as usize - d as usize));
        }
    }
}
