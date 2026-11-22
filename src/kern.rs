//! Table-driven pieces of the line kernels (dec5.rs, pass 2).
//!
//! A kernel converts the fields of a line with straight-line code whose constants
//! come from a small table row chosen by the field's width (the timestamp form),
//! and every check it makes leaves a value that is zero for a line of the usual
//! shape. Those values are OR-ed into one residue word per line. A line whose
//! residue is not zero is not decided here: it goes to the general per-line rules
//! (`dec5::event_row` / `click_row` on the plain split), which is where exactness
//! comes from. So the kernel only has to be right on the lines it accepts, and
//! for those it computes exactly what `tsfast::Win` and the general parser give.
//!
//! Dates go through a memo of the run's dates: every calendar date from a day
//! before the click window to a week after the event window, keyed by its text,
//! with the epoch second of its midnight. A date outside the memo (or text that is
//! not a date) is residue.

use core::arch::x86_64::*;

/// One memoized date: the 8 bytes at the date's start (`YYYY-MM-`), the 8 bytes
/// two further on (`YY-MM-DD`), and the epoch second of its midnight.
#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct DEnt {
    w0: u64,
    w1: u64,
    dsec: i64,
    _pad: u64,
}

const MEMO_BITS: u32 = 8;

/// Per-form constants of the timestamp lane (one 16-byte lane per field: `ts` in
/// the low lane, `ingest_ts` in the high one, which is always F1).
#[repr(C, align(32))]
struct TsRow {
    lit: [u8; 32],   // expected bytes at literal positions (subtracted)
    litm: [u8; 32],  // which bits of (byte - lit) must be zero
    nine: [u8; 32],  // 9 at digit positions, 255 elsewhere (saturating x - nine)
    shuf: [i8; 32],  // digit pairs to 16-bit slots
    max: [i16; 16],  // largest value of each 2-digit slot
    w: [i16; 16],    // slot weights: pairs of slots to 32-bit sums
}

/// Rows: 0 = F1 (`THH:MM:SSZ` at +10), 1 = F2 (`THH:MM:SS+HH:MM` at +10), 2 = F3
/// of 13 digits (at -3, so they fill bytes 3..16). The high lane is F1 throughout.
static ROWS: [TsRow; 3] = {
    const F1_LIT: [u8; 16] = [b'T', 0, 0, b':', 0, 0, b':', 0, 0, b'Z', 0, 0, 0, 0, 0, 0];
    const F1_LITM: [u8; 16] = [255, 0, 0, 255, 0, 0, 255, 0, 0, 255, 0, 0, 0, 0, 0, 0];
    const F1_NINE: [u8; 16] = [255, 9, 9, 255, 9, 9, 255, 9, 9, 255, 255, 255, 255, 255, 255, 255];
    const F1_SHUF: [i8; 16] = [1, 2, 4, 5, 7, 8, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1];
    const F1_MAX: [i16; 8] = [23, 59, 59, 0, 0, 0, 0, 0];
    const F1_W: [i16; 8] = [3600, 60, 1, 0, 0, 0, 0, 0];
    // F2: the sign byte (9) must be '+' or '-': (b - '+') & 0xFD is zero for both.
    const F2_LIT: [u8; 16] = [b'T', 0, 0, b':', 0, 0, b':', 0, 0, b'+', 0, 0, b':', 0, 0, 0];
    const F2_LITM: [u8; 16] = [255, 0, 0, 255, 0, 0, 255, 0, 0, 0xFD, 0, 0, 255, 0, 0, 0];
    const F2_NINE: [u8; 16] = [255, 9, 9, 255, 9, 9, 255, 9, 9, 255, 9, 9, 255, 9, 9, 255];
    const F2_SHUF: [i8; 16] = [1, 2, 4, 5, 7, 8, -1, -1, 10, 11, 13, 14, -1, -1, -1, -1];
    const F2_MAX: [i16; 8] = [23, 59, 59, 0, 23, 59, 0, 0];
    const F2_W: [i16; 8] = [3600, 60, 1, 0, 3600, 60, 0, 0];
    const F3_LIT: [u8; 16] = [0; 16];
    const F3_LITM: [u8; 16] = [0; 16];
    const F3_NINE: [u8; 16] = [255, 255, 255, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9];
    const F3_SHUF: [i8; 16] = [-1, -1, -1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    const F3_MAX: [i16; 8] = [i16::MAX; 8];
    const F3_W: [i16; 8] = [100, 1, 100, 1, 100, 1, 100, 1];
    const fn cat8(a: [u8; 16], b: [u8; 16]) -> [u8; 32] {
        let mut r = [0u8; 32];
        let mut i = 0;
        while i < 16 {
            r[i] = a[i];
            r[16 + i] = b[i];
            i += 1;
        }
        r
    }
    const fn cati(a: [i8; 16], b: [i8; 16]) -> [i8; 32] {
        let mut r = [0i8; 32];
        let mut i = 0;
        while i < 16 {
            r[i] = a[i];
            r[16 + i] = b[i];
            i += 1;
        }
        r
    }
    const fn cat16(a: [i16; 8], b: [i16; 8]) -> [i16; 16] {
        let mut r = [0i16; 16];
        let mut i = 0;
        while i < 8 {
            r[i] = a[i];
            r[8 + i] = b[i];
            i += 1;
        }
        r
    }
    const fn row(lit: [u8; 16], litm: [u8; 16], nine: [u8; 16], shuf: [i8; 16], max: [i16; 8], w: [i16; 8]) -> TsRow {
        TsRow {
            lit: cat8(lit, F1_LIT),
            litm: cat8(litm, F1_LITM),
            nine: cat8(nine, F1_NINE),
            shuf: cati(shuf, F1_SHUF),
            max: cat16(max, F1_MAX),
            w: cat16(w, F1_W),
        }
    }
    [
        row(F1_LIT, F1_LITM, F1_NINE, F1_SHUF, F1_MAX, F1_W),
        row(F2_LIT, F2_LITM, F2_NINE, F2_SHUF, F2_MAX, F2_W),
        row(F3_LIT, F3_LITM, F3_NINE, F3_SHUF, F3_MAX, F3_W),
    ]
};

/// By `ts` length (< 128): the row, the offset of the lane load from the field
/// start, and 1 for a length no row takes.
static FORM: [(u8, i8, u8); 128] = {
    let mut t = [(0u8, 10i8, 1u8); 128];
    t[20] = (0, 10, 0);
    t[25] = (1, 10, 0);
    t[13] = (2, -3, 0);
    t
};

pub(crate) struct Kern {
    memo: Box<[DEnt; 1 << MEMO_BITS]>,
    mul: u64,
    base: i64,   // ms (click window start)
    base_s: i64, // in seconds
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

impl Kern {
    /// For a run whose click window starts at `base` and whose event window ends at
    /// `end` (ms since the epoch).
    pub(crate) fn new(base: i64, end: i64) -> Kern {
        const DAYMS: i64 = 86_400_000;
        let first = base.div_euclid(DAYMS) - 1;
        let last = (end - 1).div_euclid(DAYMS) + 8;
        let mut dates = Vec::new();
        for day in first..=last {
            let (y, m, d) = civil(day);
            if (0..=9999).contains(&y) {
                let t = format!("{y:04}-{m:02}-{d:02}");
                let b = t.as_bytes();
                let w0 = u64::from_le_bytes(b[0..8].try_into().unwrap());
                let w1 = u64::from_le_bytes(b[2..10].try_into().unwrap());
                dates.push(DEnt { w0, w1, dsec: day * 86_400, _pad: 0 });
            }
        }
        let mut memo = Box::new([DEnt::default(); 1 << MEMO_BITS]);
        // A multiplier that sends every date to its own slot (found by trying a
        // fixed sequence; with ~40 dates in 256 slots a few tries suffice). If none
        // is found, the memo stays empty: every date is residue, still exact.
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut mul = 0;
        for _ in 0..100_000 {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            let m = (z ^ (z >> 31)) | 1;
            let mut used = [false; 1 << MEMO_BITS];
            if dates.iter().all(|e| {
                let i = (e.w1.wrapping_mul(m) >> (64 - MEMO_BITS)) as usize;
                !std::mem::replace(&mut used[i], true)
            }) {
                mul = m;
                break;
            }
        }
        if mul != 0 {
            for e in &dates {
                memo[(e.w1.wrapping_mul(mul) >> (64 - MEMO_BITS)) as usize] = *e;
            }
        }
        Kern { memo, mul, base, base_s: base.div_euclid(1000) }
    }

    /// The epoch second of midnight of the date `YYYY-MM-DD` at p, and a residue
    /// that is zero iff those 10 bytes are a memoized date.
    #[inline(always)]
    pub(crate) unsafe fn day(&self, p: *const u8) -> (i64, u64) {
        let w0 = (p as *const u64).read_unaligned();
        let w1 = (p.add(2) as *const u64).read_unaligned();
        let e = self.memo.get_unchecked((w1.wrapping_mul(self.mul) >> (64 - MEMO_BITS)) as usize);
        (e.dsec, (w0 ^ e.w0) | (w1 ^ e.w1))
    }

    /// `ts` (pt, nt) and `ingest_ts` (pi, ni) of an event line, quotes removed:
    /// (t, ingest ms, ingest key, residue). With a zero residue, t is what
    /// `Win::ts` gives and (ingest ms, key) what `Win::ingest` gives.
    #[inline(always)]
    pub(crate) unsafe fn ts_ingest(&self, pt: *const u8, nt: usize, pi: *const u8, ni: usize) -> (i64, i64, u32, u64) {
        let (row, off, badlen) = *FORM.get_unchecked(nt & 127);
        let r = ROWS.get_unchecked(row as usize);
        let v = _mm256_inserti128_si256(
            _mm256_castsi128_si256(_mm_loadu_si128(pt.offset(off as isize) as *const __m128i)),
            _mm_loadu_si128(pi.add(10) as *const __m128i),
            1,
        );
        let ld = |a: *const u8| _mm256_load_si256(a as *const __m256i);
        let x = _mm256_sub_epi8(v, _mm256_set1_epi8(b'0' as i8));
        let lit = _mm256_and_si256(_mm256_sub_epi8(v, ld(r.lit.as_ptr())), ld(r.litm.as_ptr()));
        let bad = _mm256_or_si256(_mm256_subs_epu8(x, ld(r.nine.as_ptr())), lit);
        let y = _mm256_shuffle_epi8(x, ld(r.shuf.as_ptr() as *const u8));
        let z = _mm256_maddubs_epi16(y, _mm256_set1_epi16(0x010A));
        let bad = _mm256_or_si256(bad, _mm256_cmpgt_epi16(z, ld(r.max.as_ptr() as *const u8)));
        let s = _mm256_madd_epi16(z, ld(r.w.as_ptr() as *const u8));
        let vres = (_mm256_testz_si256(bad, bad) == 0) as u64;
        let hi = _mm256_extracti128_si256::<1>(s);
        // ingest_ts: F1
        let (di, ri) = self.day(pi);
        let si = di + _mm_cvtsi128_si32(hi) as i64 + _mm_extract_epi32::<1>(hi) as i64;
        let ing_ms = si * 1000;
        let key = ((si - self.base_s) as i32 as u32) ^ 0x8000_0000;
        let mut res = vres | ri | (ni as u64 ^ 20) | badlen as u64;
        let lo = _mm256_castsi256_si128(s);
        let (a0, a1) = (_mm_cvtsi128_si32(lo) as i64, _mm_extract_epi32::<1>(lo) as i64);
        let t = if row == 2 {
            let (a2, a3) = (_mm_extract_epi32::<2>(lo) as i64, _mm_extract_epi32::<3>(lo) as i64);
            ((a0 * 10_000 + a1) * 10_000 + a2) * 10_000 + a3 - self.base
        } else {
            let (dt, rt) = self.day(pt);
            res |= rt;
            let mut st = dt + a0 + a1;
            if row == 1 {
                // UTC = local time - offset; '+' has bit 1 set, '-' not.
                let off = _mm_extract_epi32::<2>(lo) as i64;
                let plus = ((*pt.add(19) >> 1) & 1) as i64;
                st += off - 2 * off * plus;
            }
            st * 1000 - self.base
        };
        (t, ing_ms, key, res)
    }

    /// One `ts` field (clicks): (t, residue); with a zero residue t is what
    /// `Win::ts` gives. The high lane repeats the field (its checks are ignored).
    #[inline(always)]
    pub(crate) unsafe fn ts(&self, pt: *const u8, nt: usize) -> (i64, u64) {
        let (row, off, badlen) = *FORM.get_unchecked(nt & 127);
        let r = ROWS.get_unchecked(row as usize);
        let v = _mm_loadu_si128(pt.offset(off as isize) as *const __m128i);
        let ld = |a: *const u8| _mm_load_si128(a as *const __m128i);
        let x = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let lit = _mm_and_si128(_mm_sub_epi8(v, ld(r.lit.as_ptr())), ld(r.litm.as_ptr()));
        let bad = _mm_or_si128(_mm_subs_epu8(x, ld(r.nine.as_ptr())), lit);
        let y = _mm_shuffle_epi8(x, ld(r.shuf.as_ptr() as *const u8));
        let z = _mm_maddubs_epi16(y, _mm_set1_epi16(0x010A));
        let bad = _mm_or_si128(bad, _mm_cmpgt_epi16(z, ld(r.max.as_ptr() as *const u8)));
        let s = _mm_madd_epi16(z, ld(r.w.as_ptr() as *const u8));
        let mut res = (_mm_testz_si128(bad, bad) == 0) as u64 | badlen as u64;
        let (a0, a1) = (_mm_cvtsi128_si32(s) as i64, _mm_extract_epi32::<1>(s) as i64);
        let t = if row == 2 {
            let (a2, a3) = (_mm_extract_epi32::<2>(s) as i64, _mm_extract_epi32::<3>(s) as i64);
            ((a0 * 10_000 + a1) * 10_000 + a2) * 10_000 + a3 - self.base
        } else {
            let (dt, rt) = self.day(pt);
            res |= rt;
            let mut st = dt + a0 + a1;
            if row == 1 {
                let off = _mm_extract_epi32::<2>(s) as i64;
                let plus = ((*pt.add(19) >> 1) & 1) as i64;
                st += off - 2 * off * plus;
            }
            st * 1000 - self.base
        };
        (t, res)
    }
}

/// `Id::short` and `Id::hash` of the `len` (<= 15) bytes at p.
#[inline(always)]
pub(crate) unsafe fn short_hash(p: *const u8, len: usize) -> (crate::id::Id, u64) {
    let id = crate::id::Id::short(p, len);
    (id, id.hash())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tsfast::{Win, FAIL};

    fn rng(x: &mut u64) -> u64 {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *x
    }

    /// Kernel timestamps against `Win::ts` / `Win::ingest` on random and mutated
    /// text: whenever the kernel accepts (zero residue), the values are equal.
    #[test]
    fn ts_kernel_agrees() {
        let ws: i64 = 1_791_072_000_000 - 3 * 86_400_000; // some midnight
        let base = ws - 7 * 86_400_000;
        let we = ws + 14 * 86_400_000;
        let win = Win::new(base, we);
        let k = Kern::new(base, we);
        let mut x = 0x1234_5678_9abc_def1u64;
        let (mut acc, mut tot) = (0usize, 0usize);
        for i in 0..400_000 {
            // A timestamp near the window in one of the forms.
            let ms = base - 3 * 86_400_000 + (rng(&mut x) % (30 * 86_400_000u64)) as i64;
            let ms = ms - ms % 1000;
            let secs = ms / 1000;
            let (y, mo, d) = civil(secs.div_euclid(86_400));
            let sod = secs.rem_euclid(86_400);
            let (h, mi, s) = (sod / 3600, sod / 60 % 60, sod % 60);
            let mut t = match rng(&mut x) % 3 {
                0 => format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z"),
                1 => {
                    let offm = (rng(&mut x) % (22 * 60)) as i64 - 10 * 60;
                    let l = secs + offm * 60;
                    let (y, mo, d) = civil(l.div_euclid(86_400));
                    let sod = l.rem_euclid(86_400);
                    let sg = if offm < 0 { '-' } else { '+' };
                    format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}{sg}{:02}:{:02}", sod / 3600, sod / 60 % 60, sod % 60, offm.abs() / 60, offm.abs() % 60)
                }
                _ => format!("{}", ms + (rng(&mut x) % 1000) as i64),
            }
            .into_bytes();
            // Mutate some of them: a byte replaced, or the length changed.
            if i % 3 == 0 {
                let j = (rng(&mut x) % t.len() as u64) as usize;
                let c = b"0123456789:-+TZtz ./,9x\x00\xff"[(rng(&mut x) % 23) as usize];
                t[j] = c;
            } else if i % 17 == 0 {
                t.pop();
            }
            let mut buf = vec![b','; 64];
            buf.extend_from_slice(&t);
            buf.extend_from_slice(&[b','; 64]);
            let p = unsafe { buf.as_ptr().add(64) };
            let n = t.len();
            let (kt, res) = unsafe { k.ts(p, n) };
            let want = unsafe { win.ts(buf.as_ptr(), 64, 64 + n) };
            tot += 1;
            if res == 0 {
                acc += 1;
                assert!(want != FAIL && kt == want, "ts {:?}: kernel {kt} general {want}", String::from_utf8_lossy(&t));
            }
            // The same text as ingest_ts beside a valid F1 ts, and as ts beside it.
            let f1 = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z").into_bytes();
            let mut b2 = vec![b','; 64];
            b2.extend_from_slice(&f1);
            b2.push(b',');
            b2.extend_from_slice(&t);
            b2.extend_from_slice(&[b','; 64]);
            let pf = unsafe { b2.as_ptr().add(64) };
            let pt2 = unsafe { b2.as_ptr().add(64 + 21) };
            let (_, ims, ikey, res) = unsafe { k.ts_ingest(pf, 20, pt2, n) };
            if res == 0 {
                let (wms, wkey) = unsafe { win.ingest(b2.as_ptr(), 64 + 21, 64 + 21 + n) };
                assert!(ims == wms && ikey == wkey, "ingest {:?}", String::from_utf8_lossy(&t));
            }
            let (tt, _, _, res) = unsafe { k.ts_ingest(pt2, n, pf, 20) };
            if res == 0 {
                let want = unsafe { win.ts(b2.as_ptr(), 64 + 21, 64 + 21 + n) };
                assert!(want != FAIL && tt == want, "ts pair {:?}: kernel {tt} general {want}", String::from_utf8_lossy(&t));
            }
        }
        // Most unmutated lines must be accepted.
        assert!(acc * 2 > tot, "kernel accepted only {acc} of {tot}");
    }
}
