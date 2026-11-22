//! Timestamp fast paths for one dataset's window.
//!
//! A timestamp only matters if it lands in the click window (window_start - 7 days
//! to window_end). Written as F1 or F2, such a timestamp has a local date within one
//! day of that range, and those dates fall in at most two calendar months, so the
//! fast paths compare the `YYYY-MM-` prefix against those two months and never do
//! calendar arithmetic. Anything a fast path does not accept (other forms, other
//! months, invalid text) is handed to `crate::ts`, the general parser, so the result
//! is the general parser's whenever the two could differ.

const ZEROS: u64 = 0x3030_3030_3030_3030;
pub const FAIL: i64 = i64::MIN;
const DAY: i64 = 86_400_000;

#[inline(always)]
fn digits_ok(w: u64, mask: u64) -> bool {
    let x = (w ^ ZEROS) & mask;
    ((x.wrapping_add(0x7676_7676_7676_7676 & mask)) | x) & 0x8080_8080_8080_8080 & mask == 0
}

#[inline(always)]
unsafe fn load8(p: *const u8) -> u64 {
    (p as *const u64).read_unaligned()
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1")))]
#[inline(always)]
unsafe fn load4(p: *const u8) -> u64 {
    (p as *const u32).read_unaligned() as u64
}

/// Digit pairs: byte i of the result is 10 * x[i] + x[i + 1] for the masked digits.
#[inline(always)]
fn pairs(w: u64, mask: u64) -> u64 {
    let x = (w ^ ZEROS) & mask;
    x.wrapping_mul(10).wrapping_add(x >> 8)
}

#[inline(always)]
fn pick(p: u64, i: u32) -> i64 {
    ((p >> (8 * i)) & 0xff) as i64
}

/// Eight ASCII digits (little-endian load) to their value.
#[inline(always)]
fn parse8(w: u64) -> u64 {
    let v = w.wrapping_sub(ZEROS);
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32
}


/// `DDTHH:MM:SS` at p (16 bytes readable): the day, the seconds `HH:MM:SS` make, and
/// whether the text is exactly that shape with an hour, minute and second in range.
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
#[inline(always)]
unsafe fn dhms(p: *const u8) -> (i64, i64, bool) {
    use core::arch::x86_64::*;
    let v = _mm_loadu_si128(p as *const __m128i); // D D T H H : M M : S S ? ? ? ? ?
    let x = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    // A digit where one belongs (saturating x - 9 is 0), and the three separators.
    let nine = _mm_setr_epi8(9, 9, -1, 9, 9, -1, 9, 9, -1, 9, 9, -1, -1, -1, -1, -1);
    let seps = _mm_setr_epi8(0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, 0, 0, 0, 0, 0);
    let sepm = _mm_setr_epi8(0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0);
    let bad = _mm_or_si128(_mm_subs_epu8(x, nine), _mm_and_si128(_mm_xor_si128(v, seps), sepm));
    // Digits to DD, HH, MM, SS, then to DD * 24 + HH and MM * 60 + SS.
    let y = _mm_shuffle_epi8(x, _mm_setr_epi8(0, 1, 3, 4, 6, 7, 9, 10, -1, -1, -1, -1, -1, -1, -1, -1));
    let z = _mm_maddubs_epi16(y, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0, 0, 0));
    let over = _mm_cmpgt_epi16(z, _mm_setr_epi16(i16::MAX, 23, 59, 59, 0, 0, 0, 0));
    let bad = _mm_or_si128(bad, over);
    let hm = _mm_madd_epi16(z, _mm_setr_epi16(0, 3600, 60, 1, 0, 0, 0, 0));
    let d = _mm_extract_epi16(z, 0) as i64;
    let part = _mm_cvtsi128_si32(hm) as i64 + _mm_extract_epi32(hm, 1) as i64;
    (d, part, _mm_testz_si128(bad, bad) != 0)
}

#[derive(Clone, Copy)]
struct Month {
    prefix: u64, // "YYYY-MM-" as a little-endian word
    day0: i64,   // epoch day of the month's first day, minus one
    lo: i64,     // days of the month that can be in the window
    hi: i64,
}

pub struct Win {
    m: [Month; 2],
    pub base: i64,   // window_start - 7 days, ms: stored times are relative to it
    pub base_s: i64, // base in whole seconds, as the general parser's ingest keys use
    pub span: i64,   // window_end - base: a time t is in the click window iff 0 <= t < span
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

impl Win {
    /// `base` is the start of the click window and `end` the window's end, in ms.
    pub fn new(base: i64, end: i64) -> Win {
        // Local dates of in-window instants: one day either side (offsets are under 24 h).
        let first = base.div_euclid(DAY) - 1;
        let last = (end - 1).div_euclid(DAY) + 1;
        let none = Month { prefix: 0, day0: 0, lo: 1, hi: 0 };
        let mut m = [none; 2];
        let mut k = 0;
        let mut day = first;
        while day <= last {
            let (y, mo, d) = civil(day);
            if (0..=9999).contains(&y) {
                let text = format!("{y:04}-{mo:02}-");
                let prefix = u64::from_le_bytes(text.as_bytes().try_into().unwrap());
                if k > 0 && m[k - 1].prefix == prefix {
                    m[k - 1].hi = d;
                } else {
                    assert!(k < 2, "window spans more than two months");
                    m[k] = Month { prefix, day0: day - d, lo: d, hi: d };
                    k += 1;
                }
            }
            day += 1;
        }
        if k == 1 {
            m[1] = m[0];
        }
        Win { m, base, base_s: base.div_euclid(1000), span: end - base }
    }

    /// The month matching the `YYYY-MM-` word, branch-free; `ok` is false if neither does.
    #[inline(always)]
    fn month(&self, w0: u64) -> (bool, &Month) {
        let is0 = w0 == self.m[0].prefix;
        let is1 = w0 == self.m[1].prefix;
        (is0 | is1, if is0 { &self.m[0] } else { &self.m[1] })
    }

    /// F1 (`YYYY-MM-DDTHH:MM:SSZ`, 20 bytes at p) to ms since `base`, or FAIL.
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
    #[inline(always)]
    pub unsafe fn f1(&self, p: *const u8) -> i64 {
        let (mok, m) = self.month(load8(p));
        let (d, part, ok) = dhms(p.add(8));
        if ok & mok & (d >= m.lo) & (d <= m.hi) & (*p.add(19) == b'Z') {
            ((m.day0 + d) * 86_400 + part) * 1000 - self.base
        } else {
            FAIL
        }
    }

    /// F1 (`YYYY-MM-DDTHH:MM:SSZ`, 20 bytes at p) to ms since `base`, or FAIL.
    #[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1")))]
    #[inline(always)]
    pub unsafe fn f1(&self, p: *const u8) -> i64 {
        const M1: u64 = 0xFFFF_00FF_FF00_FFFF;
        let (w0, w1, w2) = (load8(p), load8(p.add(8)), load4(p.add(16)));
        let (mok, m) = self.month(w0);
        let sep = ((w1 & 0x0000_FF00_00FF_0000) == 0x0000_3A00_0054_0000) & ((w2 & 0xFF00_00FF) == 0x5A00_003A);
        let dig = digits_ok(w1, M1) & digits_ok(w2, 0x00FF_FF00);
        let (p1, p2) = (pairs(w1, M1), pairs(w2, 0x00FF_FF00));
        let (d, h, mi, s) = (pick(p1, 0), pick(p1, 3), pick(p1, 6), pick(p2, 1));
        let ok = mok & sep & dig & (d >= m.lo) & (d <= m.hi) & (h <= 23) & (mi <= 59) & (s <= 59);
        let secs = (m.day0 + d) * 86_400 + h * 3600 + mi * 60 + s;
        if ok {
            secs * 1000 - self.base
        } else {
            FAIL
        }
    }

    /// F2 (`YYYY-MM-DDTHH:MM:SS+HH:MM`, 25 bytes at p) to ms since `base`, or FAIL.
    #[inline(always)]
    pub unsafe fn f2(&self, p: *const u8) -> i64 {
        const M1: u64 = 0xFFFF_00FF_FF00_FFFF;
        // w3 = bytes 17..25: S S sign H H : M M
        let (w0, w1, w3) = (load8(p), load8(p.add(8)), load8(p.add(17)));
        let (mok, m) = self.month(w0);
        let sign = (w3 >> 16) & 0xFF;
        let sep = ((w1 & 0x0000_FF00_00FF_0000) == 0x0000_3A00_0054_0000)
            & (*p.add(16) == b':')
            & ((w3 & 0x0000_FF00_0000_0000) == 0x0000_3A00_0000_0000)
            & ((sign == b'+' as u64) | (sign == b'-' as u64));
        let dig = digits_ok(w1, M1) & digits_ok(w3, M1);
        let (p1, p3) = (pairs(w1, M1), pairs(w3, M1));
        let (d, h, mi, s) = (pick(p1, 0), pick(p1, 3), pick(p1, 6), pick(p3, 0));
        let (oh, om) = (pick(p3, 3), pick(p3, 6));
        let ok = mok & sep & dig & (d >= m.lo) & (d <= m.hi) & (h <= 23) & (mi <= 59) & (s <= 59) & (oh <= 23) & (om <= 59);
        let off = (oh * 60 + om) * 60;
        let off = if sign == b'-' as u64 { -off } else { off };
        let secs = (m.day0 + d) * 86_400 + h * 3600 + mi * 60 + s - off;
        if ok {
            secs * 1000 - self.base
        } else {
            FAIL
        }
    }

    /// F3 of exactly 13 digits at p to ms since `base`, or FAIL.
    #[inline(always)]
    pub unsafe fn f3_13(&self, p: *const u8) -> i64 {
        let (a, b) = (load8(p), load8(p.add(5)));
        if digits_ok(a, u64::MAX) & digits_ok(b, u64::MAX) {
            ((parse8(a) / 1000) * 100_000_000 + parse8(b)) as i64 - self.base
        } else {
            FAIL
        }
    }


    /// `(ts(pt, 0, nt), ingest(pi, 0, ni))` in one go: the `DDTHH:MM:SS` parts of
    /// both fields are checked and folded in one 256-bit pass (lane 0 `ts` as F1,
    /// lane 1 `ingest_ts`), a 13-digit F3 `ts` is read beside it, and the form is
    /// chosen by the length without a branch. Anything else (F2, other lengths,
    /// anything that fails a check) goes to `ts` / `ingest`. Reads 24 bytes at each.
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[inline(always)]
    pub unsafe fn ts_ingest(&self, pt: *const u8, nt: usize, pi: *const u8, ni: usize) -> (i64, i64, u32) {
        use core::arch::x86_64::*;
        let v = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128(pt.add(8) as *const __m128i)), _mm_loadu_si128(pi.add(8) as *const __m128i), 1);
        let x = _mm256_sub_epi8(v, _mm256_set1_epi8(b'0' as i8));
        let nine = _mm256_broadcastsi128_si256(_mm_setr_epi8(9, 9, -1, 9, 9, -1, 9, 9, -1, 9, 9, -1, -1, -1, -1, -1));
        let seps = _mm256_broadcastsi128_si256(_mm_setr_epi8(0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, 0, 0, 0, 0, 0));
        let sepm = _mm256_broadcastsi128_si256(_mm_setr_epi8(0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0));
        let bad = _mm256_or_si256(_mm256_subs_epu8(x, nine), _mm256_and_si256(_mm256_xor_si256(v, seps), sepm));
        let y = _mm256_shuffle_epi8(x, _mm256_broadcastsi128_si256(_mm_setr_epi8(0, 1, 3, 4, 6, 7, 9, 10, -1, -1, -1, -1, -1, -1, -1, -1)));
        let z = _mm256_maddubs_epi16(y, _mm256_broadcastsi128_si256(_mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0, 0, 0)));
        let over = _mm256_cmpgt_epi16(z, _mm256_broadcastsi128_si256(_mm_setr_epi16(i16::MAX, 23, 59, 59, 0, 0, 0, 0)));
        let bad = _mm256_or_si256(bad, over);
        let hm = _mm256_madd_epi16(z, _mm256_broadcastsi128_si256(_mm_setr_epi16(0, 3600, 60, 1, 0, 0, 0, 0)));
        let good = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bad, _mm256_setzero_si256())) as u32;
        let (dt, di) = (_mm256_extract_epi16::<0>(z) as i64, _mm256_extract_epi16::<8>(z) as i64);
        let pt_ = _mm256_extract_epi32::<0>(hm) as i64 + _mm256_extract_epi32::<1>(hm) as i64;
        let pi_ = _mm256_extract_epi32::<4>(hm) as i64 + _mm256_extract_epi32::<5>(hm) as i64;
        // ingest_ts: as `ingest`
        let (moki, mi) = self.month(load8(pi));
        let oki = (good >> 16 == 0xFFFF) & moki & (*pi.add(19) == b'Z') & (ni == 20);
        let secs = (mi.day0 + di) * 86_400 + pi_;
        let (ing_ms, key) = if oki {
            (secs * 1000, ((secs - self.base_s) as i32 as u32) ^ 0x8000_0000)
        } else {
            self.ingest_cold(pi, ni)
        };
        // ts: by its length (a branch is cheaper than working out both forms):
        // F1 from the lane, as `f1`; 13-digit F3 as `f3_13`; anything else `ts`.
        let t = if nt == 20 {
            let (mok, m) = self.month(load8(pt));
            if (good & 0xFFFF == 0xFFFF) & mok & (dt >= m.lo) & (dt <= m.hi) & (*pt.add(19) == b'Z') {
                ((m.day0 + dt) * 86_400 + pt_) * 1000 - self.base
            } else {
                self.ts_cold(pt, nt)
            }
        } else if nt == 13 {
            let (w0, w5) = (load8(pt), load8(pt.add(5)));
            if digits_ok(w0, u64::MAX) & digits_ok(w5, u64::MAX) {
                ((parse8(w0) / 1000) * 100_000_000 + parse8(w5)) as i64 - self.base
            } else {
                self.ts_cold(pt, nt)
            }
        } else {
            self.ts_cold(pt, nt)
        };
        (t, ing_ms, key)
    }

    #[cold]
    #[inline(never)]
    unsafe fn ts_cold(&self, p: *const u8, n: usize) -> i64 {
        self.ts(p, 0, n)
    }

    #[cold]
    #[inline(never)]
    unsafe fn ingest_cold(&self, p: *const u8, n: usize) -> (i64, u32) {
        self.ingest(p, 0, n)
    }

    /// A `ts` field to ms since `base`; FAIL if it is not a timestamp. The caller
    /// checks the window (a FAIL is outside every window).
    #[inline(always)]
    pub unsafe fn ts(&self, d: *const u8, s: usize, e: usize) -> i64 {
        let n = e - s;
        let p = d.add(s);
        let t = if n == 20 {
            self.f1(p)
        } else if n == 13 {
            self.f3_13(p)
        } else if n == 25 {
            self.f2(p)
        } else {
            FAIL
        };
        if t != FAIL {
            t
        } else {
            self.ts_slow(std::slice::from_raw_parts(p, n))
        }
    }

    #[cold]
    #[inline(never)]
    fn ts_slow(&self, b: &[u8]) -> i64 {
        match crate::ts::parse_ts(b) {
            Some(t) => t.checked_sub(self.base).unwrap_or(i64::MAX),
            None => FAIL,
        }
    }

    /// An `ingest_ts` field: (epoch ms, dedup key). The key orders copies of an event:
    /// seconds since `base`, clamped to 32 bits and biased to sort unsigned. Text that
    /// is not a timestamp (rules.md says it always is a valid F1) gives
    /// (i64::MAX, INGEST_NONE): later than anything, so the event counts as late.
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
    #[inline(always)]
    pub unsafe fn ingest(&self, d: *const u8, s: usize, e: usize) -> (i64, u32) {
        let p = d.add(s);
        if e - s == 20 {
            let (mok, m) = self.month(load8(p));
            let (day, part, ok) = dhms(p.add(8));
            if ok & mok & (*p.add(19) == b'Z') {
                // F1 in a window month, every digit a digit: the general parser's arithmetic.
                let secs = (m.day0 + day) * 86_400 + part;
                return (secs * 1000, ((secs - self.base_s) as i32 as u32) ^ 0x8000_0000);
            }
        }
        self.ingest_slow(std::slice::from_raw_parts(p, e - s))
    }

    /// An `ingest_ts` field: (epoch ms, dedup key). The key orders copies of an event:
    /// seconds since `base`, clamped to 32 bits and biased to sort unsigned. Text that
    /// is not a timestamp (rules.md says it always is a valid F1) gives
    /// (i64::MAX, INGEST_NONE): later than anything, so the event counts as late.
    #[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1")))]
    #[inline(always)]
    pub unsafe fn ingest(&self, d: *const u8, s: usize, e: usize) -> (i64, u32) {
        const M1: u64 = 0xFFFF_00FF_FF00_FFFF;
        let p = d.add(s);
        if e - s == 20 {
            let (w0, w1, w2) = (load8(p), load8(p.add(8)), load4(p.add(16)));
            let (mok, m) = self.month(w0);
            if mok & ((w1 & 0x00FF_0000) == 0x0054_0000) & (w2 >> 24 == b'Z' as u64) {
                // Shaped like F1 in a window month: its digits are taken as given, exactly
                // as the general parser does.
                let (p1, p2) = (pairs(w1, M1), pairs(w2, 0x00FF_FF00));
                let secs = (m.day0 + pick(p1, 0)) * 86_400 + pick(p1, 3) * 3600 + pick(p1, 6) * 60 + pick(p2, 1);
                return (secs * 1000, ((secs - self.base_s) as i32 as u32) ^ 0x8000_0000);
            }
        }
        self.ingest_slow(std::slice::from_raw_parts(p, e - s))
    }

    #[cold]
    #[inline(never)]
    fn ingest_slow(&self, b: &[u8]) -> (i64, u32) {
        match crate::ts::parse_ingest(b) {
            Some(ms) => {
                let key = (ms.div_euclid(1000) - self.base_s).clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32;
                (ms, (key as u32) ^ 0x8000_0000)
            }
            None => (i64::MAX, INGEST_NONE),
        }
    }
}

/// The key of a copy whose `ingest_ts` could not be read: after every real one.
pub const INGEST_NONE: u32 = (i32::MAX as u32) ^ 0x8000_0000;

#[cfg(test)]
mod tests {
    use super::*;

    fn check(w: &Win, s: &str) {
        let mut buf = s.as_bytes().to_vec();
        let n = buf.len();
        buf.extend_from_slice(&[b','; 40]);
        let fast = unsafe { w.ts(buf.as_ptr(), 0, n) };
        let slow = match crate::ts::parse_ts(s.as_bytes()) {
            Some(t) => t.checked_sub(w.base).unwrap_or(i64::MAX),
            None => FAIL,
        };
        // Outside the click window the two may differ: only "not in the window" matters.
        let inw = |t: i64| t != FAIL && t >= 0 && t < w.span;
        if inw(fast) || inw(slow) {
            assert_eq!(fast, slow, "{s:?}");
        }
        let fi = unsafe { w.ingest(buf.as_ptr(), 0, n) };
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        {
            // Every pairing of this text as ts with a valid ingest_ts, and as ingest_ts with a valid ts.
            let good = b"2026-10-03T23:58:10Z";
            let mut g = good.to_vec();
            g.extend_from_slice(&[b','; 40]);
            let gi = unsafe { w.ingest(g.as_ptr(), 0, 20) };
            let gt = unsafe { w.ts(g.as_ptr(), 0, 20) };
            assert_eq!(unsafe { w.ts_ingest(buf.as_ptr(), n, g.as_ptr(), 20) }, (fast, gi.0, gi.1), "ts_ingest ts {s:?}");
            assert_eq!(unsafe { w.ts_ingest(g.as_ptr(), 20, buf.as_ptr(), n) }, (gt, fi.0, fi.1), "ts_ingest ingest {s:?}");
        }
        let want = match crate::ts::parse_ingest(s.as_bytes()) {
            Some(ms) => {
                let key = (ms.div_euclid(1000) - w.base_s).clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32;
                (ms, (key as u32) ^ 0x8000_0000)
            }
            None => (i64::MAX, INGEST_NONE),
        };
        assert_eq!(fi, want, "ingest {s:?}");
    }

    #[test]
    fn fast_paths_agree_with_the_general_parser() {
        let day = 86_400_000i64;
        // Windows at month and year edges, leap days included.
        let starts = [
            "2026-10-01T00:00:00Z", "2028-02-20T00:00:00Z", "2027-02-20T00:00:00Z", "2025-12-25T00:00:00Z",
            "2026-01-01T00:00:00Z", "2028-03-01T00:00:00Z", "2026-09-07T00:00:00Z", "2100-02-22T00:00:00Z",
        ];
        let mut x: u64 = 88172645463325252;
        let mut rnd = move |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        for st in starts {
            let ws = crate::ts::parse_ts(st.as_bytes()).unwrap();
            let w = Win::new(ws - 7 * day, ws + 14 * day);
            for _ in 0..40_000 {
                // An instant around the window, in each form.
                let ms = ws - 12 * day + rnd(34 * day as u64) as i64;
                let secs = ms.div_euclid(1000);
                let fmt = |secs: i64, tail: &str| {
                    let (y, m, d) = civil(secs.div_euclid(86_400));
                    let sod = secs.rem_euclid(86_400);
                    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}{tail}", sod / 3600, sod / 60 % 60, sod % 60)
                };
                check(&w, &fmt(secs, "Z"));
                check(&w, &ms.to_string());
                let (oh, om) = (rnd(24) as i64, rnd(60) as i64);
                let off = (oh * 60 + om) * 60;
                check(&w, &fmt(secs + off, &format!("+{oh:02}:{om:02}")));
                check(&w, &fmt(secs - off, &format!("-{oh:02}:{om:02}")));
                // The same text with one byte changed.
                let mut t = if rnd(2) == 0 { fmt(secs, "Z") } else { fmt(secs + off, &format!("+{oh:02}:{om:02}")) }.into_bytes();
                let i = rnd(t.len() as u64) as usize;
                t[i] = b"0123456789:-+TZtz .9"[rnd(20) as usize];
                check(&w, &String::from_utf8(t).unwrap());
            }
            for s in [
                "", "0", "86400000", "1791071890000.0", "1.79e12", "-5", "0001791071890000", "00000000000000000000",
                "0000000000000000000000000", "99999999999999999999", "2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z",
                "2028-02-29T00:00:00Z", "2028-02-30T00:00:00Z", "2026-10-03T24:00:00Z", "2026-10-03T23:58:10+24:00",
                "2026-10-03T23:58:10+05:60", "2026-10-03T23:58:10+0200", "2026-10-03T23:58:10-00:00", "2026-10-03T23:58:10Z\r",
                "2026-10-00T00:00:00Z", "2026-10-32T00:00:00Z", "2026-09-31T00:00:00Z", "2025-12-32T00:00:00Z",
            ] {
                check(&w, s);
            }
        }
    }
}
