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

    /// An `ingest_ts` field: (epoch ms, dedup key), or None if it is not a timestamp
    /// (rules.md says it always is a valid F1). The key orders copies of an event:
    /// seconds since `base`, clamped to 32 bits and biased to sort unsigned.
    #[inline(always)]
    pub unsafe fn ingest(&self, d: *const u8, s: usize, e: usize) -> Option<(i64, u32)> {
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
                return Some((secs * 1000, ((secs - self.base_s) as i32 as u32) ^ 0x8000_0000));
            }
        }
        self.ingest_slow(std::slice::from_raw_parts(p, e - s))
    }

    #[cold]
    #[inline(never)]
    fn ingest_slow(&self, b: &[u8]) -> Option<(i64, u32)> {
        let ms = crate::ts::parse_ingest(b)?;
        let key = (ms.div_euclid(1000) - self.base_s).clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32;
        Some((ms, (key as u32) ^ 0x8000_0000))
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
        let fi = unsafe { w.ingest(buf.as_ptr(), 0, n) }.map(|x| x.0);
        assert_eq!(fi, crate::ts::parse_ingest(s.as_bytes()), "ingest {s:?}");
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
