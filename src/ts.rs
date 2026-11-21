//! Fast timestamp parsing (rules.md "Timestamps"): F1 `YYYY-MM-DDTHH:MM:SSZ`,
//! F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 digits-only epoch milliseconds.
//! `crate::parse_ts` is the straightforward version; tests check they agree.

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// True if every byte selected by `mask` (0xFF per byte) is an ASCII digit.
#[inline(always)]
fn digits_ok(w: u64, mask: u64) -> bool {
    let x = (w ^ ZEROS) & mask;
    // A byte b (after xor with '0') is a digit iff b < 10, i.e. b + 0x76 has no high bit, and b itself has none.
    ((x.wrapping_add(0x7676_7676_7676_7676 & mask)) | x) & 0x8080_8080_8080_8080 & mask == 0
}

#[inline(always)]
fn load8(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// Days from 1970-01-01 to January 1 of each year 0..=10000.
static YEAR_START: [i32; 10001] = {
    let mut t = [0i32; 10001];
    let mut y = 0;
    while y <= 10000 {
        let yy = y as i64 - 1;
        t[y] = (365 * yy + yy.div_euclid(4) - yy.div_euclid(100) + yy.div_euclid(400) - 719_162) as i32;
        y += 1;
    }
    t
};

const CUM: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if (n == 20 || n == 25) && b[4] == b'-' {
        parse_iso(b, n)
    } else {
        parse_f3(b)
    }
}

#[inline(always)]
fn parse_iso(b: &[u8], n: usize) -> Option<i64> {
    let w0 = load8(b, 0); // YYYY-MM-
    let w1 = load8(b, 8); // DDTHH:MM
    // Bytes 16..24 for F2 (":SS+HH:MM" minus the last byte), 16..20 padded for F1.
    let w2 = if n == 25 { load8(b, 16) } else { u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64 };
    const M0: u64 = 0x00FF_FF00_FFFF_FFFF;
    const M1: u64 = 0xFFFF_00FF_FF00_FFFF;
    let m2: u64 = if n == 25 { 0xFF00_FFFF_00FF_FF00 } else { 0x0000_0000_00FF_FF00 };
    let sep_ok = (w0 & 0xFF00_00FF_0000_0000) == 0x2D00_002D_0000_0000
        && (w1 & 0x0000_FF00_00FF_0000) == 0x0000_3A00_0054_0000
        && (w2 & 0xFF) == b':' as u64;
    if !(sep_ok & digits_ok(w0, M0) & digits_ok(w1, M1) & digits_ok(w2, m2)) {
        return None;
    }
    // Digit values with separators zeroed; pair value at byte i = 10 * x[i] + x[i + 1].
    let p0 = {
        let x = (w0 ^ ZEROS) & M0;
        x.wrapping_mul(10).wrapping_add(x >> 8)
    };
    let p1 = {
        let x = (w1 ^ ZEROS) & M1;
        x.wrapping_mul(10).wrapping_add(x >> 8)
    };
    let p2 = {
        let x = (w2 ^ ZEROS) & m2;
        x.wrapping_mul(10).wrapping_add(x >> 8)
    };
    let pick = |p: u64, i: u32| ((p >> (8 * i)) & 0xff) as i64;
    let y = pick(p0, 0) * 100 + pick(p0, 2);
    let mo = pick(p0, 5);
    let d = pick(p1, 0);
    let h = pick(p1, 3);
    let mi = pick(p1, 6);
    let s = pick(p2, 1);
    let ys = YEAR_START[y as usize] as i64;
    let leap = YEAR_START[y as usize + 1] as i64 - ys - 365;
    // Days in month: 30 + (m + m/8) % 2, February 28 + leap.
    let dim = if mo == 2 { 28 + leap } else { 30 + ((mo + mo / 8) & 1) };
    let ok = (1..=12).contains(&mo) & (d >= 1) & (d <= dim) & (h <= 23) & (mi <= 59) & (s <= 59);
    if !ok {
        return None;
    }
    let off = if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        // Offset digits are bytes 20,21 (pair at byte 4 of p2) and 23,24; byte 24 is past w2.
        let e = b[24].wrapping_sub(b'0');
        if e > 9 || b[22] != b':' {
            return None;
        }
        let oh = pick(p2, 4);
        let om = pick(p2, 7) + e as i64;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    let dn = ys + CUM[mo as usize] + (mo > 2) as i64 * leap + d - 1;
    Some(((dn * 86_400 + h * 3600 + mi * 60 + s) - off * 60) * 1000)
}

/// Eight ASCII digits (little-endian load) to their value.
#[inline(always)]
fn parse8(w: u64) -> u64 {
    let v = w.wrapping_sub(ZEROS);
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    let v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    v
}

/// Digits-only epoch milliseconds. A value too large for i64 is valid but outside every
/// window, so it maps to i64::MAX; non-digits give None.
#[inline(always)]
pub fn parse_f3(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut i = 0;
    let n = b.len();
    let mut v: u64 = 0;
    // Strip leading zeros (validating them) so long zero-padded values still parse.
    while i < n && b[i] == b'0' {
        i += 1;
    }
    let sig = n - i;
    if sig > 19 {
        return if b[i..].iter().all(|c| c.is_ascii_digit()) { Some(i64::MAX) } else { None };
    }
    while i + 8 <= n {
        let w = load8(b, i);
        if !digits_ok(w, u64::MAX) {
            return None;
        }
        v = v.wrapping_mul(100_000_000).wrapping_add(parse8(w));
        i += 8;
    }
    while i < n {
        let d = b[i].wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add(d as u64);
        i += 1;
    }
    // At most 19 significant digits: v fits in u64 without wrapping.
    Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(s: &str) {
        let fast = parse_ts(s.as_bytes());
        let slow = crate::parse_ts(s.as_bytes());
        // The slow version returns None for > u64::MAX; both mean "dropped".
        let norm = |x: Option<i64>| x.filter(|&v| v != i64::MAX);
        assert_eq!(norm(fast), norm(slow), "{s:?}");
    }

    #[test]
    fn agrees_with_reference_parser() {
        let cases = [
            "2026-10-03T23:58:10Z", "2026-10-03T19:58:10-04:00", "1791071890000", "0001791071890000",
            "2026-10-03 23:58:10Z", "2026-10-03t23:58:10Z", "2026-10-03T23:58:10z", "2026-10-03T23:58:10",
            "2026-13-03T23:58:10Z", "2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z", "2028-02-29T00:00:00Z",
            "2100-02-29T00:00:00Z", "2000-02-29T00:00:00Z", "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z",
            "2026-10-03T23:59:60Z", "2026-10-03T23:58:10+24:00", "2026-10-03T23:58:10+05:60",
            "2026-10-03T23:58:10+0200", "2026-10-03T23:58:10+23:59", "2026-10-03T23:58:10-00:00",
            "1791071890000.0", "1.79e12", "-5", "86400000", "", "0", "000000000000000000000001791071890000",
            "99999999999999999999", "18446744073709551615", "9223372036854775807", "9223372036854775808",
            "２０２６-10-03T23:58:10Z", "2026-10-03T23:58:1OZ", "2026-1a-03T23:58:10Z", "0000-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z", "1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z", "2026-10-03T23:58:10Z\r",
            "12345678901234567890", "1234567890123456789", "00000000000000000000", "2026-10-03T23:58:10+12:3x",
            "2026-00-10T00:00:00Z", "2026-10-00T00:00:00Z", "2026/10/03T23:58:10Z", "2026-10-03T23-58-10Z",
        ];
        for c in cases {
            check(c);
        }
        // Every byte value at every position of an F1 and F2 template.
        for tpl in ["2028-02-29T13:45:59Z", "2026-10-04T01:30:00+02:00"] {
            for i in 0..tpl.len() {
                for v in 0..=255u8 {
                    let mut b = tpl.as_bytes().to_vec();
                    b[i] = v;
                    let fast = parse_ts(&b);
                    let slow = crate::parse_ts(&b);
                    assert_eq!(fast, slow, "{:?}", String::from_utf8_lossy(&b));
                }
            }
        }
        // Many valid instants round-trip in both forms.
        let mut x: u64 = 12345;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let ms = (x % 300_000_000_000_000) as i64; // years 1970..~11000
            let secs = ms / 1000;
            let day = secs.div_euclid(86400);
            let (y, m, d) = civil(day);
            if y > 9999 {
                continue;
            }
            let sod = secs.rem_euclid(86400);
            let s = format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", sod / 3600, sod / 60 % 60, sod % 60);
            assert_eq!(parse_ts(s.as_bytes()), Some(secs * 1000), "{s}");
            check(&s);
            check(&ms.to_string());
        }
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
}

#[cfg(test)]
mod bench {
    #[test]
    #[ignore]
    fn bench_parse() {
        let data = std::fs::read("data/p20/events/part-00000.csv").unwrap();
        let mut ts: Vec<&[u8]> = Vec::new();
        for line in data.split(|&c| c == b'\n').skip(1).take(3_000_000) {
            let f: Vec<&[u8]> = line.split(|&c| c == b',').collect();
            if f.len() == 7 {
                let t = |x: &'static [u8]| x;
                let _ = t;
                ts.push(f[3]); // ingest: always F1
            }
        }
        let ts: Vec<&[u8]> = ts.into_iter().filter(|x| x.len() == 20 && x[0] != b'"').collect();
        for _ in 0..3 {
            let t0 = std::time::Instant::now();
            let mut acc = 0i64;
            for x in &ts {
                acc = acc.wrapping_add(super::parse_ts(x).unwrap_or(0));
            }
            let dt = t0.elapsed();
            eprintln!("{} parses in {:?} = {:.1} ns/parse (acc {acc})", ts.len(), dt, dt.as_nanos() as f64 / ts.len() as f64);
        }
    }
}
