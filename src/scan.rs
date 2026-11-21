//! SIMD CSV scanning: finds commas, newlines and quotes 64 bytes at a time and
//! hands each well-formed line's unquoted fields to a callback (rules.md "CSV dialect").

/// (comma, newline, quote) bit masks for the 64 bytes at p.
#[inline(always)]
unsafe fn classify(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use core::arch::x86_64::*;
        let c = _mm256_set1_epi8(b',' as i8);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |x: __m256i, y: __m256i| -> u64 {
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, x)) as u32 as u64)
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, y)) as u32 as u64) << 32)
        };
        (m(c, c), m(n, n), m(q, q))
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        use core::arch::x86_64::*;
        let c = _mm_set1_epi8(b',' as i8);
        let n = _mm_set1_epi8(b'\n' as i8);
        let q = _mm_set1_epi8(b'"' as i8);
        let (mut cm, mut nm, mut qm) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
            cm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, c)) as u32 as u64) << (16 * k);
            nm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, n)) as u32 as u64) << (16 * k);
            qm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u64) << (16 * k);
        }
        (cm, nm, qm)
    }
    #[cfg(target_arch = "aarch64")]
    {
        use core::arch::aarch64::*;
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(BITS.as_ptr());
        let v0 = vld1q_u8(p);
        let v1 = vld1q_u8(p.add(16));
        let v2 = vld1q_u8(p.add(32));
        let v3 = vld1q_u8(p.add(48));
        let mm = |ch: u8| -> u64 {
            let s = vdupq_n_u8(ch);
            let t0 = vandq_u8(vceqq_u8(v0, s), bits);
            let t1 = vandq_u8(vceqq_u8(v1, s), bits);
            let t2 = vandq_u8(vceqq_u8(v2, s), bits);
            let t3 = vandq_u8(vceqq_u8(v3, s), bits);
            let s0 = vpaddq_u8(t0, t1);
            let s1 = vpaddq_u8(t2, t3);
            let s0 = vpaddq_u8(s0, s1);
            let s0 = vpaddq_u8(s0, s0);
            vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0)
        };
        (mm(b','), mm(b'\n'), mm(b'"'))
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let (mut cm, mut nm, mut qm) = (0u64, 0u64, 0u64);
        for k in 0..64 {
            let c = *p.add(k);
            cm |= ((c == b',') as u64) << k;
            nm |= ((c == b'\n') as u64) << k;
            qm |= ((c == b'"') as u64) << k;
        }
        (cm, nm, qm)
    }
}

/// Calls `f` with the N unquoted fields of every well-formed line in `data`
/// ('\n'-terminated lines; a final unterminated line counts). Malformed lines
/// (wrong field count, a quote anywhere except around a whole field, empty lines)
/// are skipped.
#[inline(always)]
pub fn for_records<const N: usize>(data: &'static [u8], mut f: impl FnMut(&[&'static [u8]; N])) {
    let n = data.len();
    let base = data.as_ptr();
    let mut fields: [&'static [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields seen in the current line
    let mut bad = false;
    let mut fs = 0usize; // current field start
    let mut q_fs = 0u64; // quotes before fs
    let mut qbase = 0u64; // quotes before the current block
    let mut line_start = 0usize;
    let mut tail = [0u8; 64];
    let mut o = 0usize;
    while o < n {
        let (cm, nm, qm) = unsafe {
            if o + 64 <= n {
                classify(base.add(o))
            } else {
                tail[..n - o].copy_from_slice(&data[o..]);
                classify(tail.as_ptr())
            }
        };
        let mut seps = cm | nm;
        while seps != 0 {
            let tz = seps.trailing_zeros();
            let bit = 1u64 << tz;
            seps &= seps - 1;
            let pos = o + tz as usize;
            let qpos = qbase + (qm & (bit - 1)).count_ones() as u64;
            let qc = qpos - q_fs;
            if nf < N {
                // Branch-free: a field is plain (no quotes) or wrapped in exactly one pair.
                let len = pos - fs;
                let (first, last) = unsafe { (*base.add(fs.min(n - 1)), *base.add(pos.max(1) - 1)) };
                let quoted = (qc == 2) & (len >= 2) & (first == b'"') & (last == b'"');
                bad |= (qc != 0) & !quoted;
                let q = quoted as usize;
                fields[nf] = unsafe { std::slice::from_raw_parts(base.add(fs + q), len - 2 * q) };
            }
            nf += 1;
            fs = pos + 1;
            q_fs = qpos;
            if nm & bit != 0 {
                if !bad && nf == N {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                line_start = fs;
            }
        }
        qbase += qm.count_ones() as u64;
        o += 64;
    }
    if line_start < n {
        // Final line without a newline.
        let qc = qbase - q_fs;
        if nf < N {
            if qc == 0 {
                fields[nf] = &data[fs..n];
            } else if qc == 2 && n - fs >= 2 && data[fs] == b'"' && data[n - 1] == b'"' {
                fields[nf] = &data[fs + 1..n - 1];
            } else {
                bad = true;
            }
        }
        nf += 1;
        if !bad && nf == N {
            f(&fields);
        }
    }
}


#[cfg(test)]
mod tests {
    /// The byte-loop splitter applied line by line: the plain reading of the dialect.
    fn by_lines<const N: usize>(d: &'static [u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut s = 0;
        while s < d.len() {
            let e = d[s..].iter().position(|&c| c == b'\n').map_or(d.len(), |k| s + k);
            let mut g: [&'static [u8]; N] = [&[]; N];
            if crate::split::<N>(&d[s..e], &mut g) {
                out.push(g.iter().map(|x| x.to_vec()).collect());
            }
            s = e + 1;
        }
        out
    }

    fn simd<const N: usize>(d: &'static [u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        super::for_records::<N>(d, |f: &[&'static [u8]; N]| out.push(f.iter().map(|x| x.to_vec()).collect()));
        out
    }

    #[test]
    fn simd_scanner_matches_byte_loop() {
        let mut x: u64 = 88172645463325252;
        let mut rnd = move |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        for round in 0..30000 {
            let mut v = Vec::new();
            if round % 2 == 0 {
                // Random bytes heavy in separators and quotes.
                let len = rnd(300) as usize;
                for _ in 0..len {
                    let r = rnd(100);
                    v.push(match r {
                        0..=59 => b'a' + (rnd(26) as u8),
                        60..=74 => b',',
                        75..=82 => b'"',
                        83..=90 => b'\n',
                        91 => b'\r',
                        _ => b' ',
                    });
                }
            } else {
                // Structured lines: fields, some quoted, some broken.
                let lines = rnd(12);
                for _ in 0..lines {
                    let pick = rnd(10);
                    let nf = match pick {
                        0 => rnd(9) as usize,
                        1 => 0,
                        _ => 7,
                    };
                    for k in 0..nf {
                        if k > 0 {
                            v.push(b',');
                        }
                        let cap = if rnd(20) == 0 { 90 } else { 14 };
                        let flen = rnd(cap) as usize;
                        let quote = rnd(5) == 0;
                        if quote {
                            v.push(b'"');
                        }
                        for _ in 0..flen {
                            let c = b"abcxyz019 -:T\r"[rnd(14) as usize];
                            v.push(c);
                        }
                        if rnd(200) == 0 {
                            v.push(b'"');
                        }
                        if quote && rnd(30) != 0 {
                            v.push(b'"');
                        }
                    }
                    if rnd(25) != 0 {
                        v.push(b'\n');
                    }
                }
            }
            let d: &'static [u8] = Box::leak(v.into_boxed_slice());
            assert_eq!(simd::<2>(d), by_lines::<2>(d), "{:?}", String::from_utf8_lossy(d));
            assert_eq!(simd::<4>(d), by_lines::<4>(d), "{:?}", String::from_utf8_lossy(d));
            assert_eq!(simd::<7>(d), by_lines::<7>(d), "{:?}", String::from_utf8_lossy(d));
        }
    }
}

#[cfg(test)]
mod bench {
    #[test]
    #[ignore]
    fn bench_scan() {
        let files: Vec<&'static [u8]> = std::fs::read_dir("data/p20/events")
            .unwrap()
            .take(8)
            .map(|e| &*Box::leak(std::fs::read(e.unwrap().path()).unwrap().into_boxed_slice()))
            .collect();
        let bytes: usize = files.iter().map(|d| d.len()).sum();
        for _ in 0..2 {
            let t0 = std::time::Instant::now();
            let mut acc = 0u64;
            for d in &files {
                let mut o = 0;
                while o + 64 <= d.len() {
                    let (c, l, q) = unsafe { super::classify(d.as_ptr().add(o)) };
                    acc = acc.wrapping_add((c | l).count_ones() as u64 + q.count_ones() as u64);
                    o += 64;
                }
            }
            eprintln!("classify only: {:.3} ns/byte acc {acc}", t0.elapsed().as_nanos() as f64 / bytes as f64);
            let t0 = std::time::Instant::now();
            let mut acc = 0u64;
            for d in &files {
                let mut o = 0;
                while o + 64 <= d.len() {
                    let (c, l, _q) = unsafe { super::classify(d.as_ptr().add(o)) };
                    let mut m = c | l;
                    while m != 0 {
                        acc = acc.wrapping_add(o as u64 + m.trailing_zeros() as u64);
                        m &= m - 1;
                    }
                    o += 64;
                }
            }
            eprintln!("classify + bit loop: {:.3} ns/byte acc {acc}", t0.elapsed().as_nanos() as f64 / bytes as f64);
        }
        for round in 0..6 {
            let t0 = std::time::Instant::now();
            let mut n = 0usize;
            let mut acc = 0usize;
            for d in &files {
                super::for_records::<7>(d, |f| {
                    n += 1;
                    acc = acc.wrapping_add(f[2].len() + f[6].len());
                });
            }
            let dt = t0.elapsed();
            eprintln!(
                "round {round}: {n} lines {:.3} ns/byte {:.1} ns/line acc {acc}",
                dt.as_nanos() as f64 / bytes as f64,
                dt.as_nanos() as f64 / n as f64
            );
        }
    }
}
