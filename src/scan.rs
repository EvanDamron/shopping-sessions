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
                if qc == 0 {
                    fields[nf] = unsafe { std::slice::from_raw_parts(base.add(fs), pos - fs) };
                } else if qc == 2
                    && pos - fs >= 2
                    && unsafe { *base.add(fs) == b'"' && *base.add(pos - 1) == b'"' }
                {
                    fields[nf] = unsafe { std::slice::from_raw_parts(base.add(fs + 1), pos - fs - 2) };
                } else {
                    bad = true;
                }
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
