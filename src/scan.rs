//! CSV scanning (rules.md "CSV dialect"), in two stages over a small reusable buffer.
//!
//! Stage 1 (`Reader::index`) classifies 64 bytes at a time into comma, newline and
//! quote bit masks, validates every quote with bit arithmetic, and flattens the
//! separator positions into an index. Stage 2 (`Reader::lines`) walks that index a
//! line at a time and hands each well-formed line to the caller, which reads the
//! fields it needs straight from the buffer.
//!
//! Files are read with `pread` into the buffer rather than mapped: the buffer stays
//! in cache, no page tables are built for the input, and nothing has to be torn
//! down at exit.

use memchr::{memchr, memrchr};
use std::fs::File;

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

const FRONT: usize = 64;
const BACK: usize = 192;
/// Index entries past the last separator that are safe to read (look-ahead).
pub const SLACK: usize = 512;

pub struct Reader {
    buf: Vec<u8>,
    cap: usize,
    idx: Vec<u32>,
    nlp: Vec<u32>,
    bad: Vec<u32>,
    nsep: usize,
    nnl: usize,
    start: usize,
}

#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], pos: u64) -> usize {
    use std::os::unix::fs::FileExt;
    loop {
        match f.read_at(buf, pos) {
            Ok(n) => return n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read: {e}"),
        }
    }
}

#[cfg(not(unix))]
fn read_at(f: &File, buf: &mut [u8], pos: u64) -> usize {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = f;
    f.seek(SeekFrom::Start(pos)).expect("seek");
    f.read(buf).expect("read")
}

impl Reader {
    pub fn new(cap: usize) -> Reader {
        let cap = cap.max(64);
        Reader {
            buf: vec![0u8; FRONT + cap + BACK],
            cap,
            idx: vec![0u32; cap + SLACK],
            nlp: vec![0u32; cap + 8],
            bad: vec![0u32; cap + 72],
            nsep: 0,
            nnl: 0,
            start: 0,
        }
    }

    #[cold]
    fn grow(&mut self) {
        let cap = self.cap * 2;
        self.buf.resize(FRONT + cap + BACK, 0);
        self.idx.resize(cap + SLACK, 0);
        self.nlp.resize(cap + 8, 0);
        self.bad.resize(cap + 72, 0);
        self.cap = cap;
    }

    /// The current batch of lines starts at `data()`; field positions index into it.
    #[inline(always)]
    pub fn data(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(FRONT) }
    }

    /// Feeds the lines of `f` that start in the byte range `[a, b)` (after the
    /// header line when `a == 0`) to `go`, a batch of whole lines at a time. A line
    /// that starts before `b` is read to its end; one that starts before `a` belongs
    /// to the previous range. A final line without a newline counts.
    pub fn run(&mut self, f: &File, a: u64, b: u64, mut go: impl FnMut(&Reader)) {
        self.feed(f, a, b, true, |rd, _, _| go(rd));
    }

    /// As `run`, without the separator index: `go(data, start, end)` gets each batch
    /// of whole lines as `data[start..end]`, with 64 readable bytes before it and
    /// 192 after it.
    pub fn run_raw(&mut self, f: &File, a: u64, b: u64, mut go: impl FnMut(*const u8, usize, usize)) {
        self.feed(f, a, b, false, |rd, s, n| go(rd.data(), s, n));
    }

    #[inline(always)]
    fn feed(&mut self, f: &File, a: u64, b: u64, index: bool, mut go: impl FnMut(&Reader, usize, usize)) {
        let mut pos = a.saturating_sub(1);
        let mut boff = pos;
        let mut fill = 0usize;
        let mut start = 0usize;
        let mut skipping = true;
        loop {
            if fill == self.cap {
                self.grow();
            }
            let cap = self.cap;
            let got = read_at(f, &mut self.buf[FRONT + fill..FRONT + cap], pos);
            pos += got as u64;
            fill += got;
            let eof = got == 0;
            if skipping {
                // Skip through the first newline: the header, or the end of a line
                // that started in the previous range.
                match memchr(b'\n', &self.buf[FRONT..FRONT + fill]) {
                    Some(q) => {
                        start = q + 1;
                        skipping = false;
                    }
                    None => {
                        boff += fill as u64;
                        fill = 0;
                        if eof || boff >= b {
                            return;
                        }
                        continue;
                    }
                }
            }
            if boff + start as u64 >= b {
                return;
            }
            let mut n = match memrchr(b'\n', &self.buf[FRONT + start..FRONT + fill]) {
                Some(k) => start + k + 1,
                None => start,
            };
            let mut done = false;
            if eof {
                if fill > n {
                    self.buf[FRONT + fill] = b'\n';
                    fill += 1;
                    n = fill;
                }
                done = true;
            } else if n == start {
                continue; // no whole line yet: read more (growing the buffer if full)
            }
            if boff + n as u64 > b {
                // Stop at the first newline at or after file position b - 1.
                let from = (b - 1 - boff) as usize;
                n = from + memchr(b'\n', &self.buf[FRONT + from..FRONT + n]).unwrap() + 1;
                done = true;
            }
            if n > start {
                if index {
                    unsafe { self.index(start, n) };
                }
                go(self, start, n);
            }
            if done {
                return;
            }
            self.buf.copy_within(FRONT + n..FRONT + fill, FRONT);
            boff += n as u64;
            fill -= n;
            start = 0;
        }
    }

    /// Stage 1 over `data[start..n]`, which starts at a line start and ends with a newline.
    #[inline(never)]
    unsafe fn index(&mut self, start: usize, n: usize) {
        let d = self.buf.as_ptr().add(FRONT);
        let idx = self.idx.as_mut_ptr();
        let nlp = self.nlp.as_mut_ptr();
        let (mut ns, mut nn, mut nb) = (0usize, 0usize, 0usize);
        let masks = |o: usize| -> (u64, u64, u64) {
            let (c, nl, q) = classify(d.add(o));
            if n - o >= 64 {
                (c, nl, q)
            } else {
                let v = (1u64 << (n - o)) - 1;
                (c & v, nl & v, q & v)
            }
        };
        let mut o = start;
        let mut cur = masks(o);
        // Carries between blocks: the bit that falls off the top of each mask.
        let (mut s_top, mut q_top, mut cl_top, mut carry) = (1u64, 0u64, 0u64, 0u64);
        loop {
            let next = if o + 64 < n { masks(o + 64) } else { (0, 0, 0) };
            let (c, nl, q) = cur;
            let s = c | nl;
            // A quote may only open a field (right after a separator) or close one
            // (right before a separator), and a field has both or neither.
            let open = q & ((s << 1) | s_top);
            let close = q & ((s >> 1) | ((next.0 | next.1) << 63));
            // Carry each opening quote up to the separator that ends its field.
            let (x, c1) = (!s).overflowing_add(open);
            let (x, c2) = x.overflowing_add(carry);
            carry = (c1 | c2) as u64;
            let ends_open = x & s;
            let after_close = (close << 1) | cl_top;
            let after_quote = (q << 1) | q_top;
            let badm = (q & !(open | close)) | (open & close) | (ends_open & !after_quote) | (after_close & !ends_open);
            s_top = s >> 63;
            q_top = q >> 63;
            cl_top = close >> 63;
            if badm != 0 {
                let mut m = badm;
                while m != 0 {
                    *self.bad.as_mut_ptr().add(nb) = (o as u32) + m.trailing_zeros();
                    nb += 1;
                    m &= m - 1;
                }
            }
            // Flatten separator positions, eight at a time.
            {
                let cnt = s.count_ones() as usize;
                let out = idx.add(ns);
                let mut m = s;
                let base = o as u32;
                for i in 0..8 {
                    *out.add(i) = base + m.trailing_zeros();
                    m &= m.wrapping_sub(1);
                }
                if cnt > 8 {
                    for i in 8..16 {
                        *out.add(i) = base + m.trailing_zeros();
                        m &= m.wrapping_sub(1);
                    }
                    let mut i = 16;
                    while m != 0 {
                        *out.add(i) = base + m.trailing_zeros();
                        m &= m - 1;
                        i += 1;
                    }
                }
                ns += cnt;
            }
            {
                let cnt = nl.count_ones() as usize;
                let out = nlp.add(nn);
                let base = o as u32;
                let mut m = nl;
                *out = base + m.trailing_zeros();
                m &= m.wrapping_sub(1);
                *out.add(1) = base + m.trailing_zeros();
                if cnt > 2 {
                    m &= m - 1;
                    let mut i = 2;
                    while m != 0 {
                        *out.add(i) = base + m.trailing_zeros();
                        m &= m - 1;
                        i += 1;
                    }
                }
                nn += cnt;
            }
            o += 64;
            if o >= n {
                break;
            }
            cur = next;
        }
        // Sentinels: `n` is past every real position and safe to read from.
        for i in 0..SLACK {
            *idx.add(ns + i) = n as u32;
        }
        *self.bad.as_mut_ptr().add(nb) = u32::MAX;
        self.nsep = ns;
        self.nnl = nn;
        self.start = start;
    }

    /// Stage 2: calls `f(ip, ls)` for each line of the batch that has exactly `N`
    /// separators (N - 1 commas and its newline) and well-formed quotes. `ls` is the
    /// line's start and `ip` points at its separator positions; `ip[N..]` are the
    /// following lines' (or `n`, a safe position, past the end).
    #[inline(always)]
    pub unsafe fn lines<const N: usize>(&self, mut f: impl FnMut(*const u32, usize)) {
        let idx = self.idx.as_ptr();
        let nlp = self.nlp.as_ptr();
        let bad = self.bad.as_ptr();
        let mut b = 0usize;
        let mut bi = 0usize;
        let mut ls = self.start;
        for k in 0..self.nnl {
            let le = *nlp.add(k);
            let isbad = *bad.add(bi) <= le;
            if *idx.add(b + N - 1) == le {
                if !isbad {
                    f(idx.add(b), ls);
                }
                b += N;
            } else {
                while *idx.add(b) != le {
                    b += 1;
                }
                b += 1;
            }
            if isbad {
                while *bad.add(bi) <= le {
                    bi += 1;
                }
            }
            ls = le as usize + 1;
        }
    }
}

/// A file mapped read-only, for the per-line decoders: they read straight from the
/// page cache, with no copy into a buffer. Only the lines that start in the zone
/// `[lo, hi)` are read from the mapping: a decoder reads a few bytes before a line
/// and up to 128 after its start, and needs the line's newline, so the first bytes
/// of the file, its last page and anything after its last newline are left to the
/// buffered reader.
pub struct Mapped {
    p: *const u8,
    pub lo: u64,
    pub hi: u64,
}
unsafe impl Send for Mapped {}
unsafe impl Sync for Mapped {}

impl Mapped {
    /// None if the file cannot be mapped or is too small to have a zone.
    pub fn new(f: &File, len: u64) -> Option<Mapped> {
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::unix::io::AsRawFd;
            let len = len as usize;
            if len < 3 * 4096 {
                return None;
            }
            let p = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
            if p == libc::MAP_FAILED {
                return None;
            }
            let p = p as *const u8;
            // The last newline of the file, if its last page has one.
            let last = memrchr(b'\n', std::slice::from_raw_parts(p.add(len - 4096), 4096)).map(|k| len - 4096 + k);
            let hi = last.map_or(len - 4096, |l| l.min(len - 4096)) as u64;
            Some(Mapped { p, lo: 64, hi })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (f, len);
            None
        }
    }

    #[inline(always)]
    pub fn ptr(&self) -> *const u8 {
        self.p
    }

    /// The whole lines that start in `[a, b)` and in the zone, as offsets
    /// `(start, end)`: `start` is a line start, `end` is just past a newline.
    pub fn span(&self, a: u64, b: u64) -> Option<(usize, usize)> {
        let (a, b) = (a.max(self.lo) as usize, b.min(self.hi) as usize);
        if a >= b {
            return None;
        }
        // Every position below `hi` has a newline at or after it (see `new`).
        unsafe {
            let nl = |from: usize| -> usize {
                let mut i = from;
                loop {
                    match memchr(b'\n', std::slice::from_raw_parts(self.p.add(i), 4096)) {
                        Some(k) => return i + k,
                        None => i += 4096,
                    }
                }
            };
            let start = nl(a - 1) + 1;
            if start >= b {
                return None;
            }
            Some((start, nl(b - 1) + 1))
        }
    }

    /// The end of a piece of at most about `max` bytes of whole lines starting at
    /// the line start `x`, within the span ending at `e`.
    pub fn piece(&self, x: usize, e: usize, max: usize) -> usize {
        if e - x <= max {
            return e;
        }
        unsafe {
            let mut i = x + max - 1;
            while *self.p.add(i) != b'\n' {
                i += 1;
            }
            i + 1
        }
    }

    /// Drops the pages that lie wholly inside `[s, e)` from this process's page
    /// tables (the file stays in the page cache): done by each thread for what it
    /// has read, so that nothing is left to tear down at exit.
    pub fn done(&self, s: usize, e: usize) {
        #[cfg(target_os = "linux")]
        unsafe {
            let (s, e) = (s.div_ceil(4096) * 4096, e / 4096 * 4096);
            if e > s {
                libc::madvise(self.p.add(s) as *mut libc::c_void, e - s, libc::MADV_DONTNEED);
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (s, e);
    }
}

/// The field `[s, e)` without its quotes, on a line whose quotes are well-formed.
#[inline(always)]
pub unsafe fn unquote(d: *const u8, s: usize, e: usize) -> (usize, usize) {
    let q = (*d.add(s) == b'"') as usize;
    (s + q, e - q)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The byte-loop splitter applied line by line to everything after the header:
    /// the plain reading of the dialect.
    fn by_lines<const N: usize>(d: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut s = match d.iter().position(|&c| c == b'\n') {
            Some(k) => k + 1,
            None => return out,
        };
        while s < d.len() {
            let e = d[s..].iter().position(|&c| c == b'\n').map_or(d.len(), |k| s + k);
            let mut g: [&[u8]; N] = [&[]; N];
            if crate::split::<N>(&d[s..e], &mut g) {
                out.push(g.iter().map(|x| x.to_vec()).collect());
            }
            s = e + 1;
        }
        out
    }

    /// The same through the reader, with the file cut into byte ranges at `cuts`.
    fn via_reader<const N: usize>(f: &File, len: u64, cuts: &[u64], cap: usize) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut rd = Reader::new(cap);
        let mut a = 0;
        for &b in cuts.iter().chain(std::iter::once(&len)) {
            if b > a {
                rd.run(f, a, b, |rd| unsafe {
                    let d = rd.data();
                    rd.lines::<N>(|ip, ls| {
                        let mut s = ls;
                        let mut row = Vec::new();
                        for j in 0..N {
                            let e = *ip.add(j) as usize;
                            let (fs, fe) = unquote(d, s, e);
                            row.push(std::slice::from_raw_parts(d.add(fs), fe - fs).to_vec());
                            s = e + 1;
                        }
                        out.push(row);
                    });
                });
                a = b;
            }
        }
        out
    }

    #[test]
    fn reader_matches_byte_loop() {
        use std::io::Write;
        let mut x: u64 = 88172645463325252;
        let mut rnd = move |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        let path = std::env::temp_dir().join(format!("ss-scan-test-{}", std::process::id()));
        for round in 0..20000 {
            let mut v = Vec::new();
            // A header line (sometimes missing its newline, sometimes absent).
            match rnd(12) {
                0 => {}
                1 => v.extend_from_slice(b"a,b"),
                _ => v.extend_from_slice(b"a,b,c\n"),
            }
            if round % 2 == 0 {
                // Random bytes heavy in separators and quotes.
                let len = rnd(400) as usize;
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
                let lines = rnd(14);
                let width = [2usize, 4, 7][rnd(3) as usize];
                for _ in 0..lines {
                    let pick = rnd(10);
                    let nf = match pick {
                        0 => rnd(9) as usize,
                        1 => 0,
                        _ => width,
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
            std::fs::File::create(&path).unwrap().write_all(&v).unwrap();
            let f = File::open(&path).unwrap();
            let len = v.len() as u64;
            let mut cuts: Vec<u64> = (0..rnd(5)).map(|_| rnd(len + 1)).collect();
            cuts.sort();
            let cap = [64usize, 128, 4096][rnd(3) as usize];
            let shown = String::from_utf8_lossy(&v).into_owned();
            assert_eq!(via_reader::<2>(&f, len, &cuts, cap), by_lines::<2>(&v), "{shown:?} cuts {cuts:?} cap {cap}");
            assert_eq!(via_reader::<4>(&f, len, &cuts, cap), by_lines::<4>(&v), "{shown:?} cuts {cuts:?} cap {cap}");
            assert_eq!(via_reader::<7>(&f, len, &cuts, cap), by_lines::<7>(&v), "{shown:?} cuts {cuts:?} cap {cap}");
        }
        let _ = std::fs::remove_file(&path);
    }
}
