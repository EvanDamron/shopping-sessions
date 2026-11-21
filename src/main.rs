//! Shopping Sessions pipeline: CSV click-stream in, two daily Parquet tables out.
//! The rules are in rules.md (not checked in); this file implements them directly.

mod parts;
mod pq;
mod scan;
mod ts;

use memchr::memchr;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

const SEC: i64 = 1_000;
const MIN30: i64 = 30 * 60 * SEC;
const DAY: i64 = 86_400 * SEC;
const WEEK: i64 = 7 * DAY;

// ---------------------------------------------------------------------------
// Byte strings that point into memory-mapped input files (leaked, so 'static).

#[derive(Clone, Copy)]
pub struct Str {
    p: *const u8,
    n: u32,
}
unsafe impl Send for Str {}
unsafe impl Sync for Str {}
impl Str {
    #[inline]
    fn new(s: &'static [u8]) -> Str {
        Str { p: s.as_ptr(), n: s.len() as u32 }
    }
    #[inline]
    pub fn b(&self) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.p, self.n as usize) }
    }
}



const HK1: u64 = 0x9E37_79B9_7F4A_7C15;
const HK2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const HK3: u64 = 0x1656_67B1_9E37_79F9;
const HK4: u64 = 0xD6E8_FEB8_6659_FD93;
const HK5: u64 = 0xA076_1D64_78BD_642F;

#[inline(always)]
fn fmix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// One pass over a string: a 64-bit hash (tables, partitions) and an independent
/// 32-bit check; the pair identifies the string.
#[inline(always)]
fn hh(b: &[u8]) -> (u64, u32) {
    let n = b.len();
    let (x, y) = if n <= 16 {
        if n >= 8 {
            (u64::from_le_bytes(b[..8].try_into().unwrap()), u64::from_le_bytes(b[n - 8..].try_into().unwrap()))
        } else if n >= 4 {
            (
                u32::from_le_bytes(b[..4].try_into().unwrap()) as u64,
                u32::from_le_bytes(b[n - 4..].try_into().unwrap()) as u64,
            )
        } else if n > 0 {
            (b[0] as u64 | (b[n / 2] as u64) << 8 | (b[n - 1] as u64) << 16, 0)
        } else {
            (0, 0)
        }
    } else {
        let (mut x, mut y) = (HK4, HK5);
        let mut i = 0;
        while i + 8 <= n {
            let w = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
            x = (x ^ w).wrapping_mul(HK1).rotate_left(29);
            y = (y ^ w).wrapping_mul(HK2).rotate_left(23);
            i += 8;
        }
        let w = u64::from_le_bytes(b[n - 8..].try_into().unwrap());
        (x ^ w.wrapping_mul(HK3), y.wrapping_add(w).wrapping_mul(HK1))
    };
    let nn = n as u64;
    let u = x.wrapping_mul(HK1) ^ y.wrapping_mul(HK2).rotate_left(31) ^ nn.wrapping_mul(HK3);
    let v = (x ^ HK4).wrapping_mul(HK5).wrapping_add((y ^ HK3).wrapping_mul(HK4).rotate_left(17)) ^ nn;
    (fmix(u), (fmix(v) >> 32) as u32)
}

// ---------------------------------------------------------------------------
// CSV dialect

/// Splits one line into exactly N fields. Returns false for a malformed line.
/// A field is either free of quotes or wrapped in exactly one pair of quotes.
#[inline]
fn split<const N: usize>(line: &'static [u8], out: &mut [&'static [u8]; N]) -> bool {
    let mut nf = 0usize;
    let mut fs = 0usize;
    let mut q = 0u32;
    for i in 0..line.len() {
        let c = line[i];
        if c == b',' {
            if nf + 1 >= N {
                return false;
            }
            match unquote(&line[fs..i], q) {
                Some(f) => out[nf] = f,
                None => return false,
            }
            nf += 1;
            fs = i + 1;
            q = 0;
        } else if c == b'"' {
            q += 1;
        }
    }
    if nf + 1 != N {
        return false;
    }
    match unquote(&line[fs..], q) {
        Some(f) => out[nf] = f,
        None => return false,
    }
    true
}

#[inline]
fn unquote(f: &'static [u8], q: u32) -> Option<&'static [u8]> {
    if q == 0 {
        Some(f)
    } else if q == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        Some(&f[1..f.len() - 1])
    } else {
        None
    }
}

/// Calls f for every line of a chunk ('\n' terminated; a final unterminated line counts).
#[inline]
fn for_lines(data: &'static [u8], mut f: impl FnMut(&'static [u8])) {
    let mut s = 0;
    while s < data.len() {
        match memchr(b'\n', &data[s..]) {
            Some(k) => {
                f(&data[s..s + k]);
                s += k + 1;
            }
            None => {
                f(&data[s..]);
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Timestamps

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as i64 * 10 + c as i64)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Parses F1, F2 or F3 into epoch milliseconds. None for anything else.
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n != 20 && n != 25 || b[4] != b'-' {
        return parse_f3(b);
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let dim = match mo {
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
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
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if b[22] != b':' || oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    Some(((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) - off * 60) * 1000)
}

#[inline]
fn parse_f3(b: &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        // Too long for a 64-bit integer: not malformed, but outside every window.
        return Some(i64::MAX);
    }
    Some(v as i64)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning for non-digits is unnecessary: invalid either way
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------------------
// Parallel helpers

fn par<S: Send, F: Fn(&mut S, usize) + Sync>(states: &mut [S], ntasks: usize, f: F) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for st in states.iter_mut() {
            let next = &next;
            let f = &f;
            sc.spawn(move || loop {
                let t = next.fetch_add(1, Relaxed);
                if t >= ntasks {
                    break;
                }
                f(st, t);
            });
        }
    });
}

/// Asks the kernel to fault in a byte range now, in one call (Linux only; no-op elsewhere).
fn populate(d: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        if std::env::var_os("NO_POPULATE").is_some() {
            return;
        }
        let page = 4096usize;
        let start = (d.as_ptr() as usize) & !(page - 1);
        let end = d.as_ptr() as usize + d.len();
        libc::madvise(start as *mut libc::c_void, end - start, 22 /* MADV_POPULATE_READ */);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = d;
}

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> SendPtr<T> {
    #[inline]
    fn get(&self) -> *mut T {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Files and chunks

fn map_file(path: &Path) -> &'static [u8] {
    let f = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::MmapOptions::new().map(&f) }
        .unwrap_or_else(|e| panic!("mmap {}: {e}", path.display()));
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".csv") && !name.starts_with('.') {
                let p = e.path();
                if p.is_file() {
                    v.push(p);
                }
            }
        }
    }
    v.sort();
    v
}

/// Body of a CSV file (after its header line), split into chunks that end at line ends.
fn chunks(files: &[PathBuf], target: usize) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    for p in files {
        let d = map_file(p);
        let body = match memchr(b'\n', d) {
            Some(k) => &d[k + 1..],
            None => continue,
        };
        let mut s = 0;
        while s < body.len() {
            let mut e = (s + target).min(body.len());
            if e < body.len() {
                e = match memchr(b'\n', &body[e..]) {
                    Some(k) => e + k + 1,
                    None => body.len(),
                };
            }
            out.push(&body[s..e]);
            s = e;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Packed string references: pointer in the low 48 bits, length in the high 16.

#[inline]
fn pack(s: &'static [u8]) -> u64 {
    if s.len() < 0xFFFF {
        (s.as_ptr() as u64) | ((s.len() as u64) << 48)
    } else {
        let mut v = Vec::with_capacity(8 + s.len());
        v.extend_from_slice(&(s.len() as u64).to_le_bytes());
        v.extend_from_slice(s);
        let p = Box::leak(v.into_boxed_slice()).as_ptr();
        (p as u64) | (0xFFFF << 48)
    }
}

#[inline]
pub fn unpack(v: u64) -> &'static [u8] {
    let p = (v & ((1u64 << 48) - 1)) as *const u8;
    let n = (v >> 48) as usize;
    unsafe {
        if n < 0xFFFF {
            std::slice::from_raw_parts(p, n)
        } else {
            let n = u64::from_le_bytes(std::slice::from_raw_parts(p, 8).try_into().unwrap()) as usize;
            std::slice::from_raw_parts(p.add(8), n)
        }
    }
}

#[inline(always)]
fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}

// ---------------------------------------------------------------------------
// Linked IDs: interning, concurrent union-find with the smallest name as root.

const LSHARDS: usize = 64;

/// One linked ID in the lookup table: its identity and its person's.
#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct LEntry {
    h: u64,
    ph: u64,
    name: u64, // packed name of the person
    h2: u32,
}

/// Linked IDs: one huge-page region holding every shard's open-addressing table
/// (an entry with name == 0 is empty).
struct Links {
    region: *const LEntry,
    shards: Vec<(usize, u64)>, // per shard: (first slot, mask)
    count: usize,
}
unsafe impl Send for Links {}
unsafe impl Sync for Links {}

#[derive(Clone, Copy)]
struct Person {
    ph: u64,
    name: u64,
}

impl Links {
    #[inline]
    fn prefetch(&self, h: u64) {
        let (off, mask) = self.shards[(h >> 58) as usize];
        prefetch(unsafe { self.region.add(off + (h & mask) as usize) } as *const u8);
    }
    #[inline]
    fn person_h(&self, s: &'static [u8], h: u64, h2: u32) -> Person {
        let (off, mask) = self.shards[(h >> 58) as usize];
        let mut i = (h & mask) as usize;
        loop {
            let e = unsafe { &*self.region.add(off + i) };
            if e.name == 0 {
                break;
            }
            if e.h == h && e.h2 == h2 {
                return Person { ph: e.ph, name: e.name };
            }
            i = (i + 1) & mask as usize;
        }
        Person { ph: h, name: pack(s) }
    }
}

fn build_links(files: &[PathBuf], nthreads: usize) -> Links {
    let ch = chunks(files, 1 << 20);
    struct LS {
        sides: Vec<Vec<(u64, Str, u32)>>, // per shard: (hash, str, edge*2+side)
        edges: Vec<[u32; 2]>,
    }
    let mut st: Vec<LS> = (0..nthreads)
        .map(|_| LS { sides: (0..LSHARDS).map(|_| Vec::new()).collect(), edges: Vec::new() })
        .collect();
    par(&mut st, ch.len(), |s, t| {
        scan::for_records::<2>(ch[t], |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let e = s.edges.len() as u32;
            s.edges.push([0, 0]);
            for side in 0..2 {
                let h = hh(f[side]).0;
                s.sides[(h >> 58) as usize].push((h, Str::new(f[side]), e * 2 + side as u32));
            }
        });
    });
    // Intern per shard.
    struct Shard {
        names: Vec<Str>,
        hashes: Vec<u64>,
        assign: Vec<(u32, u32, u32)>, // (thread, slot, local id)
    }
    let mut shards: Vec<Option<Shard>> = (0..LSHARDS).map(|_| None).collect();
    {
        let shards_ptr = SendPtr(shards.as_mut_ptr());
        let st_ref = &st;
        let mut dummy: Vec<()> = (0..nthreads).map(|_| ()).collect();
        par(&mut dummy, LSHARDS, |_, sh| {
            let total: usize = st_ref.iter().map(|s| s.sides[sh].len()).sum();
            let cap = (total * 2).next_power_of_two().max(16);
            let mask = (cap - 1) as u64;
            let mut slots = vec![0u32; cap];
            let mut names: Vec<Str> = Vec::new();
            let mut hashes: Vec<u64> = Vec::new();
            let mut assign = Vec::with_capacity(total);
            for (ti, s) in st_ref.iter().enumerate() {
                for &(h, name, slot) in &s.sides[sh] {
                    let mut i = (h & mask) as usize;
                    let id = loop {
                        let v = slots[i];
                        if v == 0 {
                            names.push(name);
                            hashes.push(h);
                            slots[i] = names.len() as u32;
                            break names.len() as u32 - 1;
                        }
                        let id = v - 1;
                        if hashes[id as usize] == h && names[id as usize].b() == name.b() {
                            break id;
                        }
                        i = (i + 1) & mask as usize;
                    };
                    assign.push((ti as u32, slot, id));
                }
            }
            unsafe {
                *shards_ptr.get().add(sh) = Some(Shard { names, hashes, assign });
            }
        });
    }
    let shards: Vec<Shard> = shards.into_iter().map(|s| s.unwrap()).collect();
    let mut bases = Vec::with_capacity(LSHARDS);
    let mut total = 0u32;
    for s in &shards {
        bases.push(total);
        total += s.names.len() as u32;
    }
    let mut names = Vec::with_capacity(total as usize);
    for s in &shards {
        names.extend_from_slice(&s.names);
    }
    // Fill edge endpoints with global ids.
    {
        let edge_ptrs: Vec<SendPtr<[u32; 2]>> = st.iter_mut().map(|s| SendPtr(s.edges.as_mut_ptr())).collect();
        let edge_ptrs = &edge_ptrs;
        let shards_ref = &shards;
        let bases_ref = &bases;
        let mut dummy: Vec<()> = (0..nthreads).map(|_| ()).collect();
        par(&mut dummy, LSHARDS, |_, sh| {
            let base = bases_ref[sh];
            for &(ti, slot, id) in &shards_ref[sh].assign {
                unsafe {
                    let e = edge_ptrs[ti as usize].get().add((slot / 2) as usize);
                    (*e)[(slot & 1) as usize] = base + id;
                }
            }
        });
    }
    // Concurrent union-find; the root of every set is its byte-wise smallest name.
    let parent: Vec<AtomicU32> = (0..total).map(AtomicU32::new).collect();
    let less = |a: u32, b: u32| names[a as usize].b() < names[b as usize].b();
    let find = |mut x: u32| -> u32 {
        loop {
            let p = parent[x as usize].load(Relaxed);
            if p == x {
                return x;
            }
            let gp = parent[p as usize].load(Relaxed);
            if gp != p {
                let _ = parent[x as usize].compare_exchange_weak(p, gp, Relaxed, Relaxed);
            }
            x = gp;
        }
    };
    {
        let nchunks = nthreads * 4;
        let all: Vec<&[[u32; 2]]> = st.iter().map(|s| &s.edges[..]).collect();
        let mut dummy: Vec<()> = (0..nthreads).map(|_| ()).collect();
        par(&mut dummy, all.len() * nchunks, |_, t| {
            let edges = all[t / nchunks];
            let k = t % nchunks;
            let lo = edges.len() * k / nchunks;
            let hi = edges.len() * (k + 1) / nchunks;
            for e in &edges[lo..hi] {
                let (mut a, mut b) = (e[0], e[1]);
                loop {
                    a = find(a);
                    b = find(b);
                    if a == b {
                        break;
                    }
                    if less(a, b) {
                        std::mem::swap(&mut a, &mut b);
                    }
                    // a is the larger root: hang it under b.
                    if parent[a as usize].compare_exchange(a, b, Relaxed, Relaxed).is_ok() {
                        break;
                    }
                }
            }
        });
    }
    // Per shard lookup tables holding each linked ID's person, all in one region.
    let mut shard_tabs: Vec<(usize, u64)> = Vec::with_capacity(LSHARDS);
    let mut nslots = 0usize;
    for sh in &shards {
        let cap = (sh.names.len() * 2).next_power_of_two().max(16);
        shard_tabs.push((nslots, (cap - 1) as u64));
        nslots += cap;
    }
    let region = parts::alloc(nslots * std::mem::size_of::<LEntry>()) as *mut LEntry; // zeroed: all empty
    {
        let rp = SendPtr(region);
        let shards_ref = &shards;
        let bases_ref = &bases;
        let names_ref = &names;
        let tabs = &shard_tabs;
        let mut dummy: Vec<()> = (0..nthreads).map(|_| ()).collect();
        par(&mut dummy, LSHARDS, |_, sh| {
            let s = &shards_ref[sh];
            let (off, mask) = tabs[sh];
            for i in 0..s.names.len() {
                let id = bases_ref[sh] + i as u32;
                let r = find(id);
                let rname = names_ref[r as usize].b();
                let me = s.names[i].b();
                let h = s.hashes[i];
                let e = LEntry { h, h2: hh(me).1, ph: hh(rname).0, name: pack(rname) };
                let mut k = (h & mask) as usize;
                unsafe {
                    while (*rp.get().add(off + k)).name != 0 {
                        k = (k + 1) & mask as usize;
                    }
                    rp.get().add(off + k).write(e);
                }
            }
        });
    }
    // Freeing the build structures is pure overhead: the process exits soon anyway.
    std::mem::forget(st);
    std::mem::forget(shards);
    std::mem::forget(names);
    std::mem::forget(parent);
    Links { region, shards: shard_tabs, count: total as usize }
}

// ---------------------------------------------------------------------------
// A small insert-only string map (products, campaigns) with the fast hash.

struct StrMap<V: Copy> {
    slots: Vec<Option<(u64, u32, V)>>,
    mask: usize,
    len: usize,
}

impl<V: Copy> StrMap<V> {
    fn new() -> Self {
        StrMap { slots: vec![None; 16], mask: 15, len: 0 }
    }
    /// Keys are identified by their 96-bit (hh) hash pair.
    #[inline]
    fn get(&self, k: &[u8]) -> Option<V> {
        let (h, h2) = hh(k);
        let mut i = h as usize & self.mask;
        loop {
            match &self.slots[i] {
                None => return None,
                Some((eh, eh2, v)) => {
                    if *eh == h && *eh2 == h2 {
                        return Some(*v);
                    }
                }
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Inserts unless present; returns true if inserted.
    fn insert(&mut self, k: &'static [u8], v: V) -> bool {
        if self.get(k).is_some() {
            return false;
        }
        if (self.len + 1) * 4 > self.slots.len() {
            let n2 = self.slots.len() * 2;
            let old = std::mem::replace(&mut self.slots, vec![None; n2]);
            self.mask = self.slots.len() - 1;
            for (h, h2, v) in old.into_iter().flatten() {
                let mut i = h as usize & self.mask;
                while self.slots[i].is_some() {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = Some((h, h2, v));
            }
        }
        let (h, h2) = hh(k);
        let mut i = h as usize & self.mask;
        while self.slots[i].is_some() {
            i = (i + 1) & self.mask;
        }
        self.slots[i] = Some((h, h2, v));
        self.len += 1;
        true
    }
}

// ---------------------------------------------------------------------------
// Records

const VIEW: u8 = 0;
const CART: u8 = 1;
const PURCHASE: u8 = 2;
const INVALID: u8 = 255;

/// An event, filed under its person. Purchases have price > 0; views and carts 0.
/// `t` is ms since base, with bit 31 set by dedup when another copy wins.
#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    ph: u64,
    name: u64,
    t: u32,
    pc: u32, // price (24 bits) | category << 24
}

impl PEv {
    #[inline(always)]
    fn price(&self) -> u32 {
        self.pc & 0x00FF_FFFF
    }
    #[inline(always)]
    fn cat(&self) -> u16 {
        (self.pc >> 24) as u16
    }
    #[inline(always)]
    fn live(&self) -> bool {
        self.t >> 31 == 0
    }
}

/// An event id copy, filed under its event-id bucket, for dedup.
#[derive(Clone, Copy)]
#[repr(C)]
struct EKey {
    h: u64,
    r: u64, // (thread << 58) | (partition << 48) | index of its PEv; u64::MAX if invalid
    h2: u32,
    ingest: i32, // seconds since base, clamped
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PClk {
    ph: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

pub struct URow {
    pub name: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue: u64,
    pub top: u16, // u16::MAX = null
}

struct Lookups {
    products: StrMap<u16>,
    unknown_cat: u16,
    categories: Vec<Vec<u8>>,
    campaigns: StrMap<u32>,
    channels: Vec<Vec<u8>>,
    campaign_ids: Vec<Vec<u8>>,
}

fn read_lookups(data: &Path) -> Lookups {
    let pd = map_file(&data.join("products.csv"));
    let mut prods: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    if let Some(k) = memchr(b'\n', pd) {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        for_lines(&pd[k + 1..], |l| {
            if split::<2>(l, &mut f) && !f[0].is_empty() {
                prods.push((f[0], f[1]));
            }
        });
    }
    let mut cats: Vec<Vec<u8>> = prods.iter().map(|p| p.1.to_vec()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_idx: HashMap<Vec<u8>, u16> = cats.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let mut products = StrMap::new();
    for p in &prods {
        products.insert(p.0, cat_idx[p.1]);
    }
    let unknown_cat = cat_idx[&b"unknown"[..]];

    let cd = map_file(&data.join("campaigns.csv"));
    let mut campaigns = StrMap::new();
    let mut channels = Vec::new();
    let mut campaign_ids = Vec::new();
    if let Some(k) = memchr(b'\n', cd) {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        for_lines(&cd[k + 1..], |l| {
            if split::<2>(l, &mut f) && !f[0].is_empty() && campaigns.insert(f[0], channels.len() as u32) {
                channels.push(f[1].to_vec());
                campaign_ids.push(f[0].to_vec());
            }
        });
    }
    Lookups { products, unknown_cat, categories: cats, campaigns, channels, campaign_ids }
}

fn read_manifest(data: &Path) -> (i64, i64) {
    let s = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let get = |key: &str| -> i64 {
        let k = s.find(&format!("\"{key}\"")).expect("manifest key");
        let rest = &s[k + key.len() + 2..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        parse_ts(rest[a..b].as_bytes()).expect("manifest timestamp")
    };
    (get("window_start"), get("window_end"))
}

const EB: usize = 256; // event-id buckets
const PB: usize = 512; // person partitions
const EB_SHIFT: u32 = 56;
const PB_SHIFT: u32 = 55;

const BATCH: usize = 16;

#[derive(Clone, Copy)]
struct Pend {
    user: &'static [u8],
    uh: u64,
    uh2: u32,
    eh: u64,
    eh2: u32,
    ing: i32,
    ts: u32,
    price: u32,
    cat: u16,
}
impl Default for Pend {
    fn default() -> Self {
        Pend { user: &[], uh: 0, uh2: 0, eh: 0, eh2: 0, ing: 0, ts: 0, price: 0, cat: 0 }
    }
}

#[derive(Clone, Copy)]
struct CPend {
    user: &'static [u8],
    uh: u64,
    uh2: u32,
    cid: u64,
    ts: u32,
    camp: u32,
}
impl Default for CPend {
    fn default() -> Self {
        CPend { user: &[], uh: 0, uh2: 0, cid: 0, ts: 0, camp: 0 }
    }
}

struct AState {
    pe: parts::Parts<PEv>,
    ek: parts::Parts<EKey>,
    pc: parts::Parts<PClk>,
}

/// Open-addressing map from a person hash to a dense local id; grows as needed.
#[derive(Default)]
struct PTable {
    slots: Vec<(u64, u32)>, // id u32::MAX = empty
    mask: usize,
    len: usize,
}

impl PTable {
    fn reset(&mut self, expect: usize) {
        let cap = (expect * 2).next_power_of_two().max(16);
        self.slots.clear();
        self.slots.resize(cap, (0, u32::MAX));
        self.mask = cap - 1;
        self.len = 0;
    }
    /// Ok(id) if present, else Err(slot to insert at).
    #[inline]
    fn find(&mut self, ph: u64) -> Result<u32, usize> {
        if (self.len + 1) * 2 > self.slots.len() {
            let old = std::mem::take(&mut self.slots);
            let cap = old.len() * 2;
            self.slots.resize(cap, (0, u32::MAX));
            self.mask = cap - 1;
            for (k, v) in old {
                if v != u32::MAX {
                    let mut i = (k as usize) & self.mask;
                    while self.slots[i].1 != u32::MAX {
                        i = (i + 1) & self.mask;
                    }
                    self.slots[i] = (k, v);
                }
            }
        }
        let mut i = (ph as usize) & self.mask;
        loop {
            let (k, v) = self.slots[i];
            if v == u32::MAX {
                return Err(i);
            }
            if k == ph {
                return Ok(v);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn put(&mut self, slot: usize, ph: u64, id: u32) {
        self.slots[slot] = (ph, id);
        self.len += 1;
    }
}

struct CS {
    rows: Vec<URow>,
    camp: Vec<[u64; 4]>, // (camp*ndays + day) -> sessions, purchasing, revenue, assisted
    starts: Vec<i64>,
    days: Vec<u32>,
    tmp: Vec<(u32, u32)>,
    catrev: Vec<(u16, u64)>,
    evs: Vec<PEv>,
    raw: Vec<PEv>,
    cl: Vec<PClk>,
    table: PTable,
    keys: Vec<u64>,
    pid: Vec<u32>,
    cnt: Vec<u32>,
    off: Vec<u32>,
    cpid: Vec<u32>,
    coff: Vec<u32>,
}

struct Ctx {
    ndays: usize,
    dayoff: i64, // day index = (ts + dayoff) / DAY
    organic: u32,
    day0: i64,
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 63);
    let log = |what: &str| eprintln!("[{:7.3}s] {what}", t0.elapsed().as_secs_f64());

    let (ws, we) = read_manifest(&data);
    let lk = read_lookups(&data);
    let event_files = csv_files(&data.join("events"));
    let click_files = csv_files(&data.join("clicks"));
    let link_files = csv_files(&data.join("links"));
    log("lookups");

    // All in-window times are stored as u32 milliseconds since `base`.
    let base = ws - WEEK;
    assert!(we - base < 1i64 << 31, "window too long for 31-bit offsets");
    assert!(lk.categories.len() <= 256, "more than 256 categories");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = lk.channels.len();
    let ctx = Ctx { ndays, dayoff: base - day0 * DAY, organic: ncamp as u32, day0 };
    let base_s = base.div_euclid(1000);

    let links = build_links(&link_files, nthreads);
    log(&format!("links: {} ids", links.count));

    // ---- Phase A: parse events and clicks.
    let ech = chunks(&event_files, 4 << 20);
    let cch = chunks(&click_files, 4 << 20);
    let ebytes: usize = ech.iter().map(|c| c.len()).sum();
    let cbytes: usize = cch.iter().map(|c| c.len()).sum();
    log(&format!("mapped: {} event chunks, {} click chunks", ech.len(), cch.len()));
    // Reserve generously (untouched capacity costs no memory): ~60 bytes per event line, ~40 per click.
    let pe_cap = ebytes / 60 / nthreads / PB + 64;
    let ek_cap = ebytes / 60 / nthreads / EB + 64;
    let pc_cap = cbytes / 40 / nthreads / PB + 64;
    let mut ast: Vec<AState> = (0..nthreads)
        .map(|_| AState {
            pe: parts::Parts::new(PB, pe_cap + pe_cap / 4),
            ek: parts::Parts::new(EB, ek_cap + ek_cap / 4),
            pc: parts::Parts::new(PB, pc_cap + pc_cap / 4),
        })
        .collect();
    let nev = ech.len();
    let wsc = ws - WEEK;
    par_t(&mut ast, nev + cch.len(), |a, tid, t| {
        let tid = tid as u64;
        populate(if t < nev { ech[t] } else { cch[t - nev] });
        if t < nev {
            let mut pend: [Pend; BATCH] = [Pend::default(); BATCH];
            let mut np = 0usize;
            let flush = |a: &mut AState, pend: &[Pend]| {
                for q in pend {
                    let p = links.person_h(q.user, q.uh, q.uh2);
                    let part = (p.ph >> PB_SHIFT) as usize;
                    let i = a.pe.push(part, PEv { ph: p.ph, name: p.name, t: q.ts, pc: q.price | ((q.cat as u32) << 24) });
                    let r = (tid << 58) | ((part as u64) << 48) | i as u64;
                    a.ek.push((q.eh >> EB_SHIFT) as usize, EKey { h: q.eh, r, h2: q.eh2, ingest: q.ing });
                }
            };
            scan::for_records::<7>(ech[t], |f| {
                if f[0].is_empty() {
                    return;
                }
                let ingest = ts::parse_ingest(f[3]);
                let et = f[4];
                let mut kind = if et.len() == 4 {
                    match u32::from_le_bytes(et.try_into().unwrap()) {
                        0x7765_6976 => VIEW, // "view"
                        0x7472_6163 => CART, // "cart"
                        _ => INVALID,
                    }
                } else if et.len() == 8 && u64::from_le_bytes(et.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
                    PURCHASE
                } else {
                    INVALID
                };
                let mut ts = 0i64;
                let mut price = 0u32;
                let mut cat = 0u16;
                if kind != INVALID && (f[1].is_empty() || f[5].is_empty() || ingest.is_none()) {
                    kind = INVALID;
                }
                if kind != INVALID {
                    match ts::parse_ts(f[2]) {
                        Some(t) if t >= ws && t < we && ingest.unwrap() - t <= DAY => ts = t,
                        _ => kind = INVALID,
                    }
                }
                if kind == PURCHASE {
                    match parse_price(f[6]) {
                        Some(p) => {
                            price = p;
                            cat = lk.products.get(f[5]).unwrap_or(lk.unknown_cat);
                        }
                        None => kind = INVALID,
                    }
                }
                let (eh, eh2) = hh(f[0]);
                let ing = match ingest {
                    Some(i) => (i.div_euclid(1000) - base_s).clamp(i32::MIN as i64 + 1, i32::MAX as i64) as i32,
                    None => i32::MAX,
                };
                if kind == INVALID {
                    a.ek.push((eh >> EB_SHIFT) as usize, EKey { h: eh, r: u64::MAX, h2: eh2, ingest: ing });
                    return;
                }
                let (uh, uh2) = hh(f[1]);
                links.prefetch(uh);
                // Ring: resolve the entry pushed BATCH lines ago, whose prefetch has landed.
                let k = np & (BATCH - 1);
                if np >= BATCH {
                    flush(a, &pend[k..k + 1]);
                }
                pend[k] = Pend { user: f[1], uh, uh2, eh, eh2, ing, ts: (ts - base) as u32, price, cat };
                np += 1;
            });
            for i in np.saturating_sub(BATCH)..np {
                let k = i & (BATCH - 1);
                flush(a, &pend[k..k + 1]);
            }
        } else {
            let mut pend: [CPend; BATCH] = [CPend::default(); BATCH];
            let mut np = 0usize;
            let flush = |a: &mut AState, pend: &[CPend]| {
                for q in pend {
                    let p = links.person_h(q.user, q.uh, q.uh2);
                    a.pc.push((p.ph >> PB_SHIFT) as usize, PClk { ph: p.ph, cid: q.cid, ts: q.ts, camp: q.camp });
                }
            };
            scan::for_records::<4>(cch[t - nev], |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let camp = match lk.campaigns.get(f[2]) {
                    Some(c) => c,
                    None => return,
                };
                let ts = match ts::parse_ts(f[3]) {
                    Some(t) if t >= wsc && t < we => t,
                    _ => return,
                };
                let (uh, uh2) = hh(f[1]);
                links.prefetch(uh);
                let k = np & (BATCH - 1);
                if np >= BATCH {
                    flush(a, &pend[k..k + 1]);
                }
                pend[k] = CPend { user: f[1], uh, uh2, cid: pack(f[0]), ts: (ts - base) as u32, camp };
                np += 1;
            });
            for i in np.saturating_sub(BATCH)..np {
                let k = i & (BATCH - 1);
                flush(a, &pend[k..k + 1]);
            }
        }
    });
    log("phase A parsed");
    rest(ast, links, lk, ctx, ws, base, ncamp, day0, nthreads, &out, t0);
}

fn par_t<S: Send, F: Fn(&mut S, usize, usize) + Sync>(states: &mut [S], ntasks: usize, f: F) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for (tid, st) in states.iter_mut().enumerate() {
            let next = &next;
            let f = &f;
            sc.spawn(move || loop {
                let t = next.fetch_add(1, Relaxed);
                if t >= ntasks {
                    break;
                }
                f(st, tid, t);
            });
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn rest(
    ast: Vec<AState>,
    links: Links,
    lk: Lookups,
    ctx: Ctx,
    _ws: i64,
    _base: i64,
    ncamp: usize,
    day0: i64,
    nthreads: usize,
    out: &Path,
    t0: Instant,
) {
    let log = |what: &str| eprintln!("[{:7.3}s] {what}", t0.elapsed().as_secs_f64());
    drop(links);

    // ---- Phase B: dedup on event id. Losing copies get `del` set in their PEv.
    {
        let ek: Vec<&parts::Parts<EKey>> = ast.iter().map(|a| &a.ek).collect();
        let ek = &ek;
        let pes: Vec<&parts::Parts<PEv>> = ast.iter().map(|a| &a.pe).collect();
        let pes = &pes;
        let kill = |r: u64| {
            if r != u64::MAX {
                let t = (r >> 58) as usize;
                let p = ((r >> 48) & 0x3ff) as usize;
                let i = (r & 0xffff_ffff_ffff) as usize;
                // Disjoint single-field writes after all pushes are done.
                unsafe { (*pes[t].ptr(p, i)).t |= 1 << 31 };
            }
        };
        let mut dummy: Vec<Vec<u64>> = (0..nthreads).map(|_| Vec::new()).collect();
        par(&mut dummy, EB, |slots, b| {
            let total: usize = ek.iter().map(|t| t.len(b)).sum();
            if total == 0 {
                return;
            }
            let cap = (total * 2).next_power_of_two();
            let mask = cap - 1;
            slots.clear();
            slots.resize(cap, 0);
            // Keys of this bucket, flattened by (thread, index) -> u32 code.
            let mut offs = [0u32; 64];
            let mut acc = 0u32;
            for (ti, t) in ek.iter().enumerate() {
                offs[ti] = acc;
                acc += t.len(b) as u32;
            }
            let get = |code: u32| -> &EKey {
                let mut ti = 0;
                while ti + 1 < ek.len() && offs[ti + 1] <= code {
                    ti += 1;
                }
                unsafe { &*ek[ti].ptr(b, (code - offs[ti]) as usize) }
            };
            // Slot = (high 32 bits of the hash) << 32 | (code + 1); 0 = empty.
            for (ti, t) in ek.iter().enumerate() {
                let (s1, s2) = t.slices(b);
                for (i, e) in s1.iter().chain(s2.iter()).enumerate() {
                    let code = offs[ti] + i as u32;
                    let tag = e.h & 0xFFFF_FFFF_0000_0000;
                    let mine = tag | (code as u64 + 1);
                    let mut s = (e.h as usize) & mask;
                    loop {
                        let v = slots[s];
                        if v == 0 {
                            slots[s] = mine;
                            break;
                        }
                        if v & 0xFFFF_FFFF_0000_0000 == tag {
                            let o = get((v & 0xFFFF_FFFF) as u32 - 1);
                            if o.h == e.h && o.h2 == e.h2 {
                                if e.ingest < o.ingest {
                                    kill(o.r);
                                    slots[s] = mine;
                                } else {
                                    kill(e.r);
                                }
                                break;
                            }
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
        });
    }
    log("phase B dedup");

    // ---- Phase C: sessions, attribution, aggregation per person partition.
    let mut cs: Vec<CS> = (0..nthreads)
        .map(|_| CS {
            rows: Vec::new(),
            camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays],
            starts: Vec::new(),
            days: Vec::new(),
            tmp: Vec::new(),
            catrev: Vec::new(),
            evs: Vec::new(),
            raw: Vec::new(),
            cl: Vec::new(),
            table: PTable::default(),
            keys: Vec::new(),
            pid: Vec::new(),
            cnt: Vec::new(),
            off: Vec::new(),
            cpid: Vec::new(),
            coff: Vec::new(),
        })
        .collect();
    {
        let ast = &ast;
        let ctx = &ctx;
        // Largest partitions first, so a whale's partition never starts last.
        let mut order: Vec<usize> = (0..PB).collect();
        order.sort_by_key(|&p| std::cmp::Reverse(ast.iter().map(|a| a.pe.len(p) + a.pc.len(p)).sum::<usize>()));
        let order = &order;
        par(&mut cs, PB, |st, p| {
            let p = order[p];
            // Group by person with a local table, then sort each person's rows by time.
            let n_ev: usize = ast.iter().map(|a| a.pe.len(p)).sum();
            let mut table = std::mem::take(&mut st.table);
            table.reset((n_ev / 8).max(64));
            let mut keys = std::mem::take(&mut st.keys); // local person -> ph
            keys.clear();
            let mut pid = std::mem::take(&mut st.pid);
            pid.clear();
            let mut cnt = std::mem::take(&mut st.cnt);
            cnt.clear();
            let lookup = |table: &mut PTable, keys: &mut Vec<u64>, cnt: &mut Vec<u32>, ph: u64, insert: bool| -> u32 {
                match table.find(ph) {
                    Ok(v) => v,
                    Err(slot) => {
                        if !insert {
                            return u32::MAX;
                        }
                        let id = keys.len() as u32;
                        keys.push(ph);
                        cnt.push(0);
                        table.put(slot, ph, id);
                        id
                    }
                }
            };
            let mut raw = std::mem::take(&mut st.raw);
            raw.clear();
            for a in ast.iter() {
                let (s1, s2) = a.pe.slices(p);
                for e in s1.iter().chain(s2.iter()) {
                    if e.live() {
                        let id = lookup(&mut table, &mut keys, &mut cnt, e.ph, true);
                        cnt[id as usize] += 1;
                        pid.push(id);
                        raw.push(*e);
                    }
                }
            }
            let np = keys.len();
            // Event offsets per person.
            let mut off = std::mem::take(&mut st.off);
            off.clear();
            off.resize(np + 1, 0);
            for i in 0..np {
                off[i + 1] = off[i] + cnt[i];
            }
            let total = off[np] as usize;
            let mut evs = std::mem::take(&mut st.evs);
            evs.clear();
            evs.reserve(total);
            unsafe { evs.set_len(total) };
            {
                let mut pos: Vec<u32> = off[..np].to_vec();
                for (e, &id) in raw.iter().zip(pid.iter()) {
                    evs[pos[id as usize] as usize] = *e;
                    pos[id as usize] += 1;
                }
            }
            // Clicks of these persons, grouped the same way.
            let mut cpid = std::mem::take(&mut st.cpid);
            cpid.clear();
            for c in cnt.iter_mut() {
                *c = 0;
            }
            for a in ast.iter() {
                let (s1, s2) = a.pc.slices(p);
                for c in s1.iter().chain(s2.iter()) {
                    let id = lookup(&mut table, &mut keys, &mut cnt, c.ph, false);
                    if id != u32::MAX {
                        cnt[id as usize] += 1;
                    }
                    cpid.push(id);
                }
            }
            let mut coff = std::mem::take(&mut st.coff);
            coff.clear();
            coff.resize(np + 1, 0);
            for i in 0..np {
                coff[i + 1] = coff[i] + cnt[i];
            }
            let ctotal = coff[np] as usize;
            let mut cl = std::mem::take(&mut st.cl);
            cl.clear();
            cl.reserve(ctotal);
            unsafe { cl.set_len(ctotal) };
            {
                let mut pos: Vec<u32> = coff[..np].to_vec();
                let mut k = 0;
                for a in ast.iter() {
                    let (s1, s2) = a.pc.slices(p);
                for c in s1.iter().chain(s2.iter()) {
                        let id = cpid[k];
                        k += 1;
                        if id != u32::MAX {
                            cl[pos[id as usize] as usize] = *c;
                            pos[id as usize] += 1;
                        }
                    }
                }
            }
            for i in 0..np {
                let ev = &mut evs[off[i] as usize..off[i + 1] as usize];
                if ev.len() > 1 {
                    ev.sort_unstable_by_key(|e| e.t);
                }
                let c = &mut cl[coff[i] as usize..coff[i + 1] as usize];
                if c.len() > 1 {
                    c.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| unpack(a.cid).cmp(unpack(b.cid))));
                }
                person(st, &evs[off[i] as usize..off[i + 1] as usize], &cl[coff[i] as usize..coff[i + 1] as usize], ctx);
            }
            st.evs = evs;
            st.raw = raw;
            st.cl = cl;
            st.table = table;
            st.keys = keys;
            st.pid = pid;
            st.cnt = cnt;
            st.off = off;
            st.cpid = cpid;
            st.coff = coff;
        });
    }
    log("phase C sessions");
    std::mem::forget(ast);

    // ---- Output
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let ndays = ctx.ndays;
    let mut camp_tot = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in &cs {
        for (a, b) in camp_tot.iter_mut().zip(s.camp.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    std::thread::scope(|sc| {
        for (t, s) in cs.iter().enumerate() {
            let ud = &ud;
            let cats = &lk.categories;
            sc.spawn(move || {
                let rows = &s.rows;
                let cols = [
                    pq::Col::Str(rows.iter().map(|r| unpack(r.name)).collect()),
                    pq::Col::Date(rows.iter().map(|r| r.day).collect()),
                    pq::Col::I64(rows.iter().map(|r| r.sessions as i64).collect()),
                    pq::Col::I64(rows.iter().map(|r| r.events as i64).collect()),
                    pq::Col::I64(rows.iter().map(|r| r.revenue as i64).collect()),
                    pq::Col::OptStr(
                        rows.iter().map(|r| (r.top != u16::MAX).then(|| &cats[r.top as usize][..])).collect(),
                    ),
                ];
                pq::write(
                    &ud.join(format!("part-{t:05}.parquet")),
                    &["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
                    &cols,
                )
                .expect("write user_daily");
            });
        }
        let mut crow = Vec::new();
        for c in 0..=ncamp {
            for d in 0..ndays {
                let v = camp_tot[c * ndays + d];
                if v[0] > 0 || v[3] > 0 {
                    let (id, ch): (&[u8], &[u8]) =
                        if c == ncamp { (b"organic", b"organic") } else { (&lk.campaign_ids[c], &lk.channels[c]) };
                    crow.push((id, ch, (day0 + d as i64) as i32, v));
                }
            }
        }
        let cols = [
            pq::Col::Str(crow.iter().map(|r| r.0).collect()),
            pq::Col::Str(crow.iter().map(|r| r.1).collect()),
            pq::Col::Date(crow.iter().map(|r| r.2).collect()),
            pq::Col::I64(crow.iter().map(|r| r.3[0] as i64).collect()),
            pq::Col::I64(crow.iter().map(|r| r.3[1] as i64).collect()),
            pq::Col::I64(crow.iter().map(|r| r.3[2] as i64).collect()),
            pq::Col::I64(crow.iter().map(|r| r.3[3] as i64).collect()),
        ];
        pq::write(
            &cd.join("part-00000.parquet"),
            &["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"],
            &cols,
        )
        .expect("write campaign_daily");
    });
    let nrows: usize = cs.iter().map(|s| s.rows.len()).sum();
    log(&format!("wrote {nrows} user rows"));
    // Outputs are written and closed; skip freeing gigabytes of buffers.
    std::process::exit(0);
}

#[inline]
fn person(st: &mut CS, ev: &[PEv], cl: &[PClk], ctx: &Ctx) {
    let ndays = ctx.ndays;
    st.starts.clear();
    st.days.clear();
    let name = ev[0].name;
    let mut k = 0;
    let mut cj = 0usize;
    let mut grp = 0usize;
    let mut cur_day: i64 = -1;
    let (mut us, mut ue, mut ur) = (0u32, 0u32, 0u64);
    st.catrev.clear();
    while k < ev.len() {
        let start = ev[k].t as i64;
        let day = (start + ctx.dayoff).div_euclid(DAY);
        if day != cur_day {
            if cur_day >= 0 {
                flush(st, name, cur_day + ctx.day0, us, ue, ur);
            }
            cur_day = day;
            us = 0;
            ue = 0;
            ur = 0;
        }
        let mut last = start;
        let mut n = 0u32;
        let mut rev = 0u64;
        let mut purch = false;
        while k < ev.len() && ev[k].t as i64 - last <= MIN30 {
            let e = &ev[k];
            last = e.t as i64;
            n += 1;
            let price = e.price();
            if price > 0 {
                let cat = e.cat();
                rev += price as u64;
                purch = true;
                match st.catrev.iter_mut().find(|c| c.0 == cat) {
                    Some(c) => c.1 += price as u64,
                    None => st.catrev.push((cat, price as u64)),
                }
            }
            k += 1;
        }
        us += 1;
        ue += n;
        ur += rev;
        // Credit: latest click at or before start, within 7 days; ties -> smallest click_id.
        while cj < cl.len() && (cl[cj].ts as i64) <= start {
            if cj == 0 || cl[cj].ts != cl[cj - 1].ts {
                grp = cj;
            }
            cj += 1;
        }
        let camp = if cj > 0 && start - cl[cj - 1].ts as i64 <= WEEK { cl[grp].camp } else { ctx.organic };
        let c = &mut st.camp[camp as usize * ndays + day as usize];
        c[0] += 1;
        c[1] += purch as u64;
        c[2] += rev;
        if !cl.is_empty() {
            st.starts.push(start);
            st.days.push(day as u32);
        }
    }
    flush(st, name, cur_day + ctx.day0, us, ue, ur);

    // Assists: per campaign, merge [t, t + 7d] intervals; count sessions inside per day.
    if cl.is_empty() {
        return;
    }
    let mut tmp = std::mem::take(&mut st.tmp);
    tmp.clear();
    tmp.extend(cl.iter().map(|c| (c.camp, c.ts)));
    tmp.sort_unstable();
    let mut a = 0;
    while a < tmp.len() {
        let camp = tmp[a].0;
        let mut lo = tmp[a].1 as i64;
        let mut hi = lo + WEEK;
        a += 1;
        loop {
            let more = a < tmp.len() && tmp[a].0 == camp;
            if more && (tmp[a].1 as i64) <= hi {
                hi = tmp[a].1 as i64 + WEEK;
                a += 1;
                continue;
            }
            let s = &st.starts;
            let mut i0 = s.partition_point(|&x| x < lo);
            let i1 = s.partition_point(|&x| x <= hi);
            while i0 < i1 {
                let d = st.days[i0];
                let j = i0 + st.days[i0..i1].partition_point(|&x| x <= d);
                st.camp[camp as usize * ndays + d as usize][3] += (j - i0) as u64;
                i0 = j;
            }
            if more {
                lo = tmp[a].1 as i64;
                hi = lo + WEEK;
                a += 1;
            } else {
                break;
            }
        }
    }
    st.tmp = tmp;
}

#[inline]
fn flush(st: &mut CS, name: u64, day: i64, us: u32, ue: u32, ur: u64) {
    let mut top = u16::MAX;
    let mut best = 0u64;
    for &(c, r) in &st.catrev {
        if r > best || (r == best && c < top) {
            best = r;
            top = c;
        }
    }
    st.catrev.clear();
    st.rows.push(URow { name, day: day as i32, sessions: us, events: ue, revenue: ur, top });
}
