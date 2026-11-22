//! Shopping Sessions pipeline: CSV click-stream in, two daily Parquet tables out.
//! The rules are in rules.md (not checked in); this file implements them directly.
//!
//! Shape of the run (every phase on all threads):
//!   links    link files -> table from each linked ID to its person's name
//!   phase A  event and click files, read in cache-sized pieces -> per valid event a
//!            24-byte record filed under its person, per event line a 16-byte dedup
//!            key filed under its event ID, per valid click a record under its person
//!   phase B  per event-ID bucket: the copy with the smallest ingest_ts wins, the
//!            others are marked dead in their person's records
//!   phase C  per person bucket: group, sort by time, sessionize, credit, assist,
//!            and encode the user rows straight into Parquet pages

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
mod dec;
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
mod decc;
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
mod dec5;
mod fast;
mod id;
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
mod kern;
mod links;
mod mem;
mod post;
mod pq;
mod scan;
#[cfg(test)]
mod simple;
mod stage;
mod ts;
mod tsfast;
mod tl;

#[cfg(test)]
use simple::parse_ts;

use id::Id;
use links::Links;
use memchr::memchr;
use scan::{unquote, Reader};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;
use tsfast::Win;

const SEC: i64 = 1_000;
const MIN30: i64 = 30 * 60 * SEC;
const DAY: i64 = 86_400 * SEC;
const WEEK: i64 = 7 * DAY;

// ---------------------------------------------------------------------------
// The plain reading of the CSV dialect, for the two small lookup files (and as
// the reference the scanner is tested against).

/// Splits one line into exactly N fields. Returns false for a malformed line.
/// A field is either free of quotes or wrapped in exactly one pair of quotes.
#[inline]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut nf = 0usize;
    let mut fs = 0usize;
    let mut q = 0u32;
    for i in 0..line.len() {
        let c = line[i];
        if c == b',' {
            if nf + 1 >= N {
                return false;
            }
            match unquote_field(&line[fs..i], q) {
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
    match unquote_field(&line[fs..], q) {
        Some(f) => out[nf] = f,
        None => return false,
    }
    true
}

#[inline]
fn unquote_field(f: &[u8], q: u32) -> Option<&[u8]> {
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
fn for_lines<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
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
// Small ID maps (products, campaigns, and persons within a bucket).

struct IdMap {
    slots: Vec<(Id, u32)>, // Id::EMPTY = free
    mask: usize,
    len: usize,
}

impl IdMap {
    fn new() -> IdMap {
        IdMap::with_capacity(0)
    }
    /// Room for `n` keys without growing (the size growing to `n` keys would reach).
    fn with_capacity(n: usize) -> IdMap {
        let cap = ((n + 1) * 4).next_power_of_two().max(16);
        IdMap { slots: vec![(Id::EMPTY, 0); cap], mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    fn get(&self, k: Id) -> Option<u32> {
        let mut i = k.hash() as usize & self.mask;
        loop {
            let (s, v) = unsafe { *self.slots.get_unchecked(i) };
            // (A free slot first: the empty ID is never a key, so looking it up finds nothing.)
            if s == Id::EMPTY {
                return None;
            }
            if s == k {
                return Some(v);
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Every (key, value) of the map.
    #[allow(dead_code)]
    fn entries(&self) -> impl Iterator<Item = (Id, u32)> + '_ {
        self.slots.iter().copied().filter(|e| e.0 != Id::EMPTY)
    }
    /// Inserts unless present; returns true if inserted. The empty ID is never a key.
    fn insert(&mut self, k: Id, v: u32) -> bool {
        if k == Id::EMPTY || self.get(k).is_some() {
            return false;
        }
        if (self.len + 1) * 4 > self.slots.len() {
            let n2 = self.slots.len() * 2;
            let old = std::mem::replace(&mut self.slots, vec![(Id::EMPTY, 0); n2]);
            self.mask = n2 - 1;
            for (s, v) in old {
                if s != Id::EMPTY {
                    self.put(s, v);
                }
            }
        }
        self.put(k, v);
        self.len += 1;
        true
    }
    fn put(&mut self, k: Id, v: u32) {
        let mut i = k.hash() as usize & self.mask;
        while self.slots[i].0 != Id::EMPTY {
            i = (i + 1) & self.mask;
        }
        self.slots[i] = (k, v);
    }
}

struct Lookups {
    products: IdMap, // product -> category index
    unknown_cat: u32,
    categories: Vec<Vec<u8>>,
    campaigns: IdMap, // campaign -> index
    channels: Vec<Vec<u8>>,
    campaign_ids: Vec<Vec<u8>>,
}

fn read_lookups(data: &Path) -> Lookups {
    let pd = std::fs::read(data.join("products.csv")).unwrap_or_default();
    let mut prods: Vec<(&[u8], &[u8])> = Vec::new();
    if let Some(k) = memchr(b'\n', &pd) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        for_lines(&pd[k + 1..], |l| {
            if split::<2>(l, &mut f) && !f[0].is_empty() {
                prods.push((f[0], f[1]));
            }
        });
    }
    // The distinct categories first (a few dozen), then each product's index among
    // them, sorted by name: sorting and searching all 50,000 product rows' copies took
    // about 20 ms, on the side thread that runs while the links are built.
    let mut index: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    for p in &prods {
        index.entry(p.1).or_insert(0);
    }
    index.entry(b"unknown").or_insert(0);
    let mut cats: Vec<Vec<u8>> = index.keys().map(|c| c.to_vec()).collect();
    cats.sort();
    for (k, c) in cats.iter().enumerate() {
        *index.get_mut(&c[..]).unwrap() = k as u32;
    }
    let mut products = IdMap::with_capacity(prods.len());
    for p in &prods {
        products.insert(Id::intern(p.0), index[p.1]);
    }
    let unknown_cat = cats.binary_search_by(|c| c[..].cmp(b"unknown")).unwrap() as u32;

    let cd = std::fs::read(data.join("campaigns.csv")).unwrap_or_default();
    let mut campaigns = IdMap::new();
    let mut channels = Vec::new();
    let mut campaign_ids = Vec::new();
    if let Some(k) = memchr(b'\n', &cd) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        for_lines(&cd[k + 1..], |l| {
            if split::<2>(l, &mut f) && !f[0].is_empty() && campaigns.insert(Id::intern(f[0]), channels.len() as u32) {
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
        ts::parse_ts(rest[a..b].as_bytes()).expect("manifest timestamp")
    };
    (get("window_start"), get("window_end"))
}

/// The `*.csv` files of a folder, opened, with their sizes; empty ones are left out.
fn csv_files(dir: &Path) -> Vec<(File, u64)> {
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
    v.into_iter()
        .filter_map(|p| {
            let f = File::open(&p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            (len > 0).then_some((f, len))
        })
        .collect()
}

/// Byte ranges of about `target` bytes covering every file: (file, start, end).
fn ranges(files: &[(File, u64)], target: u64) -> Vec<(usize, u64, u64)> {
    let mut out = Vec::new();
    for (i, (_, len)) in files.iter().enumerate() {
        let n = len.div_ceil(target).max(1);
        for k in 0..n {
            out.push((i, len * k / n, len * (k + 1) / n));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Records

/// A valid event, filed under its person. `who` is the person's number (links.rs).
/// `t` is ms since the click window's start, with bit 31 set by dedup when another
/// copy of the event wins. `pc` is the price (24 bits, 0 unless a purchase) and the
/// category (high 8 bits).
#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    who: u32,
    t: u32,
    pc: u32,
}

/// A valid click, filed under its person: its click_id is in the filing thread's
/// `Worker::cids` at `cref` (read only to break a same-instant tie).
#[derive(Clone, Copy)]
#[repr(C)]
struct KRec {
    who: u32,
    ts: u32,
    camp: u32,
    cref: u32,
}

/// A valid click as the index-order fast path files it, under its user number
/// (`who`, with the alias in `_pad`); also the general click line's decision.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    who: u32,
    ts: u32,
    camp: u32,
    _pad: u32,
    cid: Id,
}

/// A click of the person being processed.
#[derive(Clone, Copy)]
struct Clk {
    ts: u32,
    camp: u32,
    cid: Id,
}

const DEAD: u32 = 1 << 31;
const NOSLOT: u32 = u32::MAX;
const PCH: usize = 1024; // event records per chunk
const DCH: usize = 1024; // dedup keys per chunk
const CCH: usize = 256; // click records per chunk
const PREC: usize = std::mem::size_of::<PRec>();
const CREC: usize = std::mem::size_of::<CRec>();
const KREC: usize = std::mem::size_of::<KRec>();

/// Write-combining buffers: the last few records of a bucket gather here, in cache,
/// and go to the bucket's memory whole cache lines at a time with streaming stores.
/// Scattering records straight into hundreds of buckets would stall on a
/// read-for-ownership miss for nearly every line written.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct WcK([u64; 8]); // 4 dedup keys
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct WcE([u32; 48]); // 16 event records
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct WcC([u64; 32]); // 8 fast-path click records, or 16 person click records

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
unsafe impl Sync for dec::Dec {}
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
unsafe impl Send for dec::Dec {}

/// What phases A to C share.
struct Shared<'a> {
    links: Links,
    lk: &'a Lookups,
    win: Win,
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
    kern: kern::Kern, // the line kernels' tables (kern.rs)
    evspan: u64, // window_end - window_start
    pbits: u32, // person bucket = person number & pmask, place in it = number >> pbits
    pmask: u32,
    dshift: u32,
    ev: mem::Arena, // event records, addressed by slot
    dk: mem::Arena, // dedup keys
    ck: mem::Arena, // click records
    fx: Option<fast::Fx>, // the index-order fast path (fast.rs), when the data's IDs allow it
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
    dec: Option<Box<dec::Dec>>, // the per-line decoder's tables (dec.rs), with the fast path
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
    decc: Option<Box<decc::DecC>>, // the click-line decoder's own tables (decc.rs), with `dec`
    deccheck: bool, // test hook: every line the decoder takes is also read the plain way and compared
    dec5: bool,     // the per-line decoders for opaque IDs (dec5.rs), without the fast path
    kern_on: bool,  // SS_KERN=1: event lines through the line kernel (dec5::events_k) instead of the two passes (measured slower; kept for reference)
}

impl Shared<'_> {
    /// Places per person bucket: every person number is below `places() << pbits`.
    fn places(&self) -> usize {
        ((self.links.persons() as usize) >> self.pbits) + 1
    }
}

/// Per bucket: records so far, and where the current chunk starts.
#[derive(Clone, Copy, Default)]
struct Fill {
    n: u32,
    cur: u32,
}

/// One thread's phase A output: chunk lists per bucket.
struct Worker {
    pfill: Vec<Fill>,       // per person bucket: events; `cur` is the chunk's first slot
    pwc: Vec<WcE>,
    pchunks: Vec<Vec<u32>>, // per person bucket: first slot of each chunk
    dcnt: Vec<u32>,         // per key bucket: keys so far
    dwc: Vec<WcK>,
    dcur: Vec<usize>, // per key bucket: current chunk
    dchunks: Vec<Vec<usize>>,
    ccnt: Vec<u32>, // per person bucket: clicks so far
    cwc: Vec<WcC>,
    ccur: Vec<usize>,
    cchunks: Vec<Vec<usize>>,
    cids: Vec<Id>, // click_ids of the clicks this thread filed, by `KRec::cref`
    added: u32, // user IDs this thread has added to the table and not yet reported
    // Fast path (fast.rs): events by event-number bucket, clicks by user-number bucket.
    xcnt: Vec<u32>,
    xwc: Vec<fast::WcX>,
    xcur: Vec<usize>,
    xchunks: Vec<Vec<usize>>,
    xccnt: Vec<u32>,
    xcwc: Vec<WcC>,
    xccur: Vec<usize>,
    xcchunks: Vec<Vec<usize>>,
    xdone: bool,    // the fast path's buffers have been written out
    odd: Vec<fast::OddEv>, // event lines whose event ID has no number
    nodd: u64,      // lines whose IDs took the general path
    ncold: u64,     // event lines the per-line decoder handed to the plain reading
    nccold: u64,    // click lines the click-line decoder handed to the plain reading
    nfast: u64,     // event lines the v5 decoder (dec5.rs) took on its fast path
    ncfast: u64,    // click lines it took on its fast path
}

impl Worker {
    fn new(pb: usize, db: usize, nb: usize, nxc: usize, ncid: usize) -> Worker {
        Worker {
            pfill: vec![Fill::default(); pb],
            pwc: vec![WcE([0; 48]); pb],
            pchunks: (0..pb).map(|_| Vec::new()).collect(),
            dcnt: vec![0; db],
            dwc: vec![WcK([0; 8]); db],
            dcur: vec![0; db],
            dchunks: (0..db).map(|_| Vec::new()).collect(),
            ccnt: vec![0; pb],
            cwc: vec![WcC([0; 32]); pb],
            ccur: vec![0; pb],
            cchunks: (0..pb).map(|_| Vec::new()).collect(),
            cids: Vec::with_capacity(ncid),
            added: 0,
            xcnt: vec![0; nb],
            xwc: vec![fast::WcX([0; 16]); nb],
            xcur: vec![0; nb],
            xchunks: (0..nb).map(|_| Vec::new()).collect(),
            xccnt: vec![0; nxc],
            xcwc: vec![WcC([0; 32]); nxc],
            xccur: vec![0; nxc],
            xcchunks: (0..nxc).map(|_| Vec::new()).collect(),
            xdone: false,
            odd: Vec::new(),
            nodd: 0,
            ncold: 0,
            nccold: 0,
            nfast: 0,
            ncfast: 0,
        }
    }

    /// Files an event under event-number bucket `b` (fast path).
    #[inline(always)]
    unsafe fn push_x(&mut self, fx: &fast::Fx, b: usize, r: fast::XEv) {
        let (r, ext) = r.pack();
        self.push_xrec(fx, b, r);
        if let Some(e) = ext {
            self.push_xrec(fx, b, e);
        }
    }

    /// Files one 16-byte record under event-number bucket `b`.
    #[inline(always)]
    unsafe fn push_xrec(&mut self, fx: &fast::Fx, b: usize, r: fast::XRec) {
        let n = *self.xcnt.get_unchecked(b);
        let off = n as usize & (fast::XCH - 1);
        if off == 0 {
            let c = fx.xe.alloc(fast::XCH * fast::XREC) as usize;
            *self.xcur.get_unchecked_mut(b) = c;
            self.xchunks.get_unchecked_mut(b).push(c);
        }
        *self.xcnt.get_unchecked_mut(b) = n + 1;
        let k = (n & 7) as usize;
        let w = self.xwc.as_mut_ptr().add(b) as *mut fast::XRec;
        w.add(k).write(r);
        if k == 7 {
            mem::stream((*self.xcur.get_unchecked(b) as *mut u8).add((off - 7) * fast::XREC), w as *const u8, 2);
        }
    }

    /// Files a click under user-number bucket `c` (fast path).
    #[inline(always)]
    unsafe fn push_xclick(&mut self, fx: &fast::Fx, c: usize, r: CRec) {
        let n = *self.xccnt.get_unchecked(c);
        let off = n as usize & (CCH - 1);
        if off == 0 {
            let p = fx.xc.alloc(CCH * CREC) as usize;
            *self.xccur.get_unchecked_mut(c) = p;
            self.xcchunks.get_unchecked_mut(c).push(p);
        }
        *self.xccnt.get_unchecked_mut(c) = n + 1;
        let k = (n & 7) as usize;
        let w = self.xcwc.as_mut_ptr().add(c) as *mut CRec;
        w.add(k).write(r);
        if k == 7 {
            mem::stream((*self.xccur.get_unchecked(c) as *mut u8).add((off - 7) * CREC), w as *const u8, 4);
        }
    }

    /// Files an event under person bucket `p`; returns its slot.
    #[inline(always)]
    unsafe fn push_event(&mut self, sh: &Shared, p: usize, r: PRec) -> u32 {
        let f = self.pfill.get_unchecked_mut(p);
        let n = f.n;
        let off = n & (PCH as u32 - 1);
        if off == 0 {
            self.new_pchunk(sh, p);
        }
        let f = self.pfill.get_unchecked_mut(p);
        f.n = n + 1;
        let slot = f.cur + off;
        let k = (n & 15) as usize;
        let w = self.pwc.as_mut_ptr().add(p) as *mut PRec;
        w.add(k).write(r);
        if k == 15 {
            mem::stream(sh.ev.base().add((slot - 15) as usize * PREC), w as *const u8, 3);
        }
        slot
    }

    /// A fresh chunk for person bucket `p`'s events, key bucket `d`'s keys, person
    /// bucket `p`'s clicks: rare, kept out of line so the filing loops keep their
    /// registers.
    #[cold]
    #[inline(never)]
    unsafe fn new_pchunk(&mut self, sh: &Shared, p: usize) {
        let f = self.pfill.get_unchecked_mut(p);
        f.cur = (sh.ev.alloc_offset(PCH * PREC) / PREC) as u32;
        self.pchunks.get_unchecked_mut(p).push(f.cur);
    }

    #[cold]
    #[inline(never)]
    unsafe fn new_dchunk(&mut self, sh: &Shared, d: usize) {
        let c = sh.dk.alloc(DCH * 16) as usize;
        *self.dcur.get_unchecked_mut(d) = c;
        self.dchunks.get_unchecked_mut(d).push(c);
    }

    #[cold]
    #[inline(never)]
    unsafe fn new_cchunk(&mut self, sh: &Shared, p: usize) {
        let c = sh.ck.alloc(CCH * KREC) as usize;
        *self.ccur.get_unchecked_mut(p) = c;
        self.cchunks.get_unchecked_mut(p).push(c);
    }

    /// Files a dedup key under key bucket `d`.
    #[inline(always)]
    unsafe fn push_key(&mut self, sh: &Shared, d: usize, w0: u64, w1: u64) {
        let n = *self.dcnt.get_unchecked(d);
        let off = n as usize & (DCH - 1);
        if off == 0 {
            self.new_dchunk(sh, d);
        }
        *self.dcnt.get_unchecked_mut(d) = n + 1;
        let k = (n & 3) as usize;
        let w = self.dwc.as_mut_ptr().add(d) as *mut u64;
        *w.add(2 * k) = w0;
        *w.add(2 * k + 1) = w1;
        if k == 3 {
            mem::stream((*self.dcur.get_unchecked(d) as *mut u8).add((off - 3) * 16), w as *const u8, 1);
        }
    }

    /// Files a click under person bucket `p`; its click_id goes to `cids`.
    #[inline(always)]
    unsafe fn push_click(&mut self, sh: &Shared, p: usize, who: u32, ts: u32, camp: u32, cid: Id) {
        let cref = self.cids.len() as u32;
        self.cids.push(cid);
        let n = *self.ccnt.get_unchecked(p);
        let off = n as usize & (CCH - 1);
        if off == 0 {
            self.new_cchunk(sh, p);
        }
        *self.ccnt.get_unchecked_mut(p) = n + 1;
        let k = (n & 15) as usize;
        let w = self.cwc.as_mut_ptr().add(p) as *mut KRec;
        w.add(k).write(KRec { who, ts, camp, cref });
        if k == 15 {
            mem::stream((*self.ccur.get_unchecked(p) as *mut u8).add((off - 15) * KREC), w as *const u8, 4);
        }
    }

    /// Writes out the records still in the write-combining buffers.
    unsafe fn finish(&mut self, sh: &Shared) {
        for p in 0..self.pfill.len() {
            let f = self.pfill[p];
            let k = (f.n & 15) as usize;
            if k != 0 {
                let off = (f.n as usize - k) & (PCH - 1);
                let dst = sh.ev.base().add((f.cur as usize + off) * PREC);
                std::ptr::copy_nonoverlapping(self.pwc.as_ptr().add(p) as *const u8, dst, k * PREC);
            }
            let n = self.ccnt[p] as usize;
            let k = n & 15;
            if k != 0 {
                let off = (n - k) & (CCH - 1);
                let dst = (self.ccur[p] as *mut u8).add(off * KREC);
                std::ptr::copy_nonoverlapping(self.cwc.as_ptr().add(p) as *const u8, dst, k * KREC);
            }
        }
        for d in 0..self.dcnt.len() {
            let n = self.dcnt[d] as usize;
            let k = n & 3;
            if k != 0 {
                let off = (n - k) & (DCH - 1);
                let dst = (self.dcur[d] as *mut u8).add(off * 16);
                std::ptr::copy_nonoverlapping(self.dwc.as_ptr().add(d) as *const u8, dst, k * 16);
            }
        }
        if !self.xdone {
            // Once only: the fast path rewrites its buckets in place afterwards.
            self.xdone = true;
            for b in 0..self.xcnt.len() {
                let n = self.xcnt[b] as usize;
                let k = n & 7;
                if k != 0 {
                    let off = (n - k) & (fast::XCH - 1);
                    let dst = (self.xcur[b] as *mut u8).add(off * fast::XREC);
                    std::ptr::copy_nonoverlapping(self.xwc.as_ptr().add(b) as *const u8, dst, k * fast::XREC);
                }
            }
            for c in 0..self.xccnt.len() {
                let n = self.xccnt[c] as usize;
                let k = n & 7;
                if k != 0 {
                    let off = (n - k) & (CCH - 1);
                    let dst = (self.xccur[c] as *mut u8).add(off * CREC);
                    std::ptr::copy_nonoverlapping(self.xcwc.as_ptr().add(c) as *const u8, dst, k * CREC);
                }
            }
        }
        mem::stream_done();
        sh.links.added(&mut self.added);
    }

    /// Events of person bucket `p`, a chunk at a time.
    fn event_chunks<'a>(&'a self, sh: &'a Shared, p: usize) -> impl Iterator<Item = &'a [PRec]> + 'a {
        let total = self.pfill[p].n as usize;
        self.pchunks[p].iter().enumerate().map(move |(i, &s)| {
            let n = (total - i * PCH).min(PCH);
            unsafe { std::slice::from_raw_parts(sh.ev.base().add(s as usize * PREC) as *const PRec, n) }
        })
    }

    fn event_count(&self, p: usize) -> usize {
        self.pfill[p].n as usize
    }

    fn click_chunks<'a>(&'a self, p: usize) -> impl Iterator<Item = &'a [KRec]> + 'a {
        let total = self.ccnt[p] as usize;
        self.cchunks[p].iter().enumerate().map(move |(i, &c)| {
            let n = (total - i * CCH).min(CCH);
            unsafe { std::slice::from_raw_parts(c as *const KRec, n) }
        })
    }

    fn click_count(&self, p: usize) -> usize {
        self.ccnt[p] as usize
    }

    /// Dedup keys of key bucket `d`, as (identity, ingest << 32 | slot) pairs.
    fn key_chunks<'a>(&'a self, d: usize) -> impl Iterator<Item = &'a [[u64; 2]]> + 'a {
        let total = self.dcnt[d] as usize;
        self.dchunks[d].iter().enumerate().map(move |(i, &c)| {
            let n = (total - i * DCH).min(DCH);
            unsafe { std::slice::from_raw_parts(c as *const [u64; 2], n) }
        })
    }
}

/// Lines ahead of the current one whose user ID is hashed early, so its table line
/// is in cache by the time the line is parsed.
const AHEAD: usize = 12;

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");
const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

/// Starts the table lookup for the user field between separators `ip[0]` and `ip[1]`.
#[inline(always)]
unsafe fn prefetch_user(sh: &Shared, d: *const u8, ip: *const u32) {
    let s = *ip as usize + 1;
    let e = *ip.add(1) as usize;
    let q = (*d.add(s) == b'"') as usize;
    let n = e.wrapping_sub(s + 2 * q).min(15);
    sh.links.prefetch(Id::short(d.add(s + q), n).hash());
}

/// Phase A for one batch of event lines (rules.md "Events", steps 1, 3 and 4; step 2,
/// dedup, needs every copy, so invalid copies leave a key too).
#[inline(never)]
unsafe fn events(sh: &Shared, w: &mut Worker, rd: &Reader) {
    if sh.links.full.load(Relaxed) {
        return;
    }
    let d = rd.data();
    let win = &sh.win;
    rd.lines::<7>(|ip, ls| {
        prefetch_user(sh, d, ip.add(7 * AHEAD));
        // Fields are taken in line order, each used as soon as it is found, so little
        // has to be kept across the whole line.
        let p0 = *ip as usize;
        let (s0, e0) = unquote(d, ls, p0);
        if s0 == e0 {
            return; // empty event_id: dropped before dedup
        }
        let (kid, kbucket) = Id::key(d, s0, e0).hash2();
        let bucket = (kbucket >> sh.dshift) as usize;
        let p1 = *ip.add(1) as usize;
        let (s1, e1) = unquote(d, p0 + 1, p1);
        let uid = Id::field(d, s1, e1); // empty user_id: the empty ID
        let p2 = *ip.add(2) as usize;
        let (s2, e2) = unquote(d, p1 + 1, p2);
        let t = win.ts(d, s2, e2);
        let p3 = *ip.add(3) as usize;
        let (s3, e3) = unquote(d, p2 + 1, p3);
        let (ing_ms, ing_key) = win.ingest(d, s3, e3);
        let p4 = *ip.add(4) as usize;
        let (s4, e4) = unquote(d, p3 + 1, p4);
        let tw = (d.add(s4) as *const u64).read_unaligned();
        let tl = e4 - s4;
        let purchase = (tl == 8) & (tw == PURCHASE);
        let typed = purchase | ((tl == 4) & ((tw as u32 == VIEW) | (tw as u32 == CART)));
        let p5 = *ip.add(5) as usize;
        let (s5, e5) = unquote(d, p4 + 1, p5);
        // ts in [window_start, window_end), and ingested at most 24 hours after it
        // (an unreadable ingest_ts counts as infinitely late).
        let mut ok = typed
            & (uid != Id::EMPTY)
            & (e5 > s5)
            & ((t.wrapping_sub(WEEK) as u64) < sh.evspan)
            & (ing_ms.wrapping_sub(t.wrapping_add(win.base)) <= DAY);
        let mut pc = 0u32;
        if purchase & ok {
            let (s6, e6) = unquote(d, p5 + 1, *ip.add(6) as usize);
            match parse_price(std::slice::from_raw_parts(d.add(s6), e6 - s6)) {
                Some(p) => pc = p | (sh.lk.products.get(Id::field(d, s5, e5)).unwrap_or(sh.lk.unknown_cat) << 24),
                None => ok = false,
            }
        }
        if !ok {
            w.push_key(sh, bucket, kid, ((ing_key as u64) << 32) | NOSLOT as u64);
            return;
        }
        let who = sh.links.person(uid, uid.hash(), &mut w.added);
        let slot = w.push_event(sh, (who & sh.pmask) as usize, PRec { who, t: t as u32, pc });
        w.push_key(sh, bucket, kid, ((ing_key as u64) << 32) | slot as u64);
    });
    if w.added >= 256 {
        sh.links.added(&mut w.added);
    }
}

/// What becomes of one event line with the fast path on.
enum XOut {
    /// Nothing: an empty event_id.
    Drop,
    /// An event ID of another form than the generator's: kept aside, matched by the ID itself later.
    Odd(fast::OddEv),
    /// Filed under its event number.
    Rec(u64, fast::XEv),
}

/// One event line with the fast path on, given its seven fields without their
/// quotes: the same parsing and validation as `events`.
#[inline(always)]
unsafe fn event_x_decide(sh: &Shared, fx: &fast::Fx, w: &mut Worker, d: *const u8, f: &[(usize, usize); 7]) -> XOut {
    let win = &sh.win;
    let (s0, e0) = f[0];
    if s0 == e0 {
        return XOut::Drop; // empty event_id: dropped before dedup
    }
    let (s1, e1) = f[1];
    let (s2, e2) = f[2];
    let t = win.ts(d, s2, e2);
    let (s3, e3) = f[3];
    let (ing_ms, ing_key) = win.ingest(d, s3, e3);
    let (s4, e4) = f[4];
    let tw = (d.add(s4) as *const u64).read_unaligned();
    let tl = e4 - s4;
    let purchase = (tl == 8) & (tw == PURCHASE);
    let typed = purchase | ((tl == 4) & ((tw as u32 == VIEW) | (tw as u32 == CART)));
    let (s5, e5) = f[5];
    let mut ok = typed
        & (e1 > s1)
        & (e5 > s5)
        & ((t.wrapping_sub(WEEK) as u64) < sh.evspan)
        & (ing_ms.wrapping_sub(t.wrapping_add(win.base)) <= DAY);
    let mut pc = 0u32;
    if purchase & ok {
        let (s6, e6) = f[6];
        match parse_price(std::slice::from_raw_parts(d.add(s6), e6 - s6)) {
            Some(p) => pc = p | (sh.lk.products.get(Id::field(d, s5, e5)).unwrap_or(sh.lk.unknown_cat) << 24),
            None => ok = false,
        }
    }
    let idx = fx.event_index(d, s0, e0);
    let a = if !ok {
        fast::INVALID
    } else {
        let un = fx.user_num(d, s1, e1);
        if un != fast::NONE {
            un
        } else {
            w.nodd += 1;
            let uid = Id::field(d, s1, e1);
            fast::ODDU | sh.links.person(uid, uid.hash(), &mut w.added) as u64
        }
    };
    if idx == fast::NONE {
        w.nodd += 1;
        return XOut::Odd(fast::OddEv { key: Id::key(d, s0, e0), ing: ing_key, a, t: t as u32, pc });
    }
    let lo = (idx & ((1 << fx.xsh) - 1)) as u32;
    XOut::Rec(idx, fast::XEv { a, t: t as u32, pc, ing: ing_key, lo })
}

/// `event_x_decide`, and the line filed accordingly.
#[inline(always)]
unsafe fn event_x_fields(sh: &Shared, fx: &fast::Fx, w: &mut Worker, d: *const u8, f: &[(usize, usize); 7]) {
    match event_x_decide(sh, fx, w, d, f) {
        XOut::Drop => {}
        XOut::Odd(o) => w.odd.push(o),
        XOut::Rec(idx, r) => w.push_x(fx, (idx >> fx.xsh) as usize, r),
    }
}

/// Phase A for one batch of event lines with the fast path on, through the
/// separator index (`event_x_fields` for each well-formed line).
#[inline(never)]
unsafe fn events_x(sh: &Shared, w: &mut Worker, rd: &Reader) {
    if sh.links.full.load(Relaxed) {
        return;
    }
    let fx = sh.fx.as_ref().unwrap_unchecked();
    let d = rd.data();
    rd.lines::<7>(|ip, ls| {
        let mut f = [(0usize, 0usize); 7];
        let mut s = ls;
        for (k, g) in f.iter_mut().enumerate() {
            let e = *ip.add(k) as usize;
            *g = unquote(d, s, e);
            s = e + 1;
        }
        event_x_fields(sh, fx, w, d, &f);
    });
    if w.added >= 256 {
        sh.links.added(&mut w.added);
    }
}

/// The N fields of the line `d[ls..le]` (without its newline), read the plain
/// way (`split`); None for a malformed line.
#[allow(dead_code)]
unsafe fn plain_fields<const N: usize>(d: *const u8, ls: usize, le: usize) -> Option<[(usize, usize); N]> {
    let line = std::slice::from_raw_parts(d.add(ls), le - ls);
    let mut g: [&[u8]; N] = [&[]; N];
    if !split::<N>(line, &mut g) {
        return None;
    }
    let mut f = [(0usize, 0usize); N];
    for k in 0..N {
        let s = g[k].as_ptr() as usize - d as usize;
        f[k] = (s, s + g[k].len());
    }
    Some(f)
}

/// The line `d[ls..le]` with the fast path on, read the plain way and filed by
/// `event_x_fields`. The per-line decoder (dec.rs) hands every line it does not
/// take to this.
#[allow(dead_code)]
#[inline(never)]
unsafe fn event_x_line(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) {
    if let Some(f) = plain_fields(d, ls, le) {
        event_x_fields(sh, sh.fx.as_ref().unwrap_unchecked(), w, d, &f);
    }
}

/// Test hook (SS_DECCHECK=1): what the plain reading makes of a line the decoder took.
#[allow(dead_code)]
#[inline(never)]
unsafe fn event_x_plain(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) -> Option<(u64, fast::XEv)> {
    match event_x_decide(sh, sh.fx.as_ref().unwrap_unchecked(), w, d, &plain_fields(d, ls, le)?) {
        XOut::Rec(idx, r) => Some((idx, r)),
        _ => None,
    }
}

/// What becomes of one click line with the fast path on.
#[allow(dead_code)]
enum COut {
    Drop,
    X(usize, CRec), // filed under user-number bucket (`push_xclick`)
    P(usize, CRec), // filed under person bucket (`push_click`)
}

/// One click line with the fast path on, given its four fields without their
/// quotes: as `clicks`, but a click whose user ID has the generator's form is filed
/// under its user number.
#[inline(always)]
unsafe fn click_x_decide(sh: &Shared, fx: &fast::Fx, w: &mut Worker, d: *const u8, f: &[(usize, usize); 4]) -> COut {
    let win = &sh.win;
    let (s0, e0) = f[0];
    let (s1, e1) = f[1];
    if s0 == e0 || s1 == e1 {
        return COut::Drop;
    }
    let (s2, e2) = f[2];
    let camp = match sh.lk.campaigns.get(Id::field(d, s2, e2)) {
        Some(c) => c,
        None => return COut::Drop,
    };
    let (s3, e3) = f[3];
    let t = win.ts(d, s3, e3);
    if t as u64 >= win.span as u64 {
        return COut::Drop;
    }
    let cid = Id::field(d, s0, e0);
    let un = fx.user_num(d, s1, e1);
    if un != fast::NONE {
        let g = un >> 9;
        COut::X((g >> fx.cshift) as usize, CRec { who: g as u32, ts: t as u32, camp, _pad: (un & 511) as u32, cid })
    } else {
        let uid = Id::field(d, s1, e1);
        let who = sh.links.person(uid, uid.hash(), &mut w.added);
        COut::P((who & sh.pmask) as usize, CRec { who, ts: t as u32, camp, _pad: 0, cid })
    }
}

/// `click_x_decide`, and the line filed accordingly.
#[inline(always)]
unsafe fn click_x_fields(sh: &Shared, fx: &fast::Fx, w: &mut Worker, d: *const u8, f: &[(usize, usize); 4]) {
    match click_x_decide(sh, fx, w, d, f) {
        COut::Drop => {}
        COut::X(c, r) => w.push_xclick(fx, c, r),
        COut::P(p, r) => w.push_click(sh, p, r.who, r.ts, r.camp, r.cid),
    }
}

/// Phase A for one batch of click lines with the fast path on, through the
/// separator index (`click_x_fields` for each well-formed line).
#[inline(never)]
unsafe fn clicks_x(sh: &Shared, w: &mut Worker, rd: &Reader) {
    if sh.links.full.load(Relaxed) {
        return;
    }
    let fx = sh.fx.as_ref().unwrap_unchecked();
    let d = rd.data();
    rd.lines::<4>(|ip, ls| {
        let (p0, p1, p2, p3) = (*ip as usize, *ip.add(1) as usize, *ip.add(2) as usize, *ip.add(3) as usize);
        let f = [unquote(d, ls, p0), unquote(d, p0 + 1, p1), unquote(d, p1 + 1, p2), unquote(d, p2 + 1, p3)];
        click_x_fields(sh, fx, w, d, &f);
    });
    if w.added >= 256 {
        sh.links.added(&mut w.added);
    }
}

/// The click line `d[ls..le]` with the fast path on, read the plain way and filed by
/// `click_x_fields`. The click-line decoder (decc.rs) hands every line it does not
/// take to this.
#[allow(dead_code)]
#[inline(never)]
unsafe fn click_x_line(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) {
    if let Some(f) = plain_fields(d, ls, le) {
        click_x_fields(sh, sh.fx.as_ref().unwrap_unchecked(), w, d, &f);
    }
}

/// Test hook (SS_DECCHECK=1): what the plain reading makes of a click line the
/// decoder took.
#[allow(dead_code)]
#[inline(never)]
unsafe fn click_x_plain(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) -> COut {
    match plain_fields(d, ls, le) {
        Some(f) => click_x_decide(sh, sh.fx.as_ref().unwrap_unchecked(), w, d, &f),
        None => COut::Drop,
    }
}

/// Phase A for one batch of click lines (rules.md "Clicks", steps 1 and 3; duplicate
/// copies are identical, so they change neither the credit nor the assists and step 2
/// needs no work).
#[inline(never)]
unsafe fn clicks(sh: &Shared, w: &mut Worker, rd: &Reader) {
    if sh.links.full.load(Relaxed) {
        return;
    }
    let d = rd.data();
    let win = &sh.win;
    rd.lines::<4>(|ip, ls| {
        prefetch_user(sh, d, ip.add(4 * AHEAD));
        let (p0, p1, p2, p3) = (*ip as usize, *ip.add(1) as usize, *ip.add(2) as usize, *ip.add(3) as usize);
        let (s0, e0) = unquote(d, ls, p0);
        let (s1, e1) = unquote(d, p0 + 1, p1);
        if s0 == e0 || s1 == e1 {
            return;
        }
        let (s2, e2) = unquote(d, p1 + 1, p2);
        let camp = match sh.lk.campaigns.get(Id::field(d, s2, e2)) {
            Some(c) => c,
            None => return,
        };
        let (s3, e3) = unquote(d, p2 + 1, p3);
        let t = win.ts(d, s3, e3);
        if t as u64 >= win.span as u64 {
            return;
        }
        let uid = Id::field(d, s1, e1);
        let who = sh.links.person(uid, uid.hash(), &mut w.added);
        let cid = Id::field(d, s0, e0);
        w.push_click(sh, (who & sh.pmask) as usize, who, t as u32, camp, cid);
    });
    if w.added >= 256 {
        sh.links.added(&mut w.added);
    }
}

// ---------------------------------------------------------------------------
// Phase C state

type Rows = pq::UserRows;

struct CS {
    rows: Rows,
    credit: Vec<[u64; 2]>, // (camp*ndays + day) -> sessions | purchasing << 32, revenue
    assist: Vec<u32>,      // (camp*ndays + day) -> assisted sessions
    starts: Vec<i64>,
    days: Vec<u32>,
    tmp: Vec<(u32, u32)>,
    catrev: Vec<(u32, u64)>,
    off: Vec<u32>,  // per person of the bucket: start of their events (then, while filling, the next free place)
    coff: Vec<u32>, // the same for clicks
    ev: Vec<u64>,
    cl: Vec<Clk>,
    rtmp: Vec<u64>,
}

impl CS {
    fn new(cells: usize) -> CS {
        CS {
            rows: Rows::default(),
            credit: vec![[0; 2]; cells],
            assist: vec![0; cells],
            starts: Vec::new(),
            days: Vec::new(),
            tmp: Vec::new(),
            catrev: Vec::new(),
            off: Vec::new(),
            coff: Vec::new(),
            ev: Vec::new(),
            cl: Vec::new(),
            rtmp: Vec::new(),
        }
    }
}

struct Ctx<'a> {
    ndays: usize,
    dayoff: i64, // day index = (t + dayoff) / DAY
    organic: u32,
    day0: i64,
    cats: &'a [Vec<u8>],
}

fn bytes_of<T: Copy>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// Sorts (time << 32 | payload) keys by time: comparison sort when small, three
/// 11-bit radix passes over the time when large (whales, shared devices).
fn sort_events(v: &mut [u64], tmp: &mut Vec<u64>) {
    if v.len() < 1024 {
        v.sort_unstable();
        return;
    }
    tmp.clear();
    tmp.resize(v.len(), 0);
    let mut cnt = [[0u32; 2048]; 3];
    for &x in v.iter() {
        let t = x >> 32;
        cnt[0][(t & 2047) as usize] += 1;
        cnt[1][((t >> 11) & 2047) as usize] += 1;
        cnt[2][(t >> 22) as usize & 2047] += 1;
    }
    let (mut a, mut b): (&mut [u64], &mut [u64]) = (v, &mut tmp[..]);
    for (pass, c) in cnt.iter_mut().enumerate() {
        let mut sum = 0u32;
        for x in c.iter_mut() {
            let n = *x;
            *x = sum;
            sum += n;
        }
        let shift = 32 + 11 * pass as u32;
        for &x in a.iter() {
            let k = ((x >> shift) & 2047) as usize;
            b[c[k] as usize] = x;
            c[k] += 1;
        }
        std::mem::swap(&mut a, &mut b);
    }
    // Three passes: the sorted keys are in `tmp` (now `a`); copy back.
    b.copy_from_slice(a);
}

/// The ID table from the link files, and whether huge pages are in use.
/// Whether the record arenas go on huge pages, and the free memory in 2 MB blocks.
fn records_huge(files: &[Vec<(File, u64)>; 3], envn: &dyn Fn(&str) -> Option<usize>) -> (bool, Option<usize>) {
    let ebytes: u64 = files[0].iter().map(|f| f.1).sum();
    let cbytes: u64 = files[1].iter().map(|f| f.1).sum();
    // Huge pages for the record arenas when the kernel has free 2 MB blocks for what
    // they will touch (~28 bytes per event, ~32 per click) with 50% to spare, or when
    // /proc/buddyinfo cannot be read. On the scorer (THP madvise) a fresh GB costs
    // 66 ms in 4 KB pages on 8 threads but 28 ms in huge pages; on a host whose free
    // memory is fragmented, huge faults stall in compaction instead.
    let huge_free = mem::free_huge_bytes();
    let touched = (ebytes / 72 * 28 + cbytes / 45 * 32) as usize;
    let huge = match envn("SS_HUGE") {
        Some(v) => v == 1,
        None => huge_free.is_none_or(|f| f >= touched + touched / 2),
    };
    (huge, huge_free)
}

fn make_links(
    files: &[Vec<(File, u64)>; 3],
    nthreads: usize,
    scale: usize,
    (huge, huge_free): (bool, Option<usize>),
    envn: &dyn Fn(&str) -> Option<usize>,
    log: &dyn Fn(&str),
) -> (Links, bool) {
    let [event_files, click_files, link_files] = files;
    let bufsz = envn("SS_BUF").unwrap_or(64 << 10);
    let ebytes: u64 = event_files.iter().map(|f| f.1).sum();
    let cbytes: u64 = click_files.iter().map(|f| f.1).sum();
    let lbytes: u64 = link_files.iter().map(|f| f.1).sum();
    // About one in eight click and user IDs is longer than 15 bytes (interned).
    id::init_long_table((cbytes / 40 + ebytes / 700) as usize / 8 + 1, huge);
    let est = (ebytes / 72).max(1) as usize;
    let ltasks = ranges(link_files, 256 << 10);
    let lfiles: Vec<&File> = link_files.iter().map(|f| &f.0).collect();
    // The table must not fill up between two reports of a thread's additions: a
    // thread adds at most one ID per 8 bytes of a batch before it reports.
    let floor = nthreads * (256 + bufsz / 8) * 8;
    // Test hooks: SS_SLOTS forces the first attempt's table size, SS_FULLPCT the fill
    // at which the table counts as full, to exercise the start-over path.
    let (users, floor) = match envn("SS_SLOTS") {
        Some(n) if scale == 1 => (0, n),
        _ => ((est / 40).max(1) * scale, floor * scale),
    };
    // The ID table always asks for huge pages (SS_HUGE=0 turns that off; 1 also forces
    // them for the records). It is small, 128 MB at 20M events and 256 MB at 60M, and
    // even with almost no free 2 MB blocks the kernel found them quickly in every
    // measurement, where the records' gigabytes stall in compaction.
    let tab_free = if envn("SS_HUGE") == Some(0) { 0 } else { usize::MAX };
    let mut links = links::build(&lfiles, &ltasks, lbytes as usize, users, floor, nthreads, bufsz, tab_free);
    if let Some(pct) = envn("SS_FULLPCT") {
        links.limit = links.slots() / 100 * pct.min(85);
    }
    log(&format!(
        "links: {} ids in {} slots (huge pages: table {}, records {}; {} MB free in 2 MB blocks)",
        links.linked,
        links.slots(),
        if links.huge { "on" } else { "off" },
        if huge { "on" } else { "off" },
        huge_free.unwrap_or(0) >> 20
    ));
    (links, huge)
}

/// The lines of `f` that start in `[a, b)`, to a per-line decoder: from the file's
/// mapping where that is possible (in pieces of about `piece` bytes of whole lines),
/// through the read buffer for the rest.
#[allow(dead_code)]
fn decode(rd: &mut Reader, f: &File, m: Option<&scan::Mapped>, a: u64, b: u64, piece: usize, mut go: impl FnMut(*const u8, usize, usize)) {
    let Some(m) = m else {
        return rd.run_raw(f, a, b, go);
    };
    if a < m.lo.min(b) {
        rd.run_raw(f, a, b.min(m.lo), &mut go);
    }
    if let Some((s, e)) = m.span(a, b) {
        let mut x = s;
        while x < e {
            let y = m.piece(x, e, piece);
            go(unsafe { m.ptr().add(x) }, 0, y - x);
            x = y;
        }
    }
    if b > m.hi.max(a) {
        rd.run_raw(f, a.max(m.hi), b, &mut go);
    }
}

/// What phase A needs that does not depend on the link table: record arenas,
/// workers, tasks and file mappings. Made on a thread of its own while the links
/// are built (for the first attempt), so the parse starts right after them.
struct Prep {
    pb: usize,
    db: usize,
    ev: mem::Arena,
    dk: mem::Arena,
    ck: mem::Arena,
    tasks: Vec<(bool, usize, u64, u64)>,
    workers: Vec<Worker>,
    emaps: Vec<Option<scan::Mapped>>,
    cmaps: Vec<Option<scan::Mapped>>,
}

fn prep(files: &[Vec<(File, u64)>; 3], fx: Option<&fast::Fx>, huge: bool, nthreads: usize, envn: &dyn Fn(&str) -> Option<usize>) -> Prep {
    let tsetup = tl::now();
    let [event_files, click_files, _] = files;
    let task = envn("SS_TASK").unwrap_or(1 << 20) as u64;
    let ahead = envn("SS_AHEAD").unwrap_or(2 << 20);
    let ebytes: u64 = event_files.iter().map(|f| f.1).sum();
    let cbytes: u64 = click_files.iter().map(|f| f.1).sum();
    let est = (ebytes / 72).max(1) as usize;
    // Bucket counts: enough that a bucket is a few megabytes at most, few enough that
    // a thread's write-combining buffers (192 bytes per person bucket, 64 per key
    // bucket) stay well inside its share of the L2 cache. Person buckets are person
    // numbers modulo the bucket count. (The link table always has far more slots than
    // 512 buckets: see `phase_a`.)
    let pb = envn("SS_PB").unwrap_or((est / 65_536).next_power_of_two().clamp(64, 512));
    let db = envn("SS_DB").unwrap_or((est / 32_768).next_power_of_two().clamp(64, 1024));
    let (pb, db) = (pb.next_power_of_two().max(2), db.next_power_of_two().max(2));
    // A valid event line is at least 34 bytes, and each thread leaves at most one
    // partly filled chunk per bucket: this region cannot overflow.
    let ev = mem::Arena::new((ebytes as usize / 34 + (nthreads * pb + 1) * PCH) * PREC, huge, ahead);
    let dk = mem::Arena::new(ebytes as usize / 3 + nthreads * db * DCH * 16, huge, ahead);
    let ck = mem::Arena::new(cbytes as usize / 2 + nthreads * pb * CCH * KREC, huge, ahead);
    assert!((ebytes / 34 + ((nthreads * pb + 1) * PCH) as u64) < u32::MAX as u64);
    let mut tasks: Vec<(bool, usize, u64, u64)> = Vec::new();
    tasks.extend(ranges(event_files, task).into_iter().map(|(f, a, b)| (true, f, a, b)));
    tasks.extend(ranges(click_files, task).into_iter().map(|(f, a, b)| (false, f, a, b)));
    let (nb, nxc) = fx.map_or((0, 0), |f| (f.nb, f.nxc));
    // About one valid click per 45 bytes of click files, shared by the threads.
    let ncid = (cbytes as usize / 45 / nthreads) * 5 / 4;
    let workers: Vec<Worker> = (0..nthreads).map(|_| Worker::new(pb, db, nb, nxc, ncid)).collect();
    // The per-line decoders read the files straight from their mappings, prefetching
    // a few lines ahead (SS_MMAP=0: through the read buffer, as the indexed path does).
    // (The same conditions as `Shared::dec` and `Shared::dec5` below.)
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
    let mapped = (if fx.is_some() { envn("SS_DEC") != Some(0) } else { envn("SS_DEC5") != Some(0) }) && envn("SS_MMAP") != Some(0);
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2")))]
    let mapped = false;
    let maps = |files: &[(File, u64)]| -> Vec<Option<scan::Mapped>> {
        files.iter().map(|f| if mapped { scan::Mapped::new(&f.0, f.1) } else { None }).collect()
    };
    let (emaps, cmaps) = (maps(event_files), maps(click_files));
    tl::span(9, tsetup);
    Prep { pb, db, ev, dk, ck, tasks, workers, emaps, cmaps }
}

/// The fast path's setup (fast.rs), if the data has the public generator's ID form.
fn fast_setup(files: &[Vec<(File, u64)>; 3], nthreads: usize, huge: bool, envn: &dyn Fn(&str) -> Option<usize>) -> Option<fast::Fx> {
    let (eb, cb): (u64, u64) = (files[0].iter().map(|f| f.1).sum(), files[1].iter().map(|f| f.1).sum());
    fast::setup(&files[0], eb, cb, nthreads, huge, envn("SS_AHEAD").unwrap_or(2 << 20), envn("SS_XSH").unwrap_or(16) as u32, envn("SS_FAST") != Some(2))
}

/// Phase A: every event and click line parsed and filed. None if the ID table
/// filled up (far more distinct users than estimated): the caller starts over with
/// a larger one.
#[allow(clippy::too_many_arguments)]
fn phase_a<'a>(
    lk: &'a Lookups,
    (links, huge): (Links, bool),
    fx: Option<fast::Fx>,
    pre: Option<Prep>,
    files: &[Vec<(File, u64)>; 3],
    ws: i64,
    we: i64,
    nthreads: usize,
    envn: &dyn Fn(&str) -> Option<usize>,
    log: &dyn Fn(&str),
) -> Option<(Shared<'a>, Vec<Worker>, usize, usize, Vec<Option<scan::Mapped>>)> {
    let [event_files, click_files, _] = files;
    let bufsz = envn("SS_BUF").unwrap_or(64 << 10);
    let Prep { pb, db, ev, dk, ck, tasks, mut workers, emaps, cmaps } = pre.unwrap_or_else(|| prep(files, fx.as_ref(), huge, nthreads, envn));
    assert!(pb <= links.slots());
    let base = ws - WEEK;
    let sh = Shared {
        pbits: pb.trailing_zeros(),
        pmask: pb as u32 - 1,
        links,
        lk,
        win: Win::new(base, we),
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
        kern: kern::Kern::new(base, we),
        evspan: (we - ws) as u64,
        dshift: 64 - db.trailing_zeros(),
        ev,
        dk,
        ck,
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
        dec: (fx.is_some() && envn("SS_DEC") != Some(0)).then(|| dec::Dec::new(base, we, (we - ws) as u64, lk)),
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
        decc: (fx.is_some() && envn("SS_DEC") != Some(0) && envn("SS_DECC") != Some(0)).then(|| decc::DecC::new(lk)),
        deccheck: envn("SS_DECCHECK") == Some(1),
        dec5: cfg!(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2")) && fx.is_none() && envn("SS_DEC5") != Some(0),
        kern_on: envn("SS_KERN") == Some(1),
        fx,
    };
    let fast = sh.fx.is_some();
    // Tasks left per file: the thread that finishes a file's last one releases its mapping.
    let left = |files: &[(File, u64)], events: bool| -> Vec<AtomicUsize> {
        (0..files.len()).map(|f| AtomicUsize::new(tasks.iter().filter(|t| t.0 == events && t.1 == f).count())).collect()
    };
    let eleft = left(event_files, true);
    {
        let next = AtomicUsize::new(0);
        let (sh, tasks, next, emaps, cmaps, eleft) = (&sh, &tasks, &next, &emaps, &cmaps, &eleft);
        std::thread::scope(|sc| {
            if let Some(f) = &sh.fx {
                // The fast path's table of user numbers is only needed after phase A:
                // one more thread fills it meanwhile.
                sc.spawn(move || {
                    let t = Instant::now();
                    fast::build_gt(f, &sh.links, 1);
                    if std::env::var_os("SS_TIME").is_some() {
                        eprintln!("  build_gt took {:?}", t.elapsed());
                    }
                });
            }
            for w in workers.iter_mut() {
                sc.spawn(move || {
                    let mut rd = Reader::new(bufsz);
                    loop {
                        let t = next.fetch_add(1, Relaxed);
                        if t >= tasks.len() {
                            break;
                        }
                        let (is_event, f, a, b) = tasks[t];
                        let ttask = tl::now();
                        match (is_event, fast) {
                            #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
                            (_, false) if sh.dec5 => {
                                let go = |d: *const u8, s: usize, n: usize| unsafe {
                                    if !sh.links.full.load(Relaxed) {
                                        if is_event {
                                            dec5::events(sh, w, d, s, n);
                                        } else {
                                            // Long click IDs are kept by reference when the lines are read from the mapping.
                                            let ext = cmaps[f].as_ref().is_some_and(|m| m.contains(d));
                                            dec5::clicks(sh, w, d, s, n, ext);
                                        }
                                        if w.added >= 256 {
                                            sh.links.added(&mut w.added);
                                        }
                                    }
                                };
                                let (fs, ms) = if is_event { (event_files, &emaps) } else { (click_files, &cmaps) };
                                decode(&mut rd, &fs[f].0, ms[f].as_ref(), a, b, bufsz, go);
                            }
                            (true, false) => rd.run(&event_files[f].0, a, b, |rd| unsafe { events(sh, w, rd) }),
                            #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
                            (true, true) if sh.dec.is_some() => {
                                let asm = sh.dec.as_ref().is_some_and(|d| d.asm);
                                let go = |d: *const u8, s: usize, n: usize| unsafe {
                                    if !sh.links.full.load(Relaxed) {
                                        if asm {
                                            dec::events_a(sh, w, d, s, n);
                                        } else {
                                            dec::events(sh, w, d, s, n);
                                        }
                                        if w.added >= 256 {
                                            sh.links.added(&mut w.added);
                                        }
                                    }
                                };
                                decode(&mut rd, &event_files[f].0, emaps[f].as_ref(), a, b, bufsz, go);
                            }
                            (true, true) => rd.run(&event_files[f].0, a, b, |rd| unsafe { events_x(sh, w, rd) }),
                            (false, false) => rd.run(&click_files[f].0, a, b, |rd| unsafe { clicks(sh, w, rd) }),
                            #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
                            (false, true) if sh.decc.is_some() => {
                                let casm = sh.decc.as_ref().is_some_and(|d| d.asm);
                                let go = |d: *const u8, s: usize, n: usize| unsafe {
                                    if !sh.links.full.load(Relaxed) {
                                        if casm {
                                            decc::clicks_a(sh, w, d, s, n);
                                        } else {
                                            decc::clicks(sh, w, d, s, n);
                                        }
                                        if w.added >= 256 {
                                            sh.links.added(&mut w.added);
                                        }
                                    }
                                };
                                decode(&mut rd, &click_files[f].0, cmaps[f].as_ref(), a, b, bufsz, go);
                            }
                            (false, true) => rd.run(&click_files[f].0, a, b, |rd| unsafe { clicks_x(sh, w, rd) }),
                        }
                        tl::span(if is_event { 6 } else { 7 }, ttask);
                        // An event file's mapping is released by the thread that finishes its last
                        // piece. Click files stay mapped until phase C is done: long click IDs point
                        // into them and phase C's tie-breaks read them (a released mapping was
                        // faulted back in there, then left for exit to tear down on one thread).
                        if is_event && eleft[f].fetch_sub(1, Relaxed) == 1 {
                            if let Some(m) = &emaps[f] {
                                let tr = tl::now();
                                m.release();
                                tl::span(31, tr);
                            }
                        }
                    }
                    let tf = tl::now();
                    unsafe { w.finish(sh) };
                    tl::span(8, tf);
                });
            }
        });
    }
    if sh.links.full.load(Relaxed) {
        // Give the abandoned attempt's memory back before trying again.
        sh.ev.release_part(0, 1);
        sh.dk.release_part(0, 1);
        sh.ck.release_part(0, 1);
        sh.links.release_part(0, 1);
        if let Some(f) = &sh.fx {
            f.release_part(0, 1);
        }
        scan::release_maps_part(&cmaps, 0, 1);
        return None;
    }
    log(&format!("phase A parsed ({pb} person buckets, {db} key buckets)"));
    if sh.dec5 {
        let sum = |f: fn(&Worker) -> u64| -> u64 { workers.iter().map(f).sum() };
        log(&format!(
            "dec5: event lines {} fast, {} plain; click lines {} fast, {} plain",
            sum(|w| w.nfast),
            sum(|w| w.ncold),
            sum(|w| w.ncfast),
            sum(|w| w.nccold)
        ));
    }
    if envn("SS_STOP") == Some(1) {
        std::process::exit(0); // test hook: time the parse alone
    }
    Some((sh, workers, pb, db, cmaps))
}

fn main() {
    // Timeline trace (SS_TL): stage numbers are listed in tools/tl.py.
    tl::init();
    tl::span(21, tl::now());
    let t0 = Instant::now();
    // Large malloc allocations (row columns, pass buffers) go on transparent huge pages
    // when the machine has plenty of free 2 MB blocks, where they halve the page faults;
    // with few, the kernel would stall compacting memory instead. glibc reads the tunable
    // only at start, so the process restarts itself once with it set. The image sets it
    // already (Dockerfile), which saves that restart on a machine like the scorer's; on
    // one without the free blocks the process restarts itself once without it instead.
    const TUNE: &str = "glibc.malloc.hugetlb=1";
    let tuned = std::env::var_os("GLIBC_TUNABLES");
    if tuned.is_none() || tuned.as_deref() == Some(std::ffi::OsStr::new(TUNE)) {
        let want = std::env::var("SS_HUGE").map_or(true, |v| v != "0") && mem::free_huge_bytes().is_some_and(|f| f >= 8 << 30);
        if want != tuned.is_some() {
            use std::os::unix::process::CommandExt;
            let mut cmd = std::process::Command::new("/proc/self/exe");
            cmd.args(std::env::args_os().skip(1));
            if want {
                cmd.env("GLIBC_TUNABLES", TUNE);
            } else {
                cmd.env_remove("GLIBC_TUNABLES");
            }
            // exec only returns on failure; then carry on as we are.
            let _ = cmd.exec();
        }
    }
    // Grow the descriptor table to 1,024 entries now, while there is one thread: growing
    // it later, with threads running (opening output files), waits for an RCU grace period.
    #[cfg(unix)]
    unsafe {
        if libc::dup2(2, 1023) == 1023 {
            libc::close(1023);
        }
    }
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let envn = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    let nthreads: usize = envn("NCPU")
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 256);
    let log = |what: &str| eprintln!("[{:7.3}s] {what}", t0.elapsed().as_secs_f64());

    let (ws, we) = read_manifest(&data);
    let files = [csv_files(&data.join("events")), csv_files(&data.join("clicks")), csv_files(&data.join("links"))];
    // The product and campaign tables, the fast path's sample check and phase A's
    // arenas, workers and file mappings are made on a thread of their own while the
    // link files are processed: none of them needs the links.
    let rh = records_huge(&files, &envn);
    let (lk, first, (fx0, pre0)) = std::thread::scope(|sc| {
        let side = sc.spawn(|| {
            let t = tl::now();
            let lk = read_lookups(&data);
            tl::span(1, t);
            let t = tl::now();
            let fx = (envn("SS_FAST") != Some(0) && lk.channels.len() < 1 << 23).then(|| fast_setup(&files, nthreads, rh.0, &envn)).flatten();
            tl::span(5, t);
            let pre = prep(&files, fx.as_ref(), rh.0, nthreads, &envn);
            (lk, (fx, pre))
        });
        let t = tl::now();
        let links = make_links(&files, nthreads, 1, rh, &envn, &log);
        tl::span(4, t);
        let (lk, fp) = side.join().expect("lookups");
        (lk, links, fp)
    });

    // All in-window times are stored as u32 milliseconds since `base`.
    let base = ws - WEEK;
    assert!(we - base < 1i64 << 31, "window too long for 31-bit offsets");
    assert!(lk.categories.len() <= 256, "more than 256 categories");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = lk.channels.len();
    let ctx = Ctx { ndays, dayoff: base - day0 * DAY, organic: ncamp as u32, day0, cats: &lk.categories };

    // ---- Phase A: parse events and clicks (again, with a larger ID table, if it filled up).
    let mut scale = 1;
    let mut links = Some(first);
    // The index-order fast path (fast.rs) is used when the data's IDs have the public
    // generator's form; SS_FAST=0 turns it off (SS_FAST=2, a test hook, skips its sample
    // check). Without it the general pipeline runs alone.
    // (The event stage keeps a campaign number in 23 bits.)
    let mut fast_on = envn("SS_FAST") != Some(0) && ncamp < 1 << 23;
    let mut first_fx = Some((fx0, pre0));
    let (sh, workers, pb, db, mut cs, cmaps) = loop {
        let l = links.take().unwrap_or_else(|| make_links(&files, nthreads, scale, records_huge(&files, &envn), &envn, &log));
        let (fx, pre) = match first_fx.take() {
            Some((fx, pre)) => (fx, Some(pre)),
            None => {
                let tfx = tl::now();
                let fx = if fast_on { fast_setup(&files, nthreads, l.1, &envn) } else { None };
                tl::span(5, tfx);
                (fx, None)
            }
        };
        if fx.is_some() {
            log("fast: keys found");
        }
        match phase_a(&lk, l, fx, pre, &files, ws, we, nthreads, &envn, &log) {
            Some(x) => {
                if x.0.fx.is_some() {
                    // If many lines did not fit the fast path after all, it is not worth
                    // its two layers: start over with the general pipeline alone.
                    let (fit, odd) = fast::stats(&x.1);
                    log(&format!("fast: {fit} event lines by number, {odd} lines the general way"));
                    let cold: u64 = x.1.iter().map(|w| w.ncold).sum();
                    log(&format!("decoder: {cold} event lines read the plain way"));
                    let ccold: u64 = x.1.iter().map(|w| w.nccold).sum();
                    log(&format!("decoder: {ccold} click lines read the plain way"));
                    if odd * 20 > fit {
                        x.0.ev.release_part(0, 1);
                        scan::release_maps_part(&x.4, 0, 1);
                        x.0.dk.release_part(0, 1);
                        x.0.ck.release_part(0, 1);
                        x.0.links.release_part(0, 1);
                        x.0.fx.as_ref().unwrap().release_part(0, 1);
                        fast_on = false;
                        continue;
                    }
                }
                let (sh, mut workers, pb, db, cmaps) = x;
                let mut cs: Vec<CS> = (0..nthreads).map(|_| CS::new((ncamp + 1) * ctx.ndays)).collect();
                // ---- Fast path: the event stage (stage.rs) turns the events filed by
                // number into rows, run by run; what does not fit is filed for phases B
                // and C below. If the events turn out not to be numbered user by user,
                // or the ID table fills up, everything starts over.
                if sh.fx.is_some() {
                    let ok = stage::run(&sh, &mut workers, &mut cs, &ctx, &log);
                    let full = sh.links.full.load(Relaxed);
                    if !ok || full {
                        log(if full { "stage: ID table full, starting over" } else { "stage: events are not numbered user by user, starting over without the fast path" });
                        sh.ev.release_part(0, 1);
                        scan::release_maps_part(&cmaps, 0, 1);
                        sh.dk.release_part(0, 1);
                        sh.ck.release_part(0, 1);
                        sh.links.release_part(0, 1);
                        sh.fx.as_ref().unwrap().release_part(0, 1);
                        if full {
                            scale *= 4;
                        } else {
                            fast_on = false;
                        }
                        continue;
                    }
                }
                break (sh, workers, pb, db, cs, cmaps);
            }
            None => scale *= 4,
        }
    };

    // ---- Phase B: dedup on event id. Losing copies are listed for phase C to drop
    // (post.rs; SS_POST=0: marked dead in their person's records, as before).
    let tdd = tl::now();
    let kills = (envn("SS_POST") != Some(0)).then(|| post::dedup(&sh, &workers, nthreads, pb, db));
    tl::span(22, tdd);
    if kills.is_none() {
        let (sh, workers) = (&sh, &workers);
        let next = AtomicUsize::new(0);
        let next = &next;
        let done = std::sync::Barrier::new(nthreads);
        let done = &done;
        std::thread::scope(|sc| {
            for ti in 0..nthreads {
                sc.spawn(move || {
                    // Once every thread is done with the keys, each gives back its share of
                    // their memory, rather than leave it all to process exit.
                    let _free = Defer(|| {
                        done.wait();
                        sh.dk.release_part(ti, nthreads);
                    });
                    let mut tab: Vec<[u64; 2]> = Vec::new();
                    let mut used: Vec<u64> = Vec::new();
                    let evbase = sh.ev.base();
                    let kill = |slot: u32| {
                        if slot != NOSLOT {
                            // Each record is named by exactly one key, so this write races with nothing.
                            unsafe { (*(evbase.add(slot as usize * PREC) as *mut PRec)).t |= DEAD };
                        }
                    };
                    loop {
                        let d = next.fetch_add(1, Relaxed);
                        if d >= db {
                            break;
                        }
                        let total: usize = workers.iter().map(|w| w.dcnt[d] as usize).sum();
                        if total == 0 {
                            continue;
                        }
                        let cap = (total * 2).next_power_of_two().max(64);
                        let (mask, shift) = (cap - 1, 64 - cap.trailing_zeros());
                        if tab.len() < cap {
                            tab.resize(cap, [0; 2]);
                        }
                        used.clear();
                        used.resize(cap / 64, 0);
                        for w in workers {
                            for chunk in w.key_chunks(d) {
                                for &[id, v] in chunk {
                                    let mut s = (id >> shift) as usize;
                                    loop {
                                        let (word, bit) = (s >> 6, 1u64 << (s & 63));
                                        unsafe {
                                            if *used.get_unchecked(word) & bit == 0 {
                                                *used.get_unchecked_mut(word) |= bit;
                                                *tab.get_unchecked_mut(s) = [id, v];
                                                break;
                                            }
                                            let o = tab.get_unchecked_mut(s);
                                            if o[0] == id {
                                                // Another copy of this event: the smaller ingest_ts stays.
                                                if (v >> 32) < (o[1] >> 32) {
                                                    kill(o[1] as u32);
                                                    o[1] = v;
                                                } else {
                                                    kill(v as u32);
                                                }
                                                break;
                                            }
                                        }
                                        s = (s + 1) & mask;
                                    }
                                }
                            }
                        }
                    }
                });
            }
        });
    }
    log(&format!("phase B dedup ({} events, {} clicks in the general buckets)", workers.iter().map(|w| (0..pb).map(|p| w.event_count(p)).sum::<usize>()).sum::<usize>(), workers.iter().map(|w| (0..pb).map(|p| w.click_count(p)).sum::<usize>()).sum::<usize>()));

    // ---- Phase C: sessions, attribution, aggregation per person bucket; each thread
    // then writes its rows as one user_daily file.
    let tcs = tl::now();
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    {
        let (sh, workers, ctx, ud, kills, cmaps) = (&sh, &workers, &ctx, &ud, &kills, &cmaps);
        // Largest buckets first, so a whale's bucket never starts last.
        let mut order: Vec<usize> = (0..pb).collect();
        order.sort_by_key(|&p| std::cmp::Reverse(workers.iter().map(|w| w.event_count(p) + w.click_count(p)).sum::<usize>()));
        let order = &order;
        tl::span(20, tcs);
        let next = AtomicUsize::new(0);
        let next = &next;
        // The tail of phase C, without a barrier: a thread that has written its rows
        // starts on the memory release as soon as every thread is done with its buckets
        // (taking pieces from a shared list, so that early finishers do more of it), and
        // the first thread to finish writing also writes campaign_daily.
        let cells = (ncamp + 1) * ndays;
        let bdone = AtomicUsize::new(0);
        let claimed = std::sync::atomic::AtomicBool::new(false);
        let parts: Vec<[AtomicUsize; 2]> = (0..nthreads).map(|_| [AtomicUsize::new(0), AtomicUsize::new(0)]).collect();
        // Release pieces: (region, part). The big regions in RP parts each, then each
        // worker's click IDs (long ones point into the click files) and the long-ID table.
        const RP: usize = 32;
        let mut rel: Vec<(u8, usize)> = Vec::new();
        for region in 0..4 + sh.fx.is_some() as u8 {
            rel.extend((0..RP).map(|k| (region, k)));
        }
        rel.extend((0..nthreads).map(|w| (5, w)));
        rel.extend((0..RP).map(|k| (6, k)));
        let rel = &rel;
        let rnext = AtomicUsize::new(0);
        let (bdone, claimed, parts, rnext, lk, cd) = (&bdone, &claimed, &parts, &rnext, &lk, &cd);
        std::thread::scope(|sc| {
            for (ti, st) in cs.iter_mut().enumerate() {
                sc.spawn(move || {
                    let mut scr = post::Scr::default();
                    let c0 = post::thread_cpu_ns();
                    loop {
                        let k = next.fetch_add(1, Relaxed);
                        if k >= pb {
                            break;
                        }
                        let tb = tl::now();
                        match kills {
                            Some(kl) => post::bucket(st, &mut scr, sh, workers, kl, ctx, order[k]),
                            None => bucket(st, sh, workers, ctx, order[k]),
                        }
                        tl::span(14, tb);
                    }
                    let tb = tl::now();
                    post::finish(st, &mut scr);
                    tl::span(15, tb);
                    drop(scr);
                    post::CPU_C.fetch_add(post::thread_cpu_ns() - c0, Relaxed);
                    if std::env::var_os("SS_TIME").is_some() {
                        eprintln!("  thread {ti}: buckets done at {:?}", t0.elapsed());
                    }
                    // This thread's campaign cells are final (st is not touched again before
                    // every thread has passed the release wait below).
                    parts[ti][0].store(st.credit.as_ptr() as usize, Relaxed);
                    parts[ti][1].store(st.assist.as_ptr() as usize, Relaxed);
                    bdone.fetch_add(1, std::sync::atomic::Ordering::Release);
                    let tt = Instant::now();
                    let ttl = tl::now();
                    pq::write_user(&ud.join(format!("part-{ti:05}.parquet")), &st.rows, ctx.cats).expect("write user_daily");
                    let tw = tt.elapsed();
                    tl::span(16, ttl);
                    let all_buckets_done = || {
                        let mut spins = 0u32;
                        while bdone.load(std::sync::atomic::Ordering::Acquire) < nthreads {
                            spins += 1;
                            if spins < 2000 {
                                std::hint::spin_loop();
                            } else {
                                std::thread::sleep(std::time::Duration::from_micros(20));
                            }
                        }
                    };
                    if !claimed.swap(true, Relaxed) {
                        let tb = tl::now();
                        all_buckets_done();
                        tl::span(17, tb);
                        let tb = tl::now();
                        let views: Vec<(&[[u64; 2]], &[u32])> = parts
                            .iter()
                            .map(|p| unsafe {
                                (
                                    std::slice::from_raw_parts(p[0].load(Relaxed) as *const [u64; 2], cells),
                                    std::slice::from_raw_parts(p[1].load(Relaxed) as *const u32, cells),
                                )
                            })
                            .collect();
                        write_campaign(lk, ctx, &views, cd);
                        tl::span(19, tb);
                    }
                    // Once every thread is done with the records, give their memory back
                    // here, in parallel, rather than leave it to process exit.
                    let tb = tl::now();
                    all_buckets_done();
                    tl::span(17, tb);
                    let tb = tl::now();
                    let tr = Instant::now();
                    loop {
                        let k = rnext.fetch_add(1, Relaxed);
                        if k >= rel.len() {
                            break;
                        }
                        let (region, part) = rel[k];
                        match region {
                            0 => sh.ev.release_part(part, RP),
                            1 => sh.ck.release_part(part, RP),
                            2 => scan::release_maps_part(cmaps, part, RP),
                            3 => sh.links.release_part(part, RP),
                            4 => sh.fx.as_ref().unwrap().release_part(part, RP),
                            5 => {
                                // Only the whole pages inside the buffer (it is never freed or read again).
                                let c = &workers[part].cids;
                                let (a, b) = (c.as_ptr() as usize, c.as_ptr() as usize + c.capacity() * std::mem::size_of::<Id>());
                                let (a, b) = ((a + 4095) & !4095, b & !4095);
                                if b > a {
                                    mem::release(a as *mut u8, b - a);
                                }
                            }
                            _ => id::release_long_table_part(part, RP),
                        }
                    }
                    let (credit, assist, n) = (std::mem::take(&mut st.credit), std::mem::take(&mut st.assist), st.rows.n);
                    *st = CS::new(0);
                    st.credit = credit;
                    st.assist = assist;
                    st.rows.n = n;
                    tl::span(18, tb);
                    if std::env::var_os("SS_TIME").is_some() {
                        eprintln!("  thread {ti}: rows written in {tw:?}, memory returned in {:?}", tr.elapsed());
                    }
                });
            }
        });
    }
    log("phase C sessions, user_daily written");
    if std::env::var_os("SS_SECT").is_some() {
        let v: Vec<String> = post::SECT.iter().map(|x| format!("{:.3}", x.load(Relaxed) as f64 / 2.4e9)).collect();
        log(&format!("post sections (s at a 2.4 GHz TSC, thread sums): {}", v.join(" ")));
    }
    log(&format!("post cpu: dedup {:.3} s, buckets {:.3} s (thread sums)", post::CPU_B.load(Relaxed) as f64 * 1e-9, post::CPU_C.load(Relaxed) as f64 * 1e-9));

    let nrows: usize = cs.iter().map(|s| s.rows.n).sum();
    log(&format!("wrote {nrows} user rows"));
    tl::dump();
    // Outputs are written and closed; skip freeing buffers.
    std::process::exit(0);
}

/// campaign_daily from every thread's campaign cells (rules.md "Output tables").
fn write_campaign(lk: &Lookups, ctx: &Ctx, parts: &[(&[[u64; 2]], &[u32])], cd: &Path) {
    let (ncamp, ndays, day0) = (lk.channels.len(), ctx.ndays, ctx.day0);
    let mut camp_tot = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for &(credit, assist) in parts {
        for ((a, c), &x) in camp_tot.iter_mut().zip(credit.iter()).zip(assist.iter()) {
            a[0] += c[0] & 0xFFFF_FFFF;
            a[1] += c[0] >> 32;
            a[2] += c[1];
            a[3] += x as u64;
        }
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
}

/// Runs a closure when dropped.
struct Defer<F: FnMut()>(F);
impl<F: FnMut()> Drop for Defer<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

/// Phase C for one person bucket, a range of person numbers: counting-sort its
/// events and clicks by person, sort each person's by time, and run the rules.
fn bucket(st: &mut CS, sh: &Shared, workers: &[Worker], ctx: &Ctx, p: usize) {
    let n_ev: usize = workers.iter().map(|w| w.event_count(p)).sum();
    if n_ev == 0 {
        return;
    }
    let (pbits, first) = (sh.pbits, p as u32); // person number = place << pbits | p
    let np = sh.places();
    // Events per person, then each person's start in `ev`.
    st.off.clear();
    st.off.resize(np + 1, 0);
    for w in workers {
        for chunk in w.event_chunks(sh, p) {
            for e in chunk {
                // A dead copy counts for nobody (the count is shifted out of the last slot).
                let i = if e.t & DEAD == 0 { (e.who >> pbits) as usize } else { np };
                unsafe { *st.off.get_unchecked_mut(i) += 1 };
            }
        }
    }
    let mut sum = 0u32;
    for x in st.off[..np].iter_mut() {
        let n = *x;
        *x = sum;
        sum += n;
    }
    st.off[np] = sum;
    // Clicks per person; a click of a person with no surviving event credits nothing.
    st.coff.clear();
    st.coff.resize(np + 1, 0);
    for w in workers {
        for chunk in w.click_chunks(p) {
            for c in chunk {
                let i = (c.who >> pbits) as usize;
                let has = unsafe { *st.off.get_unchecked(i + 1) != *st.off.get_unchecked(i) };
                unsafe { *st.coff.get_unchecked_mut(if has { i } else { np }) += 1 };
            }
        }
    }
    let mut csum = 0u32;
    for x in st.coff[..np].iter_mut() {
        let n = *x;
        *x = csum;
        csum += n;
    }
    st.coff[np] = csum;
    // File the clicks first: it needs the event starts untouched to tell who has events.
    st.cl.clear();
    st.cl.resize(csum as usize, Clk { ts: 0, camp: 0, cid: Id::EMPTY });
    for w in workers {
        for chunk in w.click_chunks(p) {
            for c in chunk {
                let i = (c.who >> pbits) as usize;
                unsafe {
                    if *st.off.get_unchecked(i + 1) != *st.off.get_unchecked(i) {
                        let at = st.coff.get_unchecked_mut(i);
                        *st.cl.get_unchecked_mut(*at as usize) = Clk { ts: c.ts, camp: c.camp, cid: *w.cids.get_unchecked(c.cref as usize) };
                        *at += 1;
                    }
                }
            }
        }
    }
    st.ev.clear();
    st.ev.resize(sum as usize, 0);
    for w in workers {
        for chunk in w.event_chunks(sh, p) {
            for e in chunk {
                if e.t & DEAD == 0 {
                    unsafe {
                        let at = st.off.get_unchecked_mut((e.who >> pbits) as usize);
                        *st.ev.get_unchecked_mut(*at as usize) = ((e.t as u64) << 32) | e.pc as u64;
                        *at += 1;
                    }
                }
            }
        }
    }
    // After filing, off[i] and coff[i] are the ends of person i's rows; the starts
    // are the previous person's ends.
    let mut ev = std::mem::take(&mut st.ev);
    let mut cl = std::mem::take(&mut st.cl);
    let mut rtmp = std::mem::take(&mut st.rtmp);
    let (mut es, mut cstart) = (0usize, 0usize);
    for i in 0..np {
        let (ee, ce) = (st.off[i] as usize, st.coff[i] as usize);
        if ee > es {
            let e = &mut ev[es..ee];
            sort_events(e, &mut rtmp);
            let c = &mut cl[cstart..ce];
            if c.len() > 1 {
                // Time order only; equal-time ties are broken by click_id when crediting.
                c.sort_unstable_by_key(|x| x.ts);
            }
            let name = sh.links.name((i as u32) << pbits | first);
            person(st, name, e, c, ctx);
        }
        es = ee;
        cstart = ce;
    }
    st.ev = ev;
    st.cl = cl;
    st.rtmp = rtmp;
}

/// Starts a person's user_id dictionary entry; `flush` then appends that person's
/// rows, which must follow consecutively.
#[inline]
pub(crate) fn begin_person(st: &mut CS, name: Id) {
    let r = &mut st.rows;
    let nb = name.bytes();
    r.dict.extend_from_slice(&(nb.len() as u32).to_le_bytes());
    r.dict.extend_from_slice(nb);
    r.ndict += 1;
    r.runs.push(0);
}

/// One person's sorted events and clicks: sessions, credit, assists, daily rows
/// (rules.md "Events" steps 5 to 7 and "Clicks" steps 4 to 7).
#[inline]
fn person(st: &mut CS, name: Id, ev: &[u64], cl: &[Clk], ctx: &Ctx) {
    let ndays = ctx.ndays;
    begin_person(st, name);
    st.starts.clear();
    st.days.clear();
    let mut k = 0;
    let mut cj = 0usize;
    let mut grp = 0usize;
    let mut cur_day: i64 = -1;
    let (mut us, mut ue, mut ur) = (0u32, 0u32, 0u64);
    st.catrev.clear();
    while k < ev.len() {
        let start = (ev[k] >> 32) as i64;
        let day = (start + ctx.dayoff).div_euclid(DAY);
        if day != cur_day {
            if cur_day >= 0 {
                flush(st, name, cur_day + ctx.day0, us, ue, ur, ctx);
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
        while k < ev.len() && (ev[k] >> 32) as i64 - last <= MIN30 {
            let e = ev[k];
            last = (e >> 32) as i64;
            n += 1;
            let price = e as u32 & 0x00FF_FFFF;
            if price > 0 {
                let cat = e as u32 >> 24;
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
        let camp = if cj > 0 && start - cl[cj - 1].ts as i64 <= WEEK {
            // Latest click wins; among clicks at that same instant, the smallest click_id.
            let mut best = grp;
            for k in grp + 1..cj {
                if cl[k].cid.less(&cl[best].cid) {
                    best = k;
                }
            }
            cl[best].camp
        } else {
            ctx.organic
        };
        let c = &mut st.credit[camp as usize * ndays + day as usize];
        c[0] += 1 + ((purch as u64) << 32);
        c[1] += rev;
        if !cl.is_empty() {
            st.starts.push(start);
            st.days.push(day as u32);
        }
    }
    flush(st, name, cur_day + ctx.day0, us, ue, ur, ctx);

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
                st.assist[camp as usize * ndays + d as usize] += (j - i0) as u32;
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

/// Appends one user_daily row.
#[inline]
fn flush(st: &mut CS, name: Id, day: i64, us: u32, ue: u32, ur: u64, ctx: &Ctx) {
    let mut top = u32::MAX;
    let mut best = 0u64;
    for &(c, r) in &st.catrev {
        if r > best || (r == best && c < top) {
            best = r;
            top = c;
        }
    }
    st.catrev.clear();
    let r = &mut st.rows;
    let _ = name;
    *r.runs.last_mut().unwrap() += 1;
    r.days.push(day as i32);
    r.sessions.push(us as i32);
    r.events.push(ue as i32);
    r.revenue.push(ur as i64);
    if r.n % 8 == 0 {
        r.top_bits.push(0);
    }
    if top != u32::MAX {
        *r.top_bits.last_mut().unwrap() |= 1 << (r.n % 8);
        r.tops.push(top as u16);
    }
    r.n += 1;
}
