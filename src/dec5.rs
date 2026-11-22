//! Per-line decoders for event and click lines with opaque IDs (scorer v5 data).
//!
//! No separator index is written. A piece of whole lines is read in two steps:
//!
//!   newlines   64 bytes at a time, the newline positions of a stretch of the piece
//!              go into a small array (two written unconditionally per block, a loop
//!              only for blocks with more);
//!   lines      each line of that stretch is looked at once: four 32-byte loads from
//!              its start give its comma and quote masks (a line of up to 127 bytes,
//!              which is all but a few in a thousand), the quotes are checked with
//!              bit arithmetic on those masks, the fields come out of the comma mask,
//!              and every field is converted with straight-line code.
//!
//! IDs are opaque strings. An ID of up to 15 bytes becomes its inline `Id` on the
//! spot; a longer one is hashed once and its long-ID table slot prefetched, and is
//! interned when the batch is filed. The rows of a batch gather in a struct of
//! arrays; filing them (person lookup, the record, the dedup key) is a second pass,
//! so the table lines it reads were prefetched one batch-pass earlier.
//!
//! A line the window cannot hold (128 bytes or more) goes to `cold`, which reads it
//! the plain way (`split`) and decides it with `event_row` / `click_row`, the same
//! per-line rules the general pipeline applies. A line with the wrong number of
//! commas or a quote out of place is malformed and dropped here: the window holds
//! the whole line, so that is certain. `SS_DECCHECK=1` re-reads every line the
//! decoder takes the plain way and compares the decisions.

use crate::id::Id;
use crate::{parse_price, plain_fields, CRec, PRec, Shared, Worker, CART, DAY, NOSLOT, PURCHASE, VIEW, WEEK};
use core::arch::x86_64::*;

/// Bytes of a piece whose newlines are found in one go (a batch is the lines that
/// end in such a stretch).
const STRETCH: usize = 4096;

/// What the general rules make of one event line (rules.md "Events", steps 1, 3, 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct ERow {
    pub kid: u64, // dedup identity
    pub kb: u64,  // dedup bucket hash
    pub ok: bool, // valid, in the window and not late
    pub uid: Id,
    pub t: u32,
    pub ing: u32, // ingest key
    pub pc: u32,
}

/// What the general rules make of one click line: (person bucket input) the user ID,
/// time, campaign and click ID of a valid click.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct CRow {
    pub uid: Id,
    pub ts: u32,
    pub camp: u32,
    pub cid: Id,
}

/// One event line, given its seven fields without their quotes: the rules exactly as
/// `crate::events` applies them. None for an empty event_id.
#[inline(always)]
pub(crate) unsafe fn event_row(sh: &Shared, d: *const u8, f: &[(usize, usize); 7]) -> Option<ERow> {
    let win = &sh.win;
    let (s0, e0) = f[0];
    if s0 == e0 {
        return None;
    }
    let (kid, kb) = Id::key(d, s0, e0).hash2();
    let uid = Id::field(d, f[1].0, f[1].1);
    let t = win.ts(d, f[2].0, f[2].1);
    let (ing_ms, ing) = win.ingest(d, f[3].0, f[3].1);
    let (s4, e4) = f[4];
    let tw = (d.add(s4) as *const u64).read_unaligned();
    let tl = e4 - s4;
    let purchase = (tl == 8) & (tw == PURCHASE);
    let typed = purchase | ((tl == 4) & ((tw as u32 == VIEW) | (tw as u32 == CART)));
    let (s5, e5) = f[5];
    let mut ok = typed
        & (uid != Id::EMPTY)
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
    Some(ERow { kid, kb, ok, uid, t: t as u32, ing, pc })
}

/// One click line, given its four fields without their quotes: the rules exactly as
/// `crate::clicks` applies them. None for a click that is dropped.
#[inline(always)]
pub(crate) unsafe fn click_row(sh: &Shared, d: *const u8, f: &[(usize, usize); 4]) -> Option<CRow> {
    let win = &sh.win;
    let (s0, e0) = f[0];
    let (s1, e1) = f[1];
    if s0 == e0 || s1 == e1 {
        return None;
    }
    let camp = sh.lk.campaigns.get(Id::field(d, f[2].0, f[2].1))?;
    let t = win.ts(d, f[3].0, f[3].1);
    if t as u64 >= win.span as u64 {
        return None;
    }
    Some(CRow { uid: Id::field(d, s1, e1), ts: t as u32, camp, cid: Id::field(d, s0, e0) })
}

/// Files an event row as `crate::events` does.
#[inline(always)]
unsafe fn file_event(sh: &Shared, w: &mut Worker, r: &ERow, uh: u64) {
    let bucket = (r.kb >> sh.dshift) as usize;
    if !r.ok {
        w.push_key(sh, bucket, r.kid, ((r.ing as u64) << 32) | NOSLOT as u64);
        return;
    }
    let who = sh.links.person(r.uid, uh, &mut w.added);
    let slot = w.push_event(sh, (who >> sh.pshift) as usize, PRec { who, t: r.t, pc: r.pc });
    w.push_key(sh, bucket, r.kid, ((r.ing as u64) << 32) | slot as u64);
}

/// Files a click row as `crate::clicks` does.
#[inline(always)]
unsafe fn file_click(sh: &Shared, w: &mut Worker, r: &CRow, uh: u64) {
    let who = sh.links.person(r.uid, uh, &mut w.added);
    w.push_click(sh, (who >> sh.pshift) as usize, CRec { who, ts: r.ts, camp: r.camp, _pad: 0, cid: r.cid });
}

/// The plain reading of an event line the window does not hold.
#[cold]
#[inline(never)]
unsafe fn cold_event(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) {
    w.ncold += 1;
    if let Some(f) = plain_fields::<7>(d, ls, le) {
        if let Some(r) = event_row(sh, d, &f) {
            file_event(sh, w, &r, r.uid.hash());
        }
    }
}

/// The plain reading of a click line the window does not hold.
#[cold]
#[inline(never)]
unsafe fn cold_click(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize, le: usize) {
    w.nccold += 1;
    if let Some(f) = plain_fields::<4>(d, ls, le) {
        if let Some(r) = click_row(sh, d, &f) {
            file_click(sh, w, &r, r.uid.hash());
        }
    }
}

#[inline(always)]
unsafe fn ld(p: *const u8) -> __m256i {
    _mm256_loadu_si256(p as *const __m256i)
}

/// Comma and quote masks of the 128 bytes at p.
#[inline(always)]
unsafe fn cq128(p: *const u8) -> (u128, u128) {
    let (v0, v1, v2, v3) = (ld(p), ld(p.add(32)), ld(p.add(64)), ld(p.add(96)));
    let c = _mm256_set1_epi8(b',' as i8);
    let q = _mm256_set1_epi8(b'"' as i8);
    let m = |v: __m256i, x: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, x)) as u32 as u64;
    let cl = m(v0, c) | (m(v1, c) << 32);
    let ch = m(v2, c) | (m(v3, c) << 32);
    let ql = m(v0, q) | (m(v1, q) << 32);
    let qh = m(v2, q) | (m(v3, q) << 32);
    (cl as u128 | ((ch as u128) << 64), ql as u128 | ((qh as u128) << 64))
}

/// Newline mask of the 64 bytes at p.
#[inline(always)]
unsafe fn nl64(p: *const u8) -> u64 {
    let n = _mm256_set1_epi8(b'\n' as i8);
    let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(ld(p), n)) as u32 as u64;
    let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(ld(p.add(32)), n)) as u32 as u64;
    a | (b << 32)
}

/// The line of `len` (< 128) bytes at p, split at its `N - 1` commas: field bounds
/// relative to p with their quotes removed; None for a malformed line (wrong comma
/// count, or a quote anywhere but around a whole field).
#[inline(always)]
unsafe fn split_window<const N: usize>(p: *const u8, len: usize) -> Option<[(usize, usize); N]> {
    let (c, q) = cq128(p);
    let lm = (1u128 << len) - 1;
    let (c, q) = (c & lm, q & lm);
    if c.count_ones() as usize != N - 1 {
        return None;
    }
    let s = c | (1u128 << len);
    // A quote may only open a field (right after a separator or at the line start)
    // or close one (right before a separator); no quote does both, and a field that
    // has an opening quote has a closing one (the opening quote, carried up to the
    // separator that ends its field, lands right after a closing quote).
    let open = q & ((s << 1) | 1);
    let close = q & (s >> 1);
    let ends_open = (!s).wrapping_add(open) & s;
    if (q & !(open | close)) | (open & close) | (ends_open ^ (close << 1)) != 0 {
        return None;
    }
    let mut f = [(0usize, 0usize); N];
    let mut m = c;
    let mut st = 0usize;
    for (k, g) in f.iter_mut().enumerate() {
        let e = if k + 1 < N { m.trailing_zeros() as usize } else { len };
        m &= m.wrapping_sub(1);
        let qd = (*p.add(st) == b'"') as usize;
        *g = (st + qd, e - qd);
        st = e + 1;
    }
    Some(f)
}

/// Finds the newlines of `d[a..b)` (b is past a newline), appending their positions
/// to `out` from `n`; returns the new count. `out` needs room for `b - a + 4`.
#[inline(always)]
unsafe fn newlines(d: *const u8, a: usize, b: usize, out: *mut u32, mut n: usize) -> usize {
    let mut o = a;
    while o < b {
        let mut m = nl64(d.add(o));
        if b - o < 64 {
            m &= (1u64 << (b - o)) - 1;
        }
        let cnt = m.count_ones() as usize;
        let base = o as u32;
        *out.add(n) = base + m.trailing_zeros();
        m &= m.wrapping_sub(1);
        *out.add(n + 1) = base + m.trailing_zeros();
        if cnt > 2 {
            m &= m - 1;
            let mut i = n + 2;
            while m != 0 {
                *out.add(i) = base + m.trailing_zeros();
                m &= m - 1;
                i += 1;
            }
        }
        n += cnt;
        o += 64;
    }
    n
}

/// Rows of an event batch, as a struct of arrays.
struct EBatch {
    r: Vec<ERow>,
    uh: Vec<u64>,      // table hash of the user ID (for a long one: of its bytes, until interned)
    long: Vec<(u32, u32, u8)>, // valid rows whose user ID is long: (row, field start, length)
    nl: Vec<u32>,
}

impl EBatch {
    fn new() -> EBatch {
        EBatch { r: Vec::with_capacity(STRETCH), uh: Vec::with_capacity(STRETCH), long: Vec::with_capacity(STRETCH), nl: vec![0; STRETCH + 8] }
    }
}

thread_local! {
    static EB: std::cell::RefCell<EBatch> = std::cell::RefCell::new(EBatch::new());
    static CB: std::cell::RefCell<CBatch> = std::cell::RefCell::new(CBatch::new());
}

/// Event lines `d[s..e]` (whole lines, 64 readable bytes before and at least 192
/// after): every line parsed and filed as `crate::events` files it.
#[inline(never)]
pub(crate) unsafe fn events(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, e: usize) {
    EB.with(|b| events_in(sh, w, d, s, e, &mut b.borrow_mut()));
}

#[inline(always)]
unsafe fn events_in(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, end: usize, bt: &mut EBatch) {
    let win = &sh.win;
    let mut a = s;
    while a < end {
        let b = (a + STRETCH).min(end);
        // The stretch ends at a line end: the last newline at or after b - 1.
        let nn = newlines(d, a, b, bt.nl.as_mut_ptr(), 0);
        let nlp = bt.nl.as_ptr();
        let (nn, b) = if nn > 0 && *nlp.add(nn - 1) as usize == b - 1 {
            (nn, b)
        } else {
            // Extend to the next newline (the piece ends with one).
            let mut le = b;
            while *d.add(le) != b'\n' {
                le += 1;
            }
            *bt.nl.as_mut_ptr().add(nn) = le as u32;
            (nn + 1, le + 1)
        };
        let nlp = bt.nl.as_ptr();
        bt.r.clear();
        bt.uh.clear();
        bt.long.clear();
        let mut ls = a;
        for k in 0..nn {
            let le = *nlp.add(k) as usize;
            let len = le - ls;
            let p = d.add(ls);
            if len >= 128 {
                cold_event(sh, w, d, ls, le);
                ls = le + 1;
                continue;
            }
            let Some(f) = split_window::<7>(p, len) else {
                ls = le + 1;
                continue; // malformed: dropped
            };
            let (s0, e0) = f[0];
            if s0 == e0 {
                ls = le + 1;
                continue; // empty event_id: dropped before dedup
            }
            let (kid, kb) = Id::key(p, s0, e0).hash2();
            let (s1, e1) = f[1];
            let ulen = e1 - s1;
            let t = win.ts(p, f[2].0, f[2].1);
            let (ing_ms, ing) = win.ingest(p, f[3].0, f[3].1);
            let (s4, e4) = f[4];
            let tw = (p.add(s4) as *const u64).read_unaligned();
            let tl = e4 - s4;
            let purchase = (tl == 8) & (tw == PURCHASE);
            let typed = purchase | ((tl == 4) & ((tw as u32 == VIEW) | (tw as u32 == CART)));
            let (s5, e5) = f[5];
            let mut ok = typed
                & (ulen != 0)
                & (e5 > s5)
                & ((t.wrapping_sub(WEEK) as u64) < sh.evspan)
                & (ing_ms.wrapping_sub(t.wrapping_add(win.base)) <= DAY);
            let mut pc = 0u32;
            if purchase & ok {
                let (s6, e6) = f[6];
                match parse_price(std::slice::from_raw_parts(p.add(s6), e6 - s6)) {
                    Some(v) => pc = v | (sh.lk.products.get(Id::field(p, s5, e5)).unwrap_or(sh.lk.unknown_cat) << 24),
                    None => ok = false,
                }
            }
            let row = bt.r.len();
            let (uid, uh) = if ulen <= 15 {
                let uid = Id::short(p.add(s1), ulen);
                let h = uid.hash();
                if ok {
                    sh.links.prefetch(h);
                }
                (uid, h)
            } else {
                let h = Id::long_hash(p.add(s1), ulen);
                if ok {
                    Id::prefetch_long(h);
                    bt.long.push((row as u32, (ls + s1) as u32, ulen as u8));
                }
                (Id::EMPTY, h)
            };
            bt.r.push(ERow { kid, kb, ok, uid, t: t as u32, ing, pc });
            bt.uh.push(uh);
            ls = le + 1;
        }
        // Long user IDs of valid rows: interned (their table slots were prefetched),
        // then their person's table line prefetched.
        for &(row, at, len) in bt.long.iter() {
            let row = row as usize;
            let r = bt.r.get_unchecked_mut(row);
            r.uid = Id::intern_long(std::slice::from_raw_parts(d.add(at as usize), len as usize), *bt.uh.get_unchecked(row));
            let h = r.uid.hash();
            *bt.uh.get_unchecked_mut(row) = h;
            sh.links.prefetch(h);
        }
        for j in 0..bt.r.len() {
            let r = *bt.r.get_unchecked(j);
            file_event(sh, w, &r, *bt.uh.get_unchecked(j));
        }
        w.nfast += bt.r.len() as u64;
        if sh.deccheck {
            check_batch_events(sh, d, a, nn, nlp, bt);
        }
        a = b;
    }
}

/// Test hook: a row the decoder made, against the plain reading of its line.
#[cold]
#[inline(never)]
unsafe fn check_event(sh: &Shared, d: *const u8, ls: usize, le: usize, r: &ERow) {
    let plain = plain_fields::<7>(d, ls, le).and_then(|f| event_row(sh, d, &f));
    let same = match plain {
        Some(p) => p.kid == r.kid && p.kb == r.kb && p.ok == r.ok && p.ing == r.ing && (!p.ok || (p.uid == r.uid && p.t == r.t && p.pc == r.pc)),
        None => false,
    };
    if !same {
        eprintln!("decoder differs from the plain reading on: {:?}\n  decoder {:?}\n  plain   {:?}", String::from_utf8_lossy(std::slice::from_raw_parts(d.add(ls), le - ls)), r, plain);
        std::process::exit(3);
    }
}

/// Test hook: every line of a stretch, decoded rows and dropped lines alike, against
/// the plain reading.
#[cold]
#[inline(never)]
unsafe fn check_batch_events(sh: &Shared, d: *const u8, a: usize, nn: usize, nlp: *const u32, bt: &EBatch) {
    let mut ls = a;
    let mut j = 0;
    for k in 0..nn {
        let le = *nlp.add(k) as usize;
        if le - ls < 128 {
            let plain = plain_fields::<7>(d, ls, le).and_then(|f| event_row(sh, d, &f));
            match plain {
                None => {}
                Some(_) => {
                    check_event(sh, d, ls, le, &bt.r[j]);
                    j += 1;
                }
            }
        }
        ls = le + 1;
    }
    if j != bt.r.len() {
        eprintln!("decoder kept {} rows of a stretch, the plain reading {}", bt.r.len(), j);
        std::process::exit(3);
    }
}

/// Rows of a click batch.
struct CBatch {
    r: Vec<CRow>,
    uh: Vec<u64>,
    lu: Vec<(u32, u32, u8)>, // rows with a long user ID: (row, field start, length)
    lc: Vec<(u32, u32, u8)>, // rows with a long click ID
    nl: Vec<u32>,
}

impl CBatch {
    fn new() -> CBatch {
        CBatch { r: Vec::with_capacity(STRETCH), uh: Vec::with_capacity(STRETCH), lu: Vec::with_capacity(STRETCH), lc: Vec::with_capacity(STRETCH), nl: vec![0; STRETCH + 8] }
    }
}

/// Click lines `d[s..e]`: every line parsed and filed as `crate::clicks` files it.
#[inline(never)]
pub(crate) unsafe fn clicks(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, e: usize) {
    CB.with(|b| clicks_in(sh, w, d, s, e, &mut b.borrow_mut()));
}

#[inline(always)]
unsafe fn clicks_in(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, end: usize, bt: &mut CBatch) {
    let win = &sh.win;
    let mut a = s;
    while a < end {
        let b = (a + STRETCH).min(end);
        let nn = newlines(d, a, b, bt.nl.as_mut_ptr(), 0);
        let nlp = bt.nl.as_ptr();
        let (nn, b) = if nn > 0 && *nlp.add(nn - 1) as usize == b - 1 {
            (nn, b)
        } else {
            let mut le = b;
            while *d.add(le) != b'\n' {
                le += 1;
            }
            *bt.nl.as_mut_ptr().add(nn) = le as u32;
            (nn + 1, le + 1)
        };
        let nlp = bt.nl.as_ptr();
        bt.r.clear();
        bt.uh.clear();
        bt.lu.clear();
        bt.lc.clear();
        let mut ls = a;
        for k in 0..nn {
            let le = *nlp.add(k) as usize;
            let len = le - ls;
            let p = d.add(ls);
            if len >= 128 {
                cold_click(sh, w, d, ls, le);
                ls = le + 1;
                continue;
            }
            let Some(f) = split_window::<4>(p, len) else {
                ls = le + 1;
                continue;
            };
            let (s0, e0) = f[0];
            let (s1, e1) = f[1];
            let (clen, ulen) = (e0 - s0, e1 - s1);
            if clen == 0 || ulen == 0 {
                ls = le + 1;
                continue;
            }
            let Some(camp) = sh.lk.campaigns.get(Id::field(p, f[2].0, f[2].1)) else {
                ls = le + 1;
                continue;
            };
            let t = win.ts(p, f[3].0, f[3].1);
            if t as u64 >= win.span as u64 {
                ls = le + 1;
                continue;
            }
            let row = bt.r.len() as u32;
            let (uid, uh) = if ulen <= 15 {
                let uid = Id::short(p.add(s1), ulen);
                let h = uid.hash();
                sh.links.prefetch(h);
                (uid, h)
            } else {
                let h = Id::long_hash(p.add(s1), ulen);
                Id::prefetch_long(h);
                bt.lu.push((row, (ls + s1) as u32, ulen as u8));
                (Id::EMPTY, h)
            };
            let cid = if clen <= 15 {
                Id::short(p.add(s0), clen)
            } else {
                bt.lc.push((row, (ls + s0) as u32, clen as u8));
                Id::prefetch_long(Id::long_hash(p.add(s0), clen));
                Id::EMPTY
            };
            bt.r.push(CRow { uid, ts: t as u32, camp, cid });
            bt.uh.push(uh);
            ls = le + 1;
        }
        for &(row, at, len) in bt.lu.iter() {
            let row = row as usize;
            let r = bt.r.get_unchecked_mut(row);
            r.uid = Id::intern_long(std::slice::from_raw_parts(d.add(at as usize), len as usize), *bt.uh.get_unchecked(row));
            let h = r.uid.hash();
            *bt.uh.get_unchecked_mut(row) = h;
            sh.links.prefetch(h);
        }
        for &(row, at, len) in bt.lc.iter() {
            let b = std::slice::from_raw_parts(d.add(at as usize), len as usize);
            bt.r.get_unchecked_mut(row as usize).cid = Id::intern_long(b, Id::long_hash(b.as_ptr(), b.len()));
        }
        for j in 0..bt.r.len() {
            let r = *bt.r.get_unchecked(j);
            file_click(sh, w, &r, *bt.uh.get_unchecked(j));
        }
        w.ncfast += bt.r.len() as u64;
        if sh.deccheck {
            check_batch_clicks(sh, d, a, nn, nlp, bt);
        }
        a = b;
    }
}

/// Test hook: every click line of a stretch against the plain reading.
#[cold]
#[inline(never)]
unsafe fn check_batch_clicks(sh: &Shared, d: *const u8, a: usize, nn: usize, nlp: *const u32, bt: &CBatch) {
    let mut ls = a;
    let mut j = 0;
    for k in 0..nn {
        let le = *nlp.add(k) as usize;
        if le - ls < 128 {
            if let Some(p) = plain_fields::<4>(d, ls, le).and_then(|f| click_row(sh, d, &f)) {
                if bt.r.get(j) != Some(&p) {
                    eprintln!("click decoder differs from the plain reading on: {:?}\n  decoder {:?}\n  plain   {:?}", String::from_utf8_lossy(std::slice::from_raw_parts(d.add(ls), le - ls)), bt.r.get(j), p);
                    std::process::exit(3);
                }
                j += 1;
            }
        }
        ls = le + 1;
    }
    if j != bt.r.len() {
        eprintln!("click decoder kept {} rows of a stretch, the plain reading {}", bt.r.len(), j);
        std::process::exit(3);
    }
}

