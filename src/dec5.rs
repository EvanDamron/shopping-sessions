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

/// Price and category of a valid-looking purchase whose product field is
/// `[s5, e5)` (raw) and price field runs to the line end `len`.
#[inline(never)]
unsafe fn purchase_pc(sh: &Shared, p: *const u8, s5: usize, e5: usize, len: usize) -> Option<u32> {
    let (s6, e6) = uq(p, e5 + 1, len);
    let (s5, e5) = uq(p, s5, e5);
    let v = parse_price(std::slice::from_raw_parts(p.add(s6), e6 - s6))?;
    Some(v | (sh.lk.products.get(Id::field(p, s5, e5)).unwrap_or(sh.lk.unknown_cat) << 24))
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



/// The field `[s, e)` of the line at p without its quotes (the line's quotes are
/// well-formed).
#[inline(always)]
unsafe fn uq(p: *const u8, s: usize, e: usize) -> (usize, usize) {
    let q = (*p.add(s) == b'"') as usize;
    (s + q, e - q)
}



/// Rows of a stretch (at most one per 8 bytes of a stretch of at most 4,223).
const ROWS: usize = 1024;

/// Rows of an event batch, as a struct of arrays.
#[repr(C, align(64))]
struct EBatch {
    kid: [u64; ROWS],
    kb: [u64; ROWS],
    ulo: [u64; ROWS], // the user ID (for a long one: filled in when interned)
    uhi: [u64; ROWS],
    uh: [u64; ROWS],  // its table hash (for a long one: the hash of its bytes, until interned)
    ti: [u64; ROWS],  // t | ingest key << 32
    pc: [u32; ROWS],
    ok: [u8; ROWS],
    long: [(u32, u32, u32); ROWS], // valid rows with a long user ID: (row, field start, length)
    lsx: [usize; ROWS], // well-formed lines of the batch: start
    cmx: [u64; ROWS],   // and structure (`structure`)
    pend: [Pending; ROWS], // valid rows with a long user ID from the batch before
    npend: usize,
}

impl EBatch {
    fn new() -> Box<EBatch> {
        unsafe { Box::new(std::mem::zeroed()) }
    }
    fn row(&self, j: usize) -> ERow {
        ERow {
            kid: self.kid[j],
            kb: self.kb[j],
            ok: self.ok[j] != 0,
            uid: Id { lo: self.ulo[j], hi: self.uhi[j] },
            t: self.ti[j] as u32,
            ing: (self.ti[j] >> 32) as u32,
            pc: self.pc[j],
        }
    }
}

thread_local! {
    static EB: std::cell::RefCell<Box<EBatch>> = std::cell::RefCell::new(EBatch::new());
    static CB: std::cell::RefCell<Box<CBatch>> = std::cell::RefCell::new(CBatch::new());
}

/// Event lines `d[s..e]` (whole lines, 64 readable bytes before and at least 192
/// after): every line parsed and filed as `crate::events` files it.
#[inline(never)]
pub(crate) unsafe fn events(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, e: usize) {
    EB.with(|b| events_in(sh, w, d, s, e, &mut b.borrow_mut()));
}

/// Comma, quote and newline masks of the 128 bytes at p, as (low, high) words.
#[inline(always)]
unsafe fn masks3(p: *const u8) -> ([u64; 2], [u64; 2], [u64; 2]) {
    let (v0, v1, v2, v3) = (ld(p), ld(p.add(32)), ld(p.add(64)), ld(p.add(96)));
    let m = |v: __m256i, x: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(x as i8))) as u32 as u64;
    let w = |x: u8| [m(v0, x) | (m(v1, x) << 32), m(v2, x) | (m(v3, x) << 32)];
    (w(b','), w(b'"'), w(b'\n'))
}

/// Lines per batch: decoded first, filed after (the table lines the filing reads
/// are prefetched by the decoding).
const BATCH: usize = 96;

/// How far ahead of a line the input is prefetched.
const AHEAD: usize = 1024;

/// The plain reading of the event line at `ls` whose newline the window did not
/// reach; returns where the next line starts.
#[cold]
#[inline(never)]
unsafe fn cold_event_from(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize) -> usize {
    let mut le = ls + 128;
    while *d.add(le) != b'\n' {
        le += 1;
    }
    cold_event(sh, w, d, ls, le);
    le + 1
}

/// `(1 << len) - 1` for `len` below 128, as (low, high) words.
static LM2: [[u64; 2]; 128] = {
    let mut t = [[0u64; 2]; 128];
    let mut i = 0;
    while i < 128 {
        let m = (1u128 << i) - 1;
        t[i] = [m as u64, (m >> 64) as u64];
        i += 1;
    }
    t
};

/// The structure of a line of `len` (< 128) bytes from its comma and quote masks,
/// packed: its K commas (bytes 0 to K - 1), its length (byte 6) and which of its
/// fields are quoted (bits 56 and up); None for a malformed line (another comma
/// count, or a quote anywhere but around a whole field). Worked on the two 64-bit
/// halves of the line with the carries between them written out.
#[inline(always)]
unsafe fn structure<const K: usize>(c: [u64; 2], q: [u64; 2], len: usize) -> Option<u64> {
    let [ml, mh] = *LM2.get_unchecked(len);
    let (cl, ch) = (c[0] & ml, c[1] & mh);
    let (ql, qh) = (q[0] & ml, q[1] & mh);
    // Separators: the commas and the newline at `len`.
    let long = ((ml as i64) >> 63) as u64; // all ones when len >= 64
    let (spl, sph) = (cl | ml.wrapping_add(1), ch | (mh.wrapping_add(1) & long));
    // A quote may only open a field (right after a separator or at the line start)
    // or close one (right before a separator); no quote does both, and a field that
    // has an opening quote has a closing one (the opening quote, carried up to the
    // separator that ends its field, lands right after a closing quote).
    let (stl, sth) = ((cl << 1) | 1, (ch << 1) | (cl >> 63)); // field starts
    let (ol, oh) = (ql & stl, qh & sth);
    let (xl, xh) = (ql & ((spl >> 1) | (sph << 63)), qh & (sph >> 1));
    let (al, carry) = (!spl).overflowing_add(ol);
    let ah = (!sph).wrapping_add(oh).wrapping_add(carry as u64);
    let (el, eh) = (al & spl, ah & sph);
    let (x2l, x2h) = (xl << 1, (xh << 1) | (xl >> 63));
    let bad = (ql ^ (ol | xl)) | (qh ^ (oh | xh)) | (ol & xl) | (oh & xh) | (el ^ x2l) | (eh ^ x2h);
    let na = cl.count_ones();
    if (na + ch.count_ones() != K as u32) | (bad != 0) {
        return None;
    }
    // Comma positions as bytes: those of the low half, then those of the high half.
    let (mut m, mut plo) = (cl, 0u64);
    let (mut n, mut phi) = (ch, 0u64);
    for k in 0..K {
        plo |= (m.trailing_zeros() as u64) << (8 * k);
        m &= m.wrapping_sub(1);
        phi |= (n.trailing_zeros() as u64) << (8 * k);
        n &= n.wrapping_sub(1);
    }
    let pos = _bzhi_u64(plo, 8 * na) | ((phi + 0x4040_4040_4040_4040) << (8 * na));
    let pos = _bzhi_u64(pos, 8 * K as u32);
    // Quoted fields: the opening quotes, gathered from the field starts.
    let fq = _pext_u64(ol, stl) | (_pext_u64(oh, sth) << stl.count_ones());
    Some(pos | ((len as u64) << 48) | (fq << 56))
}

#[inline(always)]
unsafe fn events_in(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, end: usize, bt: &mut EBatch) {
    let win = &sh.win;
    let mut ls = s;
    while ls < end {
        let first = ls;
        // ---- Pass 1: line structure (commas, quotes) of up to BATCH lines.
        let mut nv = 0usize; // well-formed lines
        let mut nl = 0usize;
        while ls < end && nl < BATCH {
            nl += 1;
            let p = d.add(ls);
            // The input a few lines ahead: the next line's address is only known at
            // the end of this one's newline search, so its loads cannot start early.
            _mm_prefetch(p.add(AHEAD) as *const i8, _MM_HINT_T0);
            let (c, q, nm) = masks3(p);
            let nm = nm[0] as u128 | ((nm[1] as u128) << 64);
            let len = nm.trailing_zeros() as usize;
            if len >= 128 {
                ls = cold_event_from(sh, w, d, ls);
                continue;
            }
            let lp = ls;
            ls += len + 1;
            if let Some(x) = structure::<6>(c, q, len) {
                *bt.lsx.get_unchecked_mut(nv) = lp;
                *bt.cmx.get_unchecked_mut(nv) = x;
                nv += 1;
            }
        }
        // ---- Pass 2: the fields of the well-formed lines.
        let mut n = 0usize; // rows
        let mut nlong = 0usize;
        for j in 0..nv {
            let lp = *bt.lsx.get_unchecked(j);
            let x = *bt.cmx.get_unchecked(j);
            let p = d.add(lp);
            let cc = |k: u32| ((x >> (8 * k)) & 0xFF) as usize;
            let qq = |k: u32| ((x >> (56 + k)) & 1) as usize;
            let (c0, c1, c2, c3, c4, c5, len) = (cc(0), cc(1), cc(2), cc(3), cc(4), cc(5), cc(6));
            let (s0, e0) = (qq(0), c0 - qq(0));
            if s0 == e0 {
                continue; // empty event_id: dropped before dedup
            }
            let (kid, kb) = Id::key(p, s0, e0).hash2();
            let (s1, e1) = (c0 + 1 + qq(1), c1 - qq(1));
            let ulen = e1 - s1;
            let (s2, e2) = (c1 + 1 + qq(2), c2 - qq(2));
            let (s3, e3) = (c2 + 1 + qq(3), c3 - qq(3));
            let (t, ing_ms, ing) = win.ts_ingest(p.add(s2), e2 - s2, p.add(s3), e3 - s3);
            let (s4, e4) = (c3 + 1 + qq(4), c4 - qq(4));
            let tw = (p.add(s4) as *const u64).read_unaligned();
            let tl = e4 - s4;
            let purchase = (tl == 8) & (tw == PURCHASE);
            let typed = purchase | ((tl == 4) & ((tw as u32 == VIEW) | (tw as u32 == CART)));
            // product_id is not empty: neither `` nor `""`
            let pfull = c5 - c4 - 1 > 2 * qq(5);
            let mut ok = typed
                & (ulen != 0)
                & pfull
                & ((t.wrapping_sub(WEEK) as u64) < sh.evspan)
                & (ing_ms.wrapping_sub(t.wrapping_add(win.base)) <= DAY);
            let mut pc = 0u32;
            if purchase & ok {
                match purchase_pc(sh, p, c4 + 1, c5, len) {
                    Some(v) => pc = v,
                    None => ok = false,
                }
            }
            if ulen <= 15 {
                let uid = Id::short(p.add(s1), ulen);
                let h = uid.hash();
                if ok {
                    sh.links.prefetch2(h);
                }
                *bt.ulo.get_unchecked_mut(n) = uid.lo;
                *bt.uhi.get_unchecked_mut(n) = uid.hi;
                *bt.uh.get_unchecked_mut(n) = h;
            } else if ok {
                let h = Id::long_hash(p.add(s1), ulen);
                Id::prefetch_long(h);
                *bt.uh.get_unchecked_mut(n) = h;
                *bt.long.get_unchecked_mut(nlong) = (n as u32, (lp + s1) as u32, ulen as u32);
                nlong += 1;
            }
            *bt.kid.get_unchecked_mut(n) = kid;
            *bt.kb.get_unchecked_mut(n) = kb;
            *bt.ti.get_unchecked_mut(n) = t as u32 as u64 | (ing as u64) << 32;
            *bt.pc.get_unchecked_mut(n) = pc;
            *bt.ok.get_unchecked_mut(n) = ok as u8;
            n += 1;
        }
        // Valid rows with a long user ID wait one batch: their interned strings are
        // prefetched now (the table slots were, during pass 2), they are interned
        // at the start of the next batch's filing and filed at its end, after their
        // person's table line had time to arrive. (With the self-check on, at once.)
        for &(row, _, _) in bt.long.get_unchecked(..nlong) {
            Id::prefetch_long_string(*bt.uh.get_unchecked(row as usize));
        }
        let np = bt.npend;
        for i in 0..np {
            let pr = bt.pend.get_unchecked_mut(i);
            let uid = Id::intern_long(std::slice::from_raw_parts(d.add(pr.at as usize), pr.len as usize), pr.uh);
            pr.uid = uid;
            pr.uh = uid.hash();
            sh.links.prefetch2(pr.uh);
        }
        let mut li = 0usize;
        for j in 0..n {
            let bucket = (*bt.kb.get_unchecked(j) >> sh.dshift) as usize;
            let ti = *bt.ti.get_unchecked(j);
            let kid = *bt.kid.get_unchecked(j);
            let ing = ti & !0xFFFF_FFFF;
            if *bt.ok.get_unchecked(j) == 0 {
                w.push_key(sh, bucket, kid, ing | NOSLOT as u64);
                continue;
            }
            if li < nlong && bt.long.get_unchecked(li).0 as usize == j {
                li += 1;
                continue; // a long user ID: filed later
            }
            let uid = Id { lo: *bt.ulo.get_unchecked(j), hi: *bt.uhi.get_unchecked(j) };
            let who = sh.links.person(uid, *bt.uh.get_unchecked(j), &mut w.added);
            let slot = w.push_event(sh, (who >> sh.pshift) as usize, PRec { who, t: ti as u32, pc: *bt.pc.get_unchecked(j) });
            w.push_key(sh, bucket, kid, ing | slot as u64);
        }
        for i in 0..np {
            let pr = *bt.pend.get_unchecked(i);
            file_pending(sh, w, &pr);
        }
        // This batch's long rows become the pending ones.
        for (i, &(row, at, len)) in bt.long.get_unchecked(..nlong).iter().enumerate() {
            let j = row as usize;
            *bt.pend.get_unchecked_mut(i) = Pending {
                kid: *bt.kid.get_unchecked(j),
                kb: *bt.kb.get_unchecked(j),
                ti: *bt.ti.get_unchecked(j),
                uh: *bt.uh.get_unchecked(j),
                uid: Id::EMPTY,
                pc: *bt.pc.get_unchecked(j),
                at,
                len,
            };
        }
        bt.npend = nlong;
        if sh.deccheck {
            // Resolve this batch's long rows now, so the check sees their IDs.
            for i in 0..nlong {
                let pr = bt.pend.get_unchecked_mut(i);
                let uid = Id::intern_long(std::slice::from_raw_parts(d.add(pr.at as usize), pr.len as usize), pr.uh);
                let row = bt.long.get_unchecked(i).0 as usize;
                *bt.ulo.get_unchecked_mut(row) = uid.lo;
                *bt.uhi.get_unchecked_mut(row) = uid.hi;
            }
        }
        w.nfast += n as u64;
        if sh.deccheck {
            check_batch_events(sh, d, first, ls, bt, n);
        }
    }
    // The piece ends: the pending rows are filed now (their bytes are in it).
    for i in 0..bt.npend {
        let pr = bt.pend.get_unchecked_mut(i);
        let uid = Id::intern_long(std::slice::from_raw_parts(d.add(pr.at as usize), pr.len as usize), pr.uh);
        pr.uid = uid;
        pr.uh = uid.hash();
        let pr = *pr;
        file_pending(sh, w, &pr);
    }
    bt.npend = 0;
}

/// A valid event row with a long user ID, waiting to be filed.
#[derive(Clone, Copy)]
struct Pending {
    kid: u64,
    kb: u64,
    ti: u64,
    uh: u64, // the hash of the ID's bytes; once interned, the ID's table hash
    uid: Id,
    pc: u32,
    at: u32,
    len: u32,
}

/// Files a pending row (its ID interned) as `crate::events` files a valid event.
#[inline(always)]
unsafe fn file_pending(sh: &Shared, w: &mut Worker, pr: &Pending) {
    let bucket = (pr.kb >> sh.dshift) as usize;
    let who = sh.links.person(pr.uid, pr.uh, &mut w.added);
    let slot = w.push_event(sh, (who >> sh.pshift) as usize, PRec { who, t: pr.ti as u32, pc: pr.pc });
    w.push_key(sh, bucket, pr.kid, (pr.ti & !0xFFFF_FFFF) | slot as u64);
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

/// Test hook: every line of a batch `d[a..b)`, decoded rows and dropped lines
/// alike, against the plain reading.
#[cold]
#[inline(never)]
unsafe fn check_batch_events(sh: &Shared, d: *const u8, a: usize, b: usize, bt: &EBatch, n: usize) {
    let mut ls = a;
    let mut j = 0;
    while ls < b {
        let mut le = ls;
        while *d.add(le) != b'\n' {
            le += 1;
        }
        if le - ls < 128 && plain_fields::<7>(d, ls, le).and_then(|f| event_row(sh, d, &f)).is_some() {
            if j >= n {
                eprintln!("decoder kept fewer rows of a batch than the plain reading");
                std::process::exit(3);
            }
            check_event(sh, d, ls, le, &bt.row(j));
            j += 1;
        }
        ls = le + 1;
    }
    if j != n {
        eprintln!("decoder kept {} rows of a batch, the plain reading {}", n, j);
        std::process::exit(3);
    }
}

/// Rows of a click batch.
#[repr(C, align(64))]
struct CBatch {
    ulo: [u64; ROWS],
    uhi: [u64; ROWS],
    uh: [u64; ROWS],
    clo: [u64; ROWS],
    chi: [u64; ROWS],
    tc: [u64; ROWS], // ts | campaign << 32
    lu: [(u32, u32, u32); ROWS], // rows with a long user ID: (row, field start, length)
    lc: [(u32, u32, u32); ROWS], // rows with a long click ID
    lsx: [usize; ROWS],
    cmx: [u64; ROWS],
}

impl CBatch {
    fn new() -> Box<CBatch> {
        unsafe { Box::new(std::mem::zeroed()) }
    }
    fn row(&self, j: usize) -> CRow {
        CRow {
            uid: Id { lo: self.ulo[j], hi: self.uhi[j] },
            ts: self.tc[j] as u32,
            camp: (self.tc[j] >> 32) as u32,
            cid: Id { lo: self.clo[j], hi: self.chi[j] },
        }
    }
}

/// Campaign index by number, for the campaigns named `c_` + a number written
/// without leading zeros (u32::MAX: no such campaign); built once from the lookups.
static CAMPS: std::sync::OnceLock<Vec<u32>> = std::sync::OnceLock::new();

fn camps(sh: &Shared) -> &'static [u32] {
    CAMPS.get_or_init(|| {
        let mut t = Vec::new();
        for (id, v) in sh.lk.campaigns.entries() {
            let b = id.bytes();
            if b.len() < 3 || b.len() > 9 || &b[..2] != b"c_" || !b[2..].iter().all(u8::is_ascii_digit) || (b[2] == b'0' && b.len() > 3) {
                continue;
            }
            let n = b[2..].iter().fold(0usize, |n, &c| n * 10 + (c - b'0') as usize);
            if n >= t.len() {
                t.resize(n + 1, u32::MAX);
            }
            t[n] = v;
        }
        t
    })
}

/// The campaign of the field `[s, e)` of the line at p, as `lk.campaigns.get` gives
/// it: a field `c_` + 1 to 7 digits without a leading zero is looked up by number,
/// anything else in the map.
#[inline(always)]
unsafe fn campaign(sh: &Shared, tab: &[u32], p: *const u8, s: usize, e: usize) -> Option<u32> {
    let n = e - s;
    let nd = n.wrapping_sub(2);
    if (nd.wrapping_sub(1) < 7) && ((p.add(s) as *const u16).read_unaligned() == u16::from_le_bytes(*b"c_")) && ((*p.add(s + 2) != b'0') | (nd == 1)) {
        // The digits are the top `nd` bytes of the 8 that end at e; '0's fill the rest.
        let keep = u64::MAX << (8 * (8 - nd));
        let w = ((p.add(e - 8) as *const u64).read_unaligned() & keep) | (0x3030_3030_3030_3030 & !keep);
        let x = w ^ 0x3030_3030_3030_3030;
        if (x.wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080 == 0 {
            let v = x.wrapping_mul(10).wrapping_add(x >> 8);
            let v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32)))
                .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32)));
            let num = (v >> 32) as usize;
            let c = *tab.get(num).unwrap_or(&u32::MAX);
            return (c != u32::MAX).then_some(c);
        }
    }
    sh.lk.campaigns.get(Id::field(p, s, e))
}

/// Click lines `d[s..e]`: every line parsed and filed as `crate::clicks` files it.
#[inline(never)]
pub(crate) unsafe fn clicks(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, e: usize) {
    CB.with(|b| clicks_in(sh, w, d, s, e, &mut b.borrow_mut()));
}

/// The plain reading of the click line at `ls` whose newline the window did not
/// reach; returns where the next line starts.
#[cold]
#[inline(never)]
unsafe fn cold_click_from(sh: &Shared, w: &mut Worker, d: *const u8, ls: usize) -> usize {
    let mut le = ls + 128;
    while *d.add(le) != b'\n' {
        le += 1;
    }
    cold_click(sh, w, d, ls, le);
    le + 1
}

#[inline(always)]
unsafe fn clicks_in(sh: &Shared, w: &mut Worker, d: *const u8, s: usize, end: usize, bt: &mut CBatch) {
    let win = &sh.win;
    let tab = camps(sh);
    let mut ls = s;
    while ls < end {
        let first = ls;
        // ---- Pass 1: line structure.
        let mut nv = 0usize;
        let mut nl = 0usize;
        while ls < end && nl < BATCH {
            nl += 1;
            let p = d.add(ls);
            _mm_prefetch(p.add(AHEAD) as *const i8, _MM_HINT_T0);
            let (c, q, nm) = masks3(p);
            let nm = nm[0] as u128 | ((nm[1] as u128) << 64);
            let len = nm.trailing_zeros() as usize;
            if len >= 128 {
                ls = cold_click_from(sh, w, d, ls);
                continue;
            }
            let lp = ls;
            ls += len + 1;
            if let Some(x) = structure::<3>(c, q, len) {
                *bt.lsx.get_unchecked_mut(nv) = lp;
                *bt.cmx.get_unchecked_mut(nv) = x;
                nv += 1;
            }
        }
        // ---- Pass 2: fields.
        let (mut n, mut nlu, mut nlc) = (0usize, 0usize, 0usize);
        for j in 0..nv {
            let lp = *bt.lsx.get_unchecked(j);
            let x = *bt.cmx.get_unchecked(j);
            let p = d.add(lp);
            let cc = |k: u32| ((x >> (8 * k)) & 0xFF) as usize;
            let qq = |k: u32| ((x >> (56 + k)) & 1) as usize;
            let (c0, c1, c2, len) = (cc(0), cc(1), cc(2), cc(6));
            let (s0, e0) = (qq(0), c0 - qq(0));
            let (s1, e1) = (c0 + 1 + qq(1), c1 - qq(1));
            let (clen, ulen) = (e0 - s0, e1 - s1);
            if (clen == 0) | (ulen == 0) {
                continue;
            }
            let Some(camp) = campaign(sh, tab, p, c1 + 1 + qq(2), c2 - qq(2)) else {
                continue;
            };
            let (s3, e3) = (c2 + 1 + qq(3), len - qq(3));
            let t = win.ts(p, s3, e3);
            if t as u64 >= win.span as u64 {
                continue;
            }
            if ulen <= 15 {
                let uid = Id::short(p.add(s1), ulen);
                let h = uid.hash();
                sh.links.prefetch2(h);
                *bt.ulo.get_unchecked_mut(n) = uid.lo;
                *bt.uhi.get_unchecked_mut(n) = uid.hi;
                *bt.uh.get_unchecked_mut(n) = h;
            } else {
                let h = Id::long_hash(p.add(s1), ulen);
                Id::prefetch_long(h);
                *bt.uh.get_unchecked_mut(n) = h;
                *bt.lu.get_unchecked_mut(nlu) = (n as u32, (lp + s1) as u32, ulen as u32);
                nlu += 1;
            }
            if clen <= 15 {
                let cid = Id::short(p.add(s0), clen);
                *bt.clo.get_unchecked_mut(n) = cid.lo;
                *bt.chi.get_unchecked_mut(n) = cid.hi;
            } else {
                let h = Id::long_hash(p.add(s0), clen);
                Id::prefetch_long(h);
                *bt.clo.get_unchecked_mut(n) = h; // until interned
                *bt.lc.get_unchecked_mut(nlc) = (n as u32, (lp + s0) as u32, clen as u32);
                nlc += 1;
            }
            *bt.tc.get_unchecked_mut(n) = t as u32 as u64 | (camp as u64) << 32;
            n += 1;
        }
        for &(row, at, len) in bt.lu.get_unchecked(..nlu) {
            let row = row as usize;
            let uid = Id::intern_long(std::slice::from_raw_parts(d.add(at as usize), len as usize), *bt.uh.get_unchecked(row));
            let h = uid.hash();
            *bt.ulo.get_unchecked_mut(row) = uid.lo;
            *bt.uhi.get_unchecked_mut(row) = uid.hi;
            *bt.uh.get_unchecked_mut(row) = h;
            sh.links.prefetch2(h);
        }
        for &(row, at, len) in bt.lc.get_unchecked(..nlc) {
            let row = row as usize;
            let cid = Id::intern_long(std::slice::from_raw_parts(d.add(at as usize), len as usize), *bt.clo.get_unchecked(row));
            *bt.clo.get_unchecked_mut(row) = cid.lo;
            *bt.chi.get_unchecked_mut(row) = cid.hi;
        }
        for j in 0..n {
            let uid = Id { lo: *bt.ulo.get_unchecked(j), hi: *bt.uhi.get_unchecked(j) };
            let who = sh.links.person(uid, *bt.uh.get_unchecked(j), &mut w.added);
            let tc = *bt.tc.get_unchecked(j);
            let cid = Id { lo: *bt.clo.get_unchecked(j), hi: *bt.chi.get_unchecked(j) };
            w.push_click(sh, (who >> sh.pshift) as usize, CRec { who, ts: tc as u32, camp: (tc >> 32) as u32, _pad: 0, cid });
        }
        w.ncfast += n as u64;
        if sh.deccheck {
            check_batch_clicks(sh, d, first, ls, bt, n);
        }
    }
}

/// Test hook: every click line of a batch `d[a..b)` against the plain reading.
#[cold]
#[inline(never)]
unsafe fn check_batch_clicks(sh: &Shared, d: *const u8, a: usize, b: usize, bt: &CBatch, n: usize) {
    let mut ls = a;
    let mut j = 0;
    while ls < b {
        let mut le = ls;
        while *d.add(le) != b'\n' {
            le += 1;
        }
        if le - ls < 128 {
            if let Some(p) = plain_fields::<4>(d, ls, le).and_then(|f| click_row(sh, d, &f)) {
                let mine = (j < n).then(|| bt.row(j));
                if mine != Some(p) {
                    eprintln!("click decoder differs from the plain reading on: {:?}\n  decoder {:?}\n  plain   {:?}", String::from_utf8_lossy(std::slice::from_raw_parts(d.add(ls), le - ls)), mine, p);
                    std::process::exit(3);
                }
                j += 1;
            }
        }
        ls = le + 1;
    }
    if j != n {
        eprintln!("click decoder kept {} rows of a batch, the plain reading {}", n, j);
        std::process::exit(3);
    }
}

