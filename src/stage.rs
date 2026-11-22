//! The event stage of the index-order fast path: everything between phase A and the
//! general pipeline's phases B and C.
//!
//! Phase A (main.rs, fast.rs) leaves every event line with a generator-shaped event ID
//! as a 24-byte `XRec` in the bucket of its event number, and every click with a
//! generator-shaped user ID as a `CRec` in the bucket of its user number. This module
//! turns those into rows and campaign counts in one pass per bucket:
//!
//!   place     each record goes to the slot of its event number in a table that stays
//!             in cache; of two records on one slot (copies of one event) the smaller
//!             ingest_ts stays: that is the dedup
//!   read out  the slots in order, valid events only, as three dense columns
//!             (time, user, price)
//!   cut       compares of each event with the one before it: where the user number
//!             changes a run ends, where the gap is over 30 minutes a session ends
//!   runs      a run that is provably one whole person becomes rows and campaign
//!             counts straight from its session list and the person's clicks
//!
//! Nothing is assumed about the data. Every property the shortcut relies on is checked
//! on the data itself, and what fails a check takes the general pipeline:
//!   * a run whose user number the link files tie to other user numbers or odd IDs
//!     (`COMPLEX` in `Fx::gt`), whose IDs are not all one person's, or whose events
//!     are not in time order, is filed event by event, with its clicks, in the general
//!     pipeline's buckets, which run afterwards as usual;
//!   * if the user number ever goes down in event-number order, a user number may have
//!     two runs and no run can be trusted to be a whole person: `run` returns false
//!     and the caller starts over with the general pipeline alone.
//!
//! A run that crosses a bucket boundary is not copied: each bucket leaves a summary of
//! its first and its last run (session starts and sizes, purchases, the IDs seen), the
//! summaries are joined once all buckets are done, and a joined run that fails a check
//! is read again from its buckets (`replay`).

use crate::fast::{scramble, Fx, XRec, COMPLEX, EXTRA, INVALID, ODDU, XCH};
use crate::id::Id;
use crate::mem;
use crate::{begin_person, flush, CRec, Ctx, PRec, Shared, Worker, CCH, CS};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

/// One place of the slot table: the record's user word as phase A wrote it (`XRec::a`),
/// its time with `BUY` set for a purchase, and its ingest key. A vacant slot is all
/// ones, so any real ingest key is smaller than its `ing`. The price of a purchase is
/// kept in a table of its own (`Tls::pcs`), written and read only for purchases.
#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    a: u64,
    t: u32,
    ing: u32,
}
const VACANT: Slot = Slot { a: !0, t: !0, ing: !0 };
/// In the dense user column (user number << 4 | alias): the code of every alias from
/// 15 up; the full value is kept aside (`Tls::esc`).
const FAR: u32 = 15;
const BUY: u32 = 1 << 31;
const M31: u32 = BUY - 1;
/// In the session list: this session also starts a run.
const RUN: u32 = 1 << 31;
const MIN30: i32 = 1_800_000;
const WEEK: u32 = 604_800_000;
const DAY: u32 = 86_400_000;
const NONE: u32 = u32::MAX;

/// With more sessions than this, the sessions a click reaches are found by binary
/// search rather than by comparing the click with every session start.
const SCAN: usize = 256;

/// Piece flags.
const GENERAL: u32 = 1; // already filed with the general pipeline
const UNSORTED: u32 = 2;

struct Parts<T>(*mut T);
unsafe impl<T> Sync for Parts<T> {}
unsafe impl<T> Send for Parts<T> {}

/// Each bucket's chunks in a fixed order: (base, records).
struct Lists {
    ev: Vec<Vec<(usize, u32)>>,
    cl: Vec<Vec<(usize, u32)>>,
}

#[inline(always)]
unsafe fn crec(l: &[(usize, u32)], j: usize) -> *mut CRec {
    (l.get_unchecked(j / CCH).0 as *mut CRec).add(j % CCH)
}

#[inline(always)]
fn prefetch_read<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

#[inline(always)]
pub(crate) unsafe fn prefetch_write<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_ET0);
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// A click in the click index: time, campaign << 9 | alias, and its place in the
/// click bucket's chunks (chunk number * CCH + place in the chunk).
#[derive(Clone, Copy)]
#[repr(C)]
struct Kc {
    ts: u32,
    ca: u32,
    j: u32,
}

/// One person's sessions, purchases and clicks, and the work areas for them.
#[derive(Default)]
struct Scr {
    st: Vec<u32>,         // session starts, in time order
    sn: Vec<u32>,         // events per session
    en: Vec<u32>,         // session ends (filled only where sessions are joined or added to)
    pur: Vec<(u32, u32)>, // (session, price | category << 24), by session
    days: Vec<(u32, u32)>, // the person's days: (day, first session)
    /// Per session: the latest click at or before its start and after the previous
    /// session's, as (time + 1, campaign, place in the click bucket); time 0 for none.
    top: Vec<(u32, u32, u32)>,
    /// The campaigns among the person's clicks, and per campaign the set of sessions
    /// a click of it reaches (a bit per session, `words` words each, in `reached`).
    touched: Vec<u32>,
    reached: Vec<u64>,
    slot_of: Vec<u32>, // per campaign: its place in `touched` + 1, or 0
    piv: Vec<u32>,     // the last start of each block of 16 sessions
    sday: Vec<u8>,     // per session: which of the person's days it is on
    dmask: Vec<u64>,   // per day, `words` words: the sessions on it
}

/// One thread's campaign counts for whole persons. A campaign's days are one row of
/// `1 << wsh` cells (64 bytes for up to 16 days), so the row a click will need can be
/// fetched ahead with one touch. Folded into `CS::credit` / `CS::assist` at the end.
#[derive(Clone, Copy)]
struct Acc {
    sess: *mut u32,   // credited sessions
    assist: *mut u32, // assisted sessions
    purch: *mut u32,  // credited sessions with a purchase
    rev: *mut u64,    // their revenue
    wsh: u32,
}

impl Acc {
    #[inline(always)]
    fn fetch(&self, camp: u32) {
        prefetch_read(unsafe { self.sess.add((camp as usize) << self.wsh) });
        prefetch_read(unsafe { self.assist.add((camp as usize) << self.wsh) });
    }
}

/// Adds a thread's campaign counts to the general pipeline's tables.
unsafe fn fold_counts(acc: &Acc, st: &mut CS, ctx: &Ctx) {
    let nd = ctx.ndays;
    for c in 0..=ctx.organic as usize {
        for d in 0..nd {
            let (from, to) = ((c << acc.wsh) + d, c * nd + d);
            st.credit[to][0] += *acc.sess.add(from) as u64 + ((*acc.purch.add(from) as u64) << 32);
            st.credit[to][1] += *acc.rev.add(from);
            st.assist[to] += *acc.assist.add(from);
        }
    }
}

/// What a bucket leaves behind about its first or last run.
#[derive(Clone, Copy)]
struct Piece {
    g: u32,
    bits: u32, // alias codes seen, one bit each
    flags: u32,
    last_t: u32,
    s: (u32, u32), // its sessions in `Edges::st` / `sn`
    p: (u32, u32), // its purchases in `Edges::pur`
    bucket: u32,
}

#[derive(Default)]
struct Edges {
    pieces: Vec<Piece>,
    st: Vec<u32>,
    sn: Vec<u32>,
    en: Vec<u32>,
    pur: Vec<(u32, u32)>,
}

/// Filing with the general pipeline: the person number of each alias of the user
/// number at hand is looked up once.
struct ToGeneral {
    who: [u32; 512],
    gen: [u32; 512],
    cur: u32,
}

impl ToGeneral {
    fn new() -> ToGeneral {
        ToGeneral { who: [0; 512], gen: [0; 512], cur: 0 }
    }
    #[inline(always)]
    fn next_user(&mut self) {
        self.cur += 1;
    }
    /// Files the event of user `un` (user number * 512 + alias).
    #[inline]
    unsafe fn event(&mut self, env: &Env, w: &mut Worker, un: u64, t: u32, pc: u32) {
        let a = (un & 511) as usize;
        if self.gen[a] != self.cur {
            let id = env.fx.user_id(un);
            self.who[a] = env.sh.links.person(id, id.hash(), &mut w.added);
            self.gen[a] = self.cur;
            if w.added >= 256 {
                env.sh.links.added(&mut w.added);
            }
        }
        let who = self.who[a];
        w.push_event(env.sh, (who >> env.sh.pshift) as usize, PRec { who, t, pc });
    }
}

/// One thread's tables and work areas, made once and used for every bucket.
struct Tls {
    slots: *mut Slot,
    pcs: *mut u32, // per slot: price | category << 24 of a purchase
    t: *mut u32,    // dense columns; index -1 is readable
    u: *mut u32,
    pc: *mut u32,
    ss: *mut u32, // session starts: dense index, RUN for a run start; ends with n | RUN
    pi: *mut u32, // dense indexes of purchases
    uns: Vec<u32>,
    esc: Vec<(u32, u64)>,      // (dense index, user number * 512 + alias) of far aliases
    odd: Vec<(u64, u32, u32)>, // events of odd user IDs: (person, t, pc)
    scr: Scr,
    acc: Acc,
    ahead: u32, // clicks of the index up to here have had their campaign rows fetched
    edges: Edges,
    gen: ToGeneral,
    ti: usize,
    tm: [u64; 6],  // ns: place, read out, cut, runs, clicks index, joined runs
    cnt: [u64; 4], // events in whole persons, events filed the general way, persons, sessions
}
unsafe impl Send for Tls {}
unsafe impl Sync for Env<'_> {}

impl Tls {
    fn new(cap: usize, ti: usize, ctx: &Ctx) -> Tls {
        const H: usize = 2 << 20;
        let col = (cap + 64) * 4;
        let wsh = ctx.ndays.next_power_of_two().max(16).trailing_zeros();
        let cells = (ctx.organic as usize + 1) << wsh;
        let tables = (cap + 16) * 16 + cap * 4 + 5 * col;
        let tables = tables.div_ceil(64) * 64;
        let bytes = tables + cells * 20;
        let len = bytes.div_ceil(H) * H;
        let raw = mem::map(len + H);
        let base = ((raw as usize + H - 1) & !(H - 1)) as *mut u8;
        // Small and hot: worth a huge page (SS_HUGE=0 turns the request off, as for
        // the ID table).
        if std::env::var_os("SS_HUGE").is_none_or(|v| v != "0") {
            mem::hugepages(base, len);
        }
        unsafe {
            let slots = base as *mut Slot;
            for i in 0..cap + 16 {
                *slots.add(i) = VACANT;
            }
            let pcs = base.add((cap + 16) * 16) as *mut u32;
            let cols = base.add((cap + 16) * 16 + cap * 4);
            // Touch the columns now, so no page is first written inside the bucket loop.
            std::ptr::write_bytes(cols, 0, 5 * col);
            let counts = base.add(tables);
            std::ptr::write_bytes(counts, 0, cells * 20);
            let acc = Acc {
                rev: counts as *mut u64,
                sess: counts.add(cells * 8) as *mut u32,
                assist: counts.add(cells * 12) as *mut u32,
                purch: counts.add(cells * 16) as *mut u32,
                wsh,
            };
            let at = |k: usize| cols.add(k * col + 64) as *mut u32;
            Tls {
                slots,
                pcs,
                t: at(0),
                u: at(1),
                pc: at(2),
                ss: at(3),
                pi: at(4),
                uns: Vec::new(),
                esc: Vec::new(),
                odd: Vec::new(),
                scr: Scr::default(),
                acc,
                ahead: 0,
                edges: Edges::default(),
                gen: ToGeneral::new(),
                ti,
                tm: [0; 6],
                cnt: [0; 4],
            }
        }
    }
}

/// What every thread of the stage reads.
struct Env<'a> {
    sh: &'a Shared<'a>,
    fx: &'a Fx,
    ctx: &'a Ctx<'a>,
    li: &'a Lists,
    /// The click index: per user number, where its clicks start in `kc`.
    coff: &'a [u32],
    kc: *const Kc,
    nclicks: u32,
    /// Valid events with an odd event ID and a numbered user: (user number * 512 +
    /// alias, t, pc), by user. `taken` is set once a user number's have been handled.
    extras: &'a [(u64, u32, u32)],
    taken: &'a [AtomicBool],
    cap: usize,
    wide: bool, // AVX-512 kernels
    broken: &'a AtomicBool,
    edge: Parts<[u32; 2]>, // per bucket: the pieces of its first and last run
}

// ---------------------------------------------------------------------------
// Kernels

/// Places a bucket's records in the slot table.
#[inline(never)]
unsafe fn place(tab: *mut Slot, pcs: *mut u32, l: &[(usize, u32)]) {
    const AHEAD: usize = 16;
    for &(c, n) in l {
        let n = n as usize;
        let rs = c as *const XRec;
        for i in 0..n {
            let r = &*rs.add(i);
            // (The place of a record a few ahead is fetched early, and the records
            // themselves further ahead.)
            prefetch_read(rs.add(i + 48));
            prefetch_write(tab.add((*rs.add((i + AHEAD).min(n - 1))).lo as usize));
            let at = r.lo as usize;
            let s = tab.add(at);
            if r.ing < (*s).ing {
                *s = Slot { a: r.a, t: r.t | ((r.pc != 0) as u32) << 31, ing: r.ing };
                if r.pc != 0 {
                    *pcs.add(at) = r.pc;
                }
            }
        }
    }
}

/// The dense user word of a valid slot: user number << 4 | alias code.
#[inline(always)]
fn user_word(a: u64) -> u32 {
    (((a >> 9) as u32) << 4) | ((a & 511) as u32).min(FAR)
}

/// A valid slot that is a purchase or has a far alias, or the slot of an odd user ID:
/// `n` is its place in the dense columns (the place it would have, for an odd user).
#[inline(always)]
unsafe fn special(x: &mut Tls, at: usize, s: Slot, n: usize) {
    let pc = if s.t & BUY != 0 { *x.pcs.add(at) } else { 0 };
    if s.a & (INVALID | ODDU) == ODDU {
        x.odd.push((s.a, s.t & M31, pc));
        return;
    }
    if s.t & BUY != 0 {
        *x.pc.add(n) = pc;
    }
    if (s.a & 511) as u32 >= FAR {
        x.esc.push((n as u32, s.a));
    }
}

/// Reads the slot table out in order, valid events only, into the dense columns (the
/// price column is written at purchases only), and leaves the table vacant. Returns
/// the number of events.
unsafe fn readout_plain(x: &mut Tls, cap: usize) -> usize {
    let mut n = 0;
    for at in 0..cap {
        let s = *x.slots.add(at);
        *x.slots.add(at) = VACANT;
        if s.a & (INVALID | ODDU) == 0 {
            if s.t & BUY != 0 || (s.a & 511) as u32 >= FAR {
                special(x, at, s, n);
            }
            *x.t.add(n) = s.t;
            *x.u.add(n) = user_word(s.a);
            n += 1;
        } else if s.a & (INVALID | ODDU) == ODDU {
            special(x, at, s, n);
        }
    }
    n
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,popcnt")]
unsafe fn readout_wide(x: &mut Tls, cap: usize) -> usize {
    use core::arch::x86_64::*;
    // Sixteen slots are four 64-byte loads; each field is every fourth lane.
    let pick = |f: i32| _mm512_setr_epi32(f, f + 4, f + 8, f + 12, f + 16, f + 20, f + 24, f + 28, 0, 0, 0, 0, 0, 0, 0, 0);
    let (ilo, ihi, it) = (pick(0), pick(1), pick(2));
    let ones = _mm512_set1_epi32(-1);
    let zero = _mm512_setzero_si512();
    let far = _mm512_set1_epi32(FAR as i32);
    let low9 = _mm512_set1_epi32(511);
    const { assert!(INVALID == ODDU << 1 && ODDU >= 1 << 36) };
    let mark_shift = _mm_cvtsi32_si128(ODDU.trailing_zeros() as i32 - 32);
    let (t, u) = (x.t, x.u);
    let mut n = 0usize;
    let mut at = 0usize;
    while at < cap {
        let p = x.slots.add(at) as *mut __m512i;
        let (z0, z1, z2, z3) = (_mm512_loadu_si512(p), _mm512_loadu_si512(p.add(1)), _mm512_loadu_si512(p.add(2)), _mm512_loadu_si512(p.add(3)));
        let col = |i: __m512i| {
            let a = _mm512_permutex2var_epi32(z0, i, z1);
            let b = _mm512_permutex2var_epi32(z2, i, z3);
            _mm512_inserti64x4::<1>(a, _mm512_castsi512_si256(b))
        };
        // The user word's two halves: the marks are two bits of the high half
        // (wherever fast.rs puts ODDU and, right above it, INVALID).
        let (lo, hi, vt) = (col(ilo), col(ihi), col(it));
        let marks = _mm512_and_si512(_mm512_srl_epi32(hi, mark_shift), _mm512_set1_epi32(3));
        let valid = _mm512_cmpeq_epi32_mask(marks, zero);
        let odd = _mm512_cmpeq_epi32_mask(marks, _mm512_set1_epi32(1));
        let alias = _mm512_and_si512(lo, low9);
        // user number << 4 | alias code: the number is bits 9.. of the 64-bit word.
        let vu = _mm512_or_si512(
            _mm512_or_si512(_mm512_slli_epi32::<4>(_mm512_srli_epi32::<9>(lo)), _mm512_slli_epi32::<27>(hi)),
            _mm512_min_epu32(alias, far),
        );
        let spec = (valid & (_mm512_cmplt_epi32_mask(vt, zero) | _mm512_cmpge_epu32_mask(alias, far))) | odd;
        if spec != 0 {
            let mut m = spec as u32;
            while m != 0 {
                let k = m.trailing_zeros() as usize;
                m &= m - 1;
                let before = (valid as u32 & ((1u32 << k) - 1)).count_ones() as usize;
                let s = *x.slots.add(at + k);
                special(x, at + k, s, n + before);
            }
        }
        _mm512_storeu_si512(t.add(n) as *mut __m512i, _mm512_maskz_compress_epi32(valid, vt));
        _mm512_storeu_si512(u.add(n) as *mut __m512i, _mm512_maskz_compress_epi32(valid, vu));
        n += (valid as u32).count_ones() as usize;
        _mm512_storeu_si512(p, ones);
        _mm512_storeu_si512(p.add(1), ones);
        _mm512_storeu_si512(p.add(2), ones);
        _mm512_storeu_si512(p.add(3), ones);
        at += 16;
    }
    n
}

/// What `cut` found: sessions, purchases, and whether a user number went down.
struct Cut {
    ns: usize,
    np: usize,
    down: bool,
}

/// Compares each event with the one before it. Writes the session starts (with RUN
/// where the user number changes) to `ss`, the purchases to `pi`, and events earlier
/// than their predecessor in the same run to `uns`.
unsafe fn cut_plain(x: &mut Tls, n: usize) -> Cut {
    let (t, u, ss, pi) = (x.t, x.u, x.ss, x.pi);
    let (mut ns, mut np, mut down) = (0usize, 0usize, false);
    for i in 0..n {
        let (g, tm) = (*u.add(i) >> 4, *t.add(i) & M31);
        let (run, d) = if i == 0 {
            (true, 0)
        } else {
            let (gp, tp) = (*u.add(i - 1) >> 4, *t.add(i - 1) & M31);
            down |= g < gp;
            (g != gp, tm as i32 - tp as i32)
        };
        if run | (d > MIN30) {
            *ss.add(ns) = i as u32 | if run { RUN } else { 0 };
            ns += 1;
        }
        if !run & (d < 0) {
            x.uns.push(i as u32);
        }
        if *t.add(i) & BUY != 0 {
            *pi.add(np) = i as u32;
            np += 1;
        }
    }
    *ss.add(ns) = n as u32 | RUN;
    Cut { ns, np, down }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,popcnt")]
unsafe fn cut_wide(x: &mut Tls, n: usize) -> Cut {
    use core::arch::x86_64::*;
    let (t, u, ss, pi) = (x.t, x.u, x.ss, x.pi);
    // The event before the first: a user number no event has.
    *u.sub(1) = !0;
    *t.sub(1) = 0;
    let m31 = _mm512_set1_epi32(M31 as i32);
    let gap = _mm512_set1_epi32(MIN30);
    let zero = _mm512_setzero_si512();
    let runbit = _mm512_set1_epi32(RUN as i32);
    let iota = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    let (mut ns, mut np, mut down) = (0usize, 0usize, 0u32);
    let mut i = 0usize;
    while i < n {
        let live = if n - i >= 16 { 0xFFFFu32 } else { (1u32 << (n - i)) - 1 };
        let vu = _mm512_loadu_si512(u.add(i) as *const __m512i);
        let vp = _mm512_loadu_si512(u.add(i).sub(1) as *const __m512i);
        let vt = _mm512_loadu_si512(t.add(i) as *const __m512i);
        let vq = _mm512_loadu_si512(t.add(i).sub(1) as *const __m512i);
        let (g, gp) = (_mm512_srli_epi32::<4>(vu), _mm512_srli_epi32::<4>(vp));
        let run = _mm512_cmpneq_epi32_mask(g, gp) as u32;
        let mut dn = _mm512_cmplt_epu32_mask(g, gp) as u32;
        if i == 0 {
            dn &= !1;
        }
        down |= dn & live;
        let d = _mm512_sub_epi32(_mm512_and_si512(vt, m31), _mm512_and_si512(vq, m31));
        let sess = (run | _mm512_cmpgt_epi32_mask(d, gap) as u32) & live;
        let uns = !run & _mm512_cmplt_epi32_mask(d, zero) as u32 & live;
        let buy = _mm512_cmplt_epi32_mask(vt, zero) as u32 & live;
        let idx = _mm512_add_epi32(_mm512_set1_epi32(i as i32), iota);
        let tagged = _mm512_mask_or_epi32(idx, run as __mmask16, idx, runbit);
        _mm512_storeu_si512(ss.add(ns) as *mut __m512i, _mm512_maskz_compress_epi32(sess as __mmask16, tagged));
        ns += sess.count_ones() as usize;
        _mm512_storeu_si512(pi.add(np) as *mut __m512i, _mm512_maskz_compress_epi32(buy as __mmask16, idx));
        np += buy.count_ones() as usize;
        if uns != 0 {
            let mut m = uns;
            while m != 0 {
                x.uns.push((i + m.trailing_zeros() as usize) as u32);
                m &= m - 1;
            }
        }
        i += 16;
    }
    *ss.add(ns) = n as u32 | RUN;
    Cut { ns, np, down: down != 0 }
}

fn wide_ok() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::env::var_os("SS_WIDE").is_none_or(|v| v != "0") && is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("popcnt")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[inline(always)]
unsafe fn readout(x: &mut Tls, cap: usize, wide: bool) -> usize {
    #[cfg(target_arch = "x86_64")]
    if wide {
        return readout_wide(x, cap);
    }
    let _ = wide;
    readout_plain(x, cap)
}

#[inline(always)]
unsafe fn cut(x: &mut Tls, n: usize, wide: bool) -> Cut {
    #[cfg(target_arch = "x86_64")]
    if wide {
        return cut_wide(x, n);
    }
    let _ = wide;
    cut_plain(x, n)
}

// ---------------------------------------------------------------------------
// One whole person

const P36: [u64; 9] = [1, 36, 1_296, 46_656, 1_679_616, 60_466_176, 2_176_782_336, 78_364_164_096, 2_821_109_907_456];

/// Orders numbers as their base-36 strings order byte-wise: digits left-aligned, and
/// of two equal ones (a string and its extension by zeros) the shorter first.
#[inline(always)]
fn text_key(n: u64) -> u64 {
    let mut len = 1;
    for p in &P36[1..8] {
        len += (n >= *p) as usize;
    }
    ((n * P36[8 - len]) << 4) | len as u64
}

/// Every pair of base-36 digits, as two bytes.
static PAIRS: [[u8; 2]; 1296] = {
    let d = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut t = [[0u8; 2]; 1296];
    let mut i = 0;
    while i < 1296 {
        t[i] = [d[i / 36], d[i % 36]];
        i += 1;
    }
    t
};

/// The ID "u" + base 36 of `n` (below 2^40: at most 8 digits).
#[inline]
fn user_text(n: u64) -> Id {
    // Eight digits, two at a time, then the leading zeros are skipped.
    let (hi, lo) = ((n / 1_679_616) as u32, (n % 1_679_616) as u32);
    let mut buf = [0u8; 40];
    buf[8..10].copy_from_slice(&PAIRS[(hi / 1296) as usize]);
    buf[10..12].copy_from_slice(&PAIRS[(hi % 1296) as usize]);
    buf[12..14].copy_from_slice(&PAIRS[(lo / 1296) as usize]);
    buf[14..16].copy_from_slice(&PAIRS[(lo % 1296) as usize]);
    let mut len = 1;
    for p in &P36[1..8] {
        len += (n >= *p) as usize;
    }
    let at = 16 - len - 1;
    buf[at] = b'u';
    unsafe { Id::short(buf.as_ptr().add(at), len + 1) }
}

/// Whether a run with alias codes `bits` is one whole person, given the aliases the
/// link files join under its user number (`mask`; 0 if no linked ID has that number):
/// every ID in the run is one of the joined ones, or nothing is joined and the run
/// uses a single ID, a person on its own. A far alias is never accepted here.
#[inline(always)]
fn whole(bits: u32, mask: u32) -> bool {
    bits & (1 << FAR) == 0 && if mask != 0 { bits & !mask == 0 } else { bits & (bits - 1) == 0 }
}

/// The person's name: the byte-wise smallest of their IDs.
#[cfg_attr(prof, inline(never))]
#[cfg_attr(not(prof), inline(always))]
fn name_of(g: u32, bits: u32, mask: u32, uk: u64) -> Id {
    let ids = if mask != 0 { mask } else { bits };
    let base = (g as u64) << 9;
    let first = scramble(base | ids.trailing_zeros() as u64, uk);
    let mut rest = ids & (ids - 1);
    if rest == 0 {
        return user_text(first);
    }
    let (mut best, mut key) = (first, text_key(first));
    while rest != 0 {
        let n = scramble(base | rest.trailing_zeros() as u64, uk);
        rest &= rest - 1;
        let k = text_key(n);
        if k < key {
            key = k;
            best = n;
        }
    }
    user_text(best)
}

#[inline(always)]
fn day_of(start: u32, off: i32) -> u32 {
    ((start as i32 + off) as u32) / DAY
}

/// The sessions a click at `ts` reaches: those that start at or after it and within
/// 7 days of it, as a range of the time-ordered starts `st[..m]` (16 readable entries
/// of u32::MAX follow them).
#[inline(always)]
unsafe fn reach_plain(st: *const u32, m: usize, ts: u32) -> (usize, usize) {
    if m > SCAN {
        let v = std::slice::from_raw_parts(st, m);
        return (v.partition_point(|&x| x < ts), v.partition_point(|&x| x <= ts + WEEK));
    }
    let (mut a, mut b) = (0usize, 0usize);
    for i in 0..m {
        a += (*st.add(i) < ts) as usize;
        b += (*st.add(i) <= ts + WEEK) as usize;
    }
    (a, b)
}

/// As `reach_plain`. `piv[k]` is the last start of the k-th block of 16 (u32::MAX
/// past the last full block; 16 entries): the blocks wholly before a time are counted
/// from it, and only the block the time falls in is compared start by start.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,popcnt")]
#[inline]
unsafe fn reach_wide(st: *const u32, piv: *const u32, m: usize, ts: u32) -> (usize, usize) {
    use core::arch::x86_64::*;
    if m > SCAN {
        return reach_plain(st, m, ts);
    }
    let (x, y) = (_mm512_set1_epi32(ts as i32), _mm512_set1_epi32((ts + WEEK) as i32));
    if m <= 16 {
        let v = _mm512_loadu_si512(st as *const __m512i);
        return ((_mm512_cmplt_epu32_mask(v, x) as u32).count_ones() as usize, (_mm512_cmple_epu32_mask(v, y) as u32).count_ones() as usize);
    }
    let p = _mm512_loadu_si512(piv as *const __m512i);
    let ba = (_mm512_cmplt_epu32_mask(p, x) as u32).count_ones() as usize;
    let bb = (_mm512_cmple_epu32_mask(p, y) as u32).count_ones() as usize;
    let va = _mm512_loadu_si512(st.add(16 * ba) as *const __m512i);
    let vb = _mm512_loadu_si512(st.add(16 * bb) as *const __m512i);
    (
        16 * ba + (_mm512_cmplt_epu32_mask(va, x) as u32).count_ones() as usize,
        16 * bb + (_mm512_cmple_epu32_mask(vb, y) as u32).count_ones() as usize,
    )
}

/// The person's clicks, in any order: for each, the sessions it reaches. The first of
/// them takes it as a candidate for its credit; all of them are noted as reached by
/// its campaign. Returns whether any click reaches a session.
macro_rules! click_pass {
    ($s:ident, $kc:ident, $ids:ident, $m:ident, $words:ident, $cid:ident, $reach:expr) => {{
        let stp = $s.st.as_ptr();
        let mut any = false;
        for r in $kc {
            let alias = r.ca & 511;
            if alias >= 16 || ($ids >> alias) & 1 == 0 {
                continue;
            }
            let (je, jl) = $reach(stp, $m, r.ts);
            if je >= jl {
                continue;
            }
            any = true;
            let camp = r.ca >> 9;
            // Credit: the latest such click wins; at the same instant, the smallest click_id.
            let top = $s.top.get_unchecked_mut(je);
            if r.ts + 1 > top.0 {
                *top = (r.ts + 1, camp, r.j);
            } else if r.ts + 1 == top.0 && r.j != top.2 && $cid(r.j).less(&$cid(top.2)) {
                *top = (r.ts + 1, camp, r.j);
            }
            let slot = *$s.slot_of.get_unchecked(camp as usize);
            let k = if slot == 0 {
                $s.touched.push(camp);
                $s.reached.resize($s.reached.len() + $words, 0);
                *$s.slot_of.get_unchecked_mut(camp as usize) = $s.touched.len() as u32;
                $s.touched.len() - 1
            } else {
                slot as usize - 1
            };
            let set = $s.reached.as_mut_ptr().add(k * $words);
            if $words == 1 {
                *set |= (u64::MAX >> (64 - (jl - je))) << je;
            } else {
                for wi in je / 64..=(jl - 1) / 64 {
                    let (lo, hi) = (je.max(wi * 64) - wi * 64, jl.min(wi * 64 + 64) - wi * 64);
                    *set.add(wi) |= (u64::MAX >> (64 - (hi - lo))) << lo;
                }
            }
        }
        any
    }};
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,popcnt")]
unsafe fn click_pass_wide(s: &mut Scr, kc: &[Kc], ids: u32, m: usize, words: usize, cid: &impl Fn(u32) -> Id) -> bool {
    let piv = s.piv.as_ptr();
    click_pass!(s, kc, ids, m, words, cid, |st, m, ts| reach_wide(st, piv, m, ts))
}

unsafe fn click_pass_plain(s: &mut Scr, kc: &[Kc], ids: u32, m: usize, words: usize, cid: &impl Fn(u32) -> Id) -> bool {
    click_pass!(s, kc, ids, m, words, cid, reach_plain)
}

/// One whole person from their session list (`s.st`, `s.sn`), purchases (`s.pur`) and
/// clicks (`kc`, those whose alias is in `ids`): rows, credit and assists (rules.md
/// "Events" steps 6 and 7, "Clicks" steps 4 to 7). Nothing is sorted: each click finds
/// the sessions it reaches, and each session then takes the latest click that reached
/// it first-hand, or failing that the one the session before it had. `cid` gives a
/// click's ID, asked only to break a tie.
#[cfg_attr(prof, inline(never))]
#[cfg_attr(not(prof), inline(always))]
unsafe fn person_rows(st: &mut CS, acc: &Acc, ctx: &Ctx, name: Id, s: &mut Scr, kc: &[Kc], ids: u32, wide: bool, cid: impl Fn(u32) -> Id) {
    let wsh = acc.wsh;
    let off = ctx.dayoff as i32;
    let m = s.st.len();
    let words = m.div_ceil(64);
    let mut clicks = false;
    if !kc.is_empty() {
        s.st.reserve(16);
        let end = s.st.as_mut_ptr().add(m);
        for i in 0..16 {
            *end.add(i) = u32::MAX;
        }
        if wide && m > 16 && m <= SCAN {
            s.piv.clear();
            s.piv.resize(16, u32::MAX);
            for k in 0..m / 16 {
                s.piv[k] = *s.st.get_unchecked(16 * k + 15);
            }
        }
        s.top.clear();
        s.top.resize(m, (0, 0, 0));
        if s.slot_of.len() < ctx.organic as usize {
            s.slot_of.resize(ctx.organic as usize, 0);
        }
        s.touched.clear();
        s.reached.clear();
        #[cfg(target_arch = "x86_64")]
        {
            clicks = if wide { click_pass_wide(s, kc, ids, m, words, &cid) } else { click_pass_plain(s, kc, ids, m, words, &cid) };
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = wide;
            clicks = click_pass_plain(s, kc, ids, m, words, &cid);
        }
    }
    {
        // The rows are appended to six columns that are not in cache: the lines the
        // next few persons' rows will land on are fetched now.
        let r = &st.rows;
        prefetch_write(r.dict.as_ptr().wrapping_add(r.dict.len() + 128));
        prefetch_write((r.runs.as_ptr() as *const u8).wrapping_add(r.runs.len() * 4 + 64));
        prefetch_write((r.days.as_ptr() as *const u8).wrapping_add(r.days.len() * 4 + 128));
        prefetch_write((r.sessions.as_ptr() as *const u8).wrapping_add(r.sessions.len() * 4 + 128));
        prefetch_write((r.events.as_ptr() as *const u8).wrapping_add(r.events.len() * 4 + 128));
        prefetch_write((r.revenue.as_ptr() as *const u8).wrapping_add(r.revenue.len() * 8 + 192));
        prefetch_write((r.tops.as_ptr() as *const u8).wrapping_add(r.tops.len() * 2 + 64));
    }
    let (mut cur, mut us, mut ue, mut ur) = (NONE, 0u32, 0u32, 0u64);
    let mut pp = 0usize;
    let npur = s.pur.len();
    // The click the session at hand could be credited to: (time, campaign).
    let (mut have, mut cts, mut ccamp) = (false, 0u32, 0u32);
    begin_person(st, name);
    st.catrev.clear();
    s.days.clear();
    if clicks {
        s.dmask.clear();
        if s.sday.len() < m {
            s.sday.resize(m, 0);
        }
    }
    for j in 0..m {
        let start = *s.st.get_unchecked(j);
        let day = day_of(start, off);
        if day != cur {
            if cur != NONE {
                flush(st, name, cur as i64 + ctx.day0, us, ue, ur, ctx);
            }
            cur = day;
            us = 0;
            ue = 0;
            ur = 0;
            s.days.push((day, j as u32));
            if clicks {
                s.dmask.resize(s.dmask.len() + words, 0);
            }
        }
        if clicks {
            let di = s.days.len() - 1;
            *s.sday.get_unchecked_mut(j) = di as u8;
            *s.dmask.get_unchecked_mut(di * words + j / 64) |= 1 << (j % 64);
        }
        let mut rev = 0u64;
        while pp < npur && s.pur.get_unchecked(pp).0 == j as u32 {
            let pc = s.pur.get_unchecked(pp).1;
            let (price, cat) = ((pc & 0x00FF_FFFF) as u64, pc >> 24);
            rev += price;
            match st.catrev.iter_mut().find(|c| c.0 == cat) {
                Some(c) => c.1 += price,
                None => st.catrev.push((cat, price)),
            }
            pp += 1;
        }
        us += 1;
        ue += *s.sn.get_unchecked(j);
        ur += rev;
        let mut camp = ctx.organic;
        if clicks {
            let top = *s.top.get_unchecked(j);
            if top.0 != 0 {
                have = true;
                cts = top.0 - 1;
                ccamp = top.1;
            }
            if have && start - cts <= WEEK {
                camp = ccamp;
            }
        }
        let cell = ((camp as usize) << wsh) + day as usize;
        *acc.sess.add(cell) += 1;
        if rev > 0 {
            *acc.purch.add(cell) += 1;
            *acc.rev.add(cell) += rev;
        }
    }
    flush(st, name, cur as i64 + ctx.day0, us, ue, ur, ctx);
    if clicks {
        // Assists: per campaign, the sessions it reached, counted day by day: the
        // first reached session left names a day, that day's sessions are counted
        // and taken out, until none is left.
        for (k, &c) in s.touched.iter().enumerate() {
            let row = acc.assist.add((c as usize) << wsh);
            for w in 0..words {
                let mut set = *s.reached.get_unchecked(k * words + w);
                while set != 0 {
                    let di = *s.sday.get_unchecked(w * 64 + set.trailing_zeros() as usize) as usize;
                    let on = *s.dmask.get_unchecked(di * words + w);
                    *row.add(s.days.get_unchecked(di).0 as usize) += (set & on).count_ones();
                    set &= !on;
                }
            }
            *s.slot_of.get_unchecked_mut(c as usize) = 0;
        }
    } else if !kc.is_empty() {
        for &c in &s.touched {
            *s.slot_of.get_unchecked_mut(c as usize) = 0;
        }
    }
}

/// A run that passed `whole`, with its sessions and purchases in `s`.
#[inline(always)]
unsafe fn person(env: &Env, st: &mut CS, s: &mut Scr, acc: &Acc, ahead: &mut u32, g: u32, bits: u32, mask: u32) {
    let name = name_of(g, bits, mask, env.fx.uk);
    let (c0, c1) = (*env.coff.get_unchecked(g as usize), *env.coff.get_unchecked(g as usize + 1));
    // The index is in user-number order, the order runs come in: the campaign rows of
    // the clicks a little further on are fetched now, for the next few persons.
    let to = (c1 + 24 + (c1 - c0)).min(env.nclicks);
    let mut i = (*ahead).max(c1);
    if i < to && to - i <= 4096 {
        while i < to {
            acc.fetch((*env.kc.add(i as usize)).ca >> 9);
            i += 1;
        }
        *ahead = to;
    } else if i < to {
        *ahead = c1; // a very large person: not worth fetching for
    }
    let kc = std::slice::from_raw_parts(env.kc.add(c0 as usize), (c1 - c0) as usize);
    let l = env.li.cl.get_unchecked((g >> env.fx.cshift) as usize);
    person_rows(st, acc, env.ctx, name, s, kc, if mask != 0 { mask } else { bits }, env.wide, |j| (*crec(l, j as usize)).cid);
}

// ---------------------------------------------------------------------------
// Filing with the general pipeline

/// Files a click of user number `r.who`, alias `r._pad`, with the general pipeline.
unsafe fn pool_click(env: &Env, w: &mut Worker, r: &CRec) {
    let id = env.fx.user_id(((r.who as u64) << 9) | r._pad as u64);
    let who = env.sh.links.person(id, id.hash(), &mut w.added);
    if w.added >= 256 {
        env.sh.links.added(&mut w.added);
    }
    w.push_click(env.sh, (who >> env.sh.pshift) as usize, CRec { who, ts: r.ts, camp: r.camp, _pad: 0, cid: r.cid });
}

/// Files every click of user number `g` with the general pipeline.
unsafe fn pool_clicks(env: &Env, w: &mut Worker, g: u32) {
    let l = &env.li.cl[(g >> env.fx.cshift) as usize];
    for i in env.coff[g as usize]..env.coff[g as usize + 1] {
        pool_click(env, w, &*crec(l, (*env.kc.add(i as usize)).j as usize));
    }
}

/// The odd-ID events of user number `g`, the first time it is asked (later: none).
fn extras_of<'a>(env: &Env<'a>, g: u32) -> &'a [(u64, u32, u32)] {
    let x = env.extras;
    let a = x.partition_point(|e| ((e.0 >> 9) as u32) < g);
    let b = x.partition_point(|e| ((e.0 >> 9) as u32) <= g);
    if a == b || env.taken[a].swap(true, Relaxed) {
        return &x[..0];
    }
    &x[a..b]
}

/// One more event, at `t`, for the person whose sessions are in `s` (with their ends
/// in `s.en`): it joins the session it is within 30 minutes of, joins two sessions
/// into one if it is that close to both, or is a session of its own.
fn add_event(s: &mut Scr, t: u32, pc: u32) {
    let m = s.st.len();
    let j = s.st.partition_point(|&x| x <= t);
    let near_prev = j > 0 && t as i64 - s.en[j - 1] as i64 <= MIN30 as i64;
    let near_next = j < m && (s.st[j] - t) as i64 <= MIN30 as i64;
    let at = match (near_prev, near_next) {
        (true, true) => {
            s.en[j - 1] = s.en[j];
            s.sn[j - 1] += s.sn[j] + 1;
            s.st.remove(j);
            s.en.remove(j);
            s.sn.remove(j);
            for p in s.pur.iter_mut().filter(|p| p.0 >= j as u32) {
                p.0 -= 1;
            }
            j - 1
        }
        (true, false) => {
            s.sn[j - 1] += 1;
            s.en[j - 1] = s.en[j - 1].max(t);
            j - 1
        }
        (false, true) => {
            s.sn[j] += 1;
            s.st[j] = t;
            j
        }
        (false, false) => {
            s.st.insert(j, t);
            s.en.insert(j, t);
            s.sn.insert(j, 1);
            for p in s.pur.iter_mut().filter(|p| p.0 >= j as u32) {
                p.0 += 1;
            }
            j
        }
    };
    if pc != 0 {
        let q = s.pur.partition_point(|p| p.0 <= at as u32);
        s.pur.insert(q, (at as u32, pc));
    }
}

/// Files odd-ID events with the general pipeline.
unsafe fn general_extras(env: &Env, x: &mut Tls, w: &mut Worker, more: &[(u64, u32, u32)]) {
    for &(un, t, pc) in more {
        x.gen.event(env, w, un, t, pc);
    }
    x.cnt[1] += more.len() as u64;
}

/// Reads bucket `b` again and files its events of user number `g` with the general
/// pipeline (a joined run that failed a check; rare).
#[inline(never)]
unsafe fn replay(env: &Env, x: &mut Tls, w: &mut Worker, b: usize, g: u32) {
    place(x.slots, x.pcs, &env.li.ev[b]);
    x.gen.next_user();
    for at in 0..env.cap {
        let s = *x.slots.add(at);
        *x.slots.add(at) = VACANT;
        if s.a & (INVALID | ODDU) == 0 && (s.a >> 9) as u32 == g {
            let pc = if s.t & BUY != 0 { *x.pcs.add(at) } else { 0 };
            x.gen.event(env, w, s.a, s.t & M31, pc);
            x.cnt[1] += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// One bucket

/// Sessions `k..ke` of the session list and purchases `pp..pe` of the purchase list,
/// appended to a person's or a piece's lists.
#[cfg_attr(prof, inline(never))]
#[cfg_attr(not(prof), inline(always))]
unsafe fn sessions_into(x: &Tls, k: usize, ke: usize, pp: usize, pe: usize, st: &mut Vec<u32>, sn: &mut Vec<u32>, pur: &mut Vec<(u32, u32)>) {
    let (t, ss) = (x.t, x.ss);
    let n = ke - k;
    st.reserve(n);
    sn.reserve(n);
    let (ps, pn) = (st.as_mut_ptr().add(st.len()), sn.as_mut_ptr().add(sn.len()));
    for j in 0..n {
        let a = *ss.add(k + j) & M31;
        *ps.add(j) = *t.add(a as usize) & M31;
        *pn.add(j) = (*ss.add(k + j + 1) & M31) - a;
    }
    st.set_len(st.len() + n);
    sn.set_len(sn.len() + n);
    session_tail(x, k, ke, pp, pe, pur);
}

/// The time of the last event of each of the sessions `k..ke`.
unsafe fn ends_into(x: &Tls, k: usize, ke: usize, en: &mut Vec<u32>) {
    for j in k..ke {
        en.push(*x.t.add((*x.ss.add(j + 1) & M31) as usize - 1) & M31);
    }
}

#[inline(always)]
unsafe fn session_tail(x: &Tls, k: usize, _ke: usize, pp: usize, pe: usize, pur: &mut Vec<(u32, u32)>) {
    let (pc, ss, pi) = (x.pc, x.ss, x.pi);
    let mut sj = k;
    for q in pp..pe {
        let i = *pi.add(q);
        while (*ss.add(sj + 1) & M31) <= i {
            sj += 1;
        }
        pur.push(((sj - k) as u32, *pc.add(i as usize)));
    }
}

/// Files the events `r0..r1` of the dense columns with the general pipeline.
#[inline(never)]
unsafe fn general_run(env: &Env, x: &mut Tls, w: &mut Worker, r0: usize, r1: usize, mut ep: usize) {
    x.gen.next_user();
    for i in r0..r1 {
        let u = *x.u.add(i);
        let un = if u & 15 == FAR {
            while x.esc[ep].0 < i as u32 {
                ep += 1;
            }
            x.esc[ep].1
        } else {
            ((u >> 4) as u64) << 9 | (u & 15) as u64
        };
        let t = *x.t.add(i);
        x.gen.event(env, w, un, t & M31, if t & BUY != 0 { *x.pc.add(i) } else { 0 });
    }
    x.cnt[1] += (r1 - r0) as u64;
}

/// The runs of bucket `b`, whose `n` events are in the dense columns.
#[inline(never)]
unsafe fn runs(b: usize, n: usize, c: &Cut, env: &Env, x: &mut Tls, st: &mut CS, w: &mut Worker) {
    let (u, ss, pi) = (x.u, x.ss, x.pi);
    let gt = &env.fx.gt;
    let (mut k, mut pp, mut ep, mut up) = (0usize, 0usize, 0usize, 0usize);
    let (nesc, nuns) = (x.esc.len(), x.uns.len());
    let mut ends = [NONE; 2];
    while k < c.ns {
        let r0 = (*ss.add(k) & M31) as usize;
        let mut ke = k + 1;
        while (*ss.add(ke) as i32) >= 0 {
            ke += 1;
        }
        let r1 = (*ss.add(ke) & M31) as usize;
        let g = *u.add(r0) >> 4;
        // The columns, the table of user numbers and the click index are read front
        // to back, a person at a time: what the next few persons need is fetched now.
        prefetch_read(x.t.add(r1 + 192));
        prefetch_read(u.add(r1 + 192));
        prefetch_read(gt.as_ptr().add(g as usize + 24));
        prefetch_read(env.coff.as_ptr().add(g as usize + 48));
        prefetch_read(env.kc.add(*env.coff.get_unchecked(g as usize + 1) as usize + 96));
        let mut bits = 0u32;
        for i in r0..r1 {
            bits |= 1 << (*u.add(i) & 15);
        }
        let mut pe = pp;
        while pe < c.np && (*pi.add(pe) as usize) < r1 {
            pe += 1;
        }
        let mut unsorted = false;
        while up < nuns && (x.uns[up] as usize) < r1 {
            unsorted = true;
            up += 1;
        }
        while ep < nesc && (x.esc[ep].0 as usize) < r0 {
            ep += 1;
        }
        let gw = gt.get_unchecked(g as usize).load(Relaxed);
        let complex = gw & COMPLEX != 0;
        let mask = ((gw >> 32) & 0xFFFF) as u32;
        let edge = r0 == 0 || r1 == n;
        if complex {
            // Its clicks were filed when the click index was built.
            general_run(env, x, w, r0, r1, ep);
        }
        // Odd-ID events of this user number are part of the run. (For a run that
        // crosses buckets they are taken when its pieces are joined.)
        let more = if gw & EXTRA != 0 && !edge { extras_of(env, g) } else { &[] };
        for e in more {
            bits |= 1 << ((e.0 & 511) as u32).min(FAR);
        }
        if edge {
            // The run may go on in the next bucket or have begun in the one before:
            // leave what is known about this part of it.
            let e = &mut x.edges as *mut Edges;
            let (s0, p0) = ((*e).st.len() as u32, (*e).pur.len() as u32);
            if !complex {
                sessions_into(x, k, ke, pp, pe, &mut (*e).st, &mut (*e).sn, &mut (*e).pur);
                ends_into(x, k, ke, &mut (*e).en);
            }
            let flags = if complex { GENERAL } else { 0 } | if unsorted { UNSORTED } else { 0 };
            let id = ((x.ti as u32) << 24) | (*e).pieces.len() as u32;
            (*e).pieces.push(Piece {
                g,
                bits,
                flags,
                last_t: *x.t.add(r1 - 1) & M31,
                s: (s0, (*e).st.len() as u32),
                p: (p0, (*e).pur.len() as u32),
                bucket: b as u32,
            });
            if r0 == 0 {
                ends[0] = id;
            }
            if r1 == n {
                ends[1] = id;
            }
        } else if !complex {
            if whole(bits, mask) && !unsorted {
                let s = &mut x.scr as *mut Scr;
                (*s).st.clear();
                (*s).sn.clear();
                (*s).pur.clear();
                sessions_into(x, k, ke, pp, pe, &mut (*s).st, &mut (*s).sn, &mut (*s).pur);
                if !more.is_empty() {
                    (*s).en.clear();
                    ends_into(x, k, ke, &mut (*s).en);
                    for &(_, t, pc) in more {
                        add_event(&mut *s, t, pc);
                    }
                }
                let acc = x.acc;
                person(env, st, &mut *s, &acc, &mut x.ahead, g, bits, mask);
                x.cnt[0] += (r1 - r0 + more.len()) as u64;
                x.cnt[2] += 1;
                x.cnt[3] += (*s).st.len() as u64;
            } else {
                general_run(env, x, w, r0, r1, ep);
                general_extras(env, x, w, more);
                pool_clicks(env, w, g);
            }
        }
        pp = pe;
        k = ke;
    }
    *env.edge.0.add(b) = ends;
}

/// Bucket `b`, start to finish.
unsafe fn event_bucket(b: usize, env: &Env, x: &mut Tls, st: &mut CS, w: &mut Worker) {
    let l = &env.li.ev[b];
    if l.is_empty() {
        return;
    }
    let t0 = Instant::now();
    place(x.slots, x.pcs, l);
    let t1 = Instant::now();
    x.uns.clear();
    x.esc.clear();
    x.odd.clear();
    let n = readout(x, env.cap, env.wide);
    let t2 = Instant::now();
    x.tm[0] += (t1 - t0).as_nanos() as u64;
    x.tm[1] += (t2 - t1).as_nanos() as u64;
    for i in 0..x.odd.len() {
        // Another person's event; its person number was looked up in phase A.
        let (a, t, pc) = x.odd[i];
        let who = a as u32;
        w.push_event(env.sh, (who >> env.sh.pshift) as usize, PRec { who, t, pc });
        x.cnt[1] += 1;
    }
    if n == 0 {
        return;
    }
    let c = cut(x, n, env.wide);
    let t3 = Instant::now();
    x.tm[2] += (t3 - t2).as_nanos() as u64;
    if c.down {
        env.broken.store(true, Relaxed);
        return;
    }
    runs(b, n, &c, env, x, st, w);
    x.tm[3] += t3.elapsed().as_nanos() as u64;
}

// ---------------------------------------------------------------------------
// The click index

/// Click bucket `c` into the click index: its clicks, grouped by user number, go to
/// `kc` from place `base` on, and each user number's start is noted in `off`. Clicks of
/// user numbers that take the general path are filed there.
unsafe fn click_bucket(c: usize, env: &Env, off: &mut [u32], kc: *mut Kc, base: u32, cnt: &mut Vec<u32>, tmp: &mut Vec<(Kc, u32)>, w: &mut Worker) {
    let fx = env.fx;
    let l = &env.li.cl[c];
    let span = off.len();
    if l.is_empty() {
        off.iter_mut().for_each(|x| *x = base);
        return;
    }
    let g0 = (c * fx.gper) as u32;
    cnt.clear();
    cnt.resize(span, 0);
    // One reading of the click records: each leaves its index entry and its user
    // number in `tmp`, in cache, from where the entries are then grouped.
    tmp.clear();
    for (ci, &(p, n)) in l.iter().enumerate() {
        let rs = std::slice::from_raw_parts(p as *const CRec, n as usize);
        tmp.reserve(rs.len());
        let out = tmp.as_mut_ptr().add(tmp.len());
        for (i, r) in rs.iter().enumerate() {
            let k = r.who - g0;
            *cnt.get_unchecked_mut(k as usize) += 1;
            *out.add(i) = (Kc { ts: r.ts, ca: (r.camp << 9) | r._pad, j: (ci * CCH + i) as u32 }, k);
        }
        tmp.set_len(tmp.len() + rs.len());
    }
    mem::populate(kc.add(base as usize) as *mut u8, tmp.len() * std::mem::size_of::<Kc>());
    let mut sum = base;
    for k in 0..span {
        off[k] = sum;
        sum += cnt[k];
        cnt[k] = off[k];
    }
    for &(e, k) in tmp.iter() {
        let at = cnt.get_unchecked_mut(k as usize);
        *kc.add(*at as usize) = e;
        *at += 1;
    }
    // (After filing, cnt[k] is the end of user number k's clicks.)
    for k in 0..span {
        if cnt[k] > off[k] && fx.gt[g0 as usize + k].load(Relaxed) & COMPLEX != 0 {
            for i in off[k]..cnt[k] {
                pool_click(env, w, &*crec(l, (*kc.add(i as usize)).j as usize));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Runs that cross bucket boundaries

/// A run made of the pieces `ids` (in bucket order), all of one user number.
unsafe fn joined(ids: &[u32], env: &Env, all: &[Parts<Edges>], x: &mut Tls, st: &mut CS, w: &mut Worker) {
    let piece = |id: u32| -> (&Edges, &Piece) {
        let e = &*all[(id >> 24) as usize].0;
        (e, &e.pieces[(id & 0x00FF_FFFF) as usize])
    };
    let (_, first) = piece(ids[0]);
    if first.flags & GENERAL != 0 {
        return;
    }
    let g = first.g;
    let s = &mut x.scr as *mut Scr;
    (*s).st.clear();
    (*s).sn.clear();
    (*s).en.clear();
    (*s).pur.clear();
    let (mut bits, mut bad, mut last_t, mut events) = (0u32, false, 0u32, 0u64);
    for (i, &id) in ids.iter().enumerate() {
        let (e, p) = piece(id);
        bits |= p.bits;
        bad |= p.flags & UNSORTED != 0;
        let (ts, ns, es) = (&e.st[p.s.0 as usize..p.s.1 as usize], &e.sn[p.s.0 as usize..p.s.1 as usize], &e.en[p.s.0 as usize..p.s.1 as usize]);
        let d = if i > 0 { ts[0] as i64 - last_t as i64 } else { i64::MAX };
        bad |= d < 0;
        // Its first session goes on from the last one so far if the gap allows.
        let join = d <= MIN30 as i64;
        let at = (*s).st.len() as u32 - join as u32;
        if join {
            *(*s).sn.last_mut().unwrap() += ns[0];
            *(*s).en.last_mut().unwrap() = es[0];
        }
        (*s).st.extend_from_slice(&ts[join as usize..]);
        (*s).sn.extend_from_slice(&ns[join as usize..]);
        (*s).en.extend_from_slice(&es[join as usize..]);
        (*s).pur.extend(e.pur[p.p.0 as usize..p.p.1 as usize].iter().map(|&(sj, pc)| (sj + at, pc)));
        last_t = p.last_t;
        events += ns.iter().map(|&n| n as u64).sum::<u64>();
    }
    let gw = env.fx.gt[g as usize].load(Relaxed);
    let mask = ((gw >> 32) & 0xFFFF) as u32;
    let more = if gw & EXTRA != 0 { extras_of(env, g) } else { &[] };
    for e in more {
        bits |= 1 << ((e.0 & 511) as u32).min(FAR);
    }
    if !bad && whole(bits, mask) {
        for &(_, t, pc) in more {
            add_event(&mut *s, t, pc);
        }
        let acc = x.acc;
        person(env, st, &mut *s, &acc, &mut x.ahead, g, bits, mask);
        x.cnt[0] += events + more.len() as u64;
        x.cnt[2] += 1;
        x.cnt[3] += (*s).st.len() as u64;
    } else {
        for &id in ids {
            replay(env, x, w, piece(id).1.bucket as usize, g);
        }
        general_extras(env, x, w, more);
        pool_clicks(env, w, g);
    }
}

// ---------------------------------------------------------------------------
// The stage

/// After phase A: the click index, then every bucket of events, then the runs that
/// cross buckets. Whole persons become rows and campaign counts in `cs`; everything
/// else is filed in the general pipeline's buckets (through each thread's `Worker`).
/// Returns false if the events are not numbered user by user (see the module text):
/// nothing this call produced may be used then.
pub fn run(sh: &Shared, workers: &mut [Worker], cs: &mut [CS], ctx: &Ctx, log: &dyn Fn(&str)) -> bool {
    let fx = sh.fx.as_ref().unwrap();
    let (nb, nxc, gper) = (fx.nb, fx.nxc, fx.gper);
    let nthreads = workers.len();
    assert!(nthreads <= 256 && fx.xsh >= 6 && (ctx.organic as u64) < 1 << 23);
    let t0 = Instant::now();
    let li = Lists {
        ev: (0..nb)
            .map(|b| {
                let mut v = Vec::new();
                for w in workers.iter() {
                    let total = w.xcnt[b] as usize;
                    v.extend(w.xchunks[b].iter().enumerate().map(|(i, &c)| (c, (total - i * XCH).min(XCH) as u32)));
                }
                v
            })
            .collect(),
        cl: (0..nxc)
            .map(|c| {
                let mut v = Vec::new();
                for w in workers.iter() {
                    let total = w.xccnt[c] as usize;
                    v.extend(w.xcchunks[c].iter().enumerate().map(|(i, &p)| (p, (total - i * CCH).min(CCH) as u32)));
                }
                v
            })
            .collect(),
    };
    // The click index: where each click bucket's clicks start, and room for them all.
    let mut cbase = vec![0u32; nxc + 1];
    for c in 0..nxc {
        cbase[c + 1] = cbase[c] + li.cl[c].iter().map(|x| x.1).sum::<u32>();
    }
    let nclicks = cbase[nxc] as usize;
    let kc_bytes = (nclicks + 16) * std::mem::size_of::<Kc>();
    let kc = mem::map(kc_bytes) as *mut Kc;
    let kc_huge = fx.huge || std::env::var_os("SS_KCHUGE").is_some();
    if kc_huge {
        mem::hugepages(kc as *mut u8, kc_bytes);
    }
    let mut coff = vec![0u32; nxc * gper + 1];
    coff[nxc * gper] = nclicks as u32;
    let mut edge = vec![[NONE; 2]; nb];
    let broken = AtomicBool::new(false);
    let cap = 1usize << fx.xsh;

    // Events whose ID has no number (a handful): dedup them here by the ID itself. A
    // survivor with an odd user ID, or whose user number takes the general path, is
    // filed there now; the others wait, by user number, for that number's run.
    let mut odd = Vec::new();
    for w in workers.iter_mut() {
        odd.append(&mut w.odd);
    }
    odd.sort_unstable_by_key(|o| (o.key.lo, o.key.hi, o.ing));
    let mut extras: Vec<(u64, u32, u32)> = Vec::new();
    let mut now: Vec<(u64, u32, u32)> = Vec::new();
    for (i, o) in odd.iter().enumerate() {
        if (i > 0 && odd[i - 1].key == o.key) || o.a & INVALID != 0 {
            continue; // a later copy, or the kept copy is invalid
        }
        if o.a & ODDU != 0 {
            let who = o.a as u32;
            unsafe { workers[0].push_event(sh, (who >> sh.pshift) as usize, PRec { who, t: o.t, pc: o.pc }) };
        } else if fx.gt[(o.a >> 9) as usize].load(Relaxed) & COMPLEX != 0 {
            now.push((o.a, o.t, o.pc));
        } else {
            fx.gt[(o.a >> 9) as usize].fetch_or(EXTRA, Relaxed);
            extras.push((o.a, o.t, o.pc));
        }
    }
    extras.sort_unstable();
    now.sort_unstable();
    let taken: Vec<AtomicBool> = extras.iter().map(|_| AtomicBool::new(false)).collect();
    let pcoff = Parts(coff.as_mut_ptr());
    let env = Env {
        sh,
        fx,
        ctx,
        li: &li,
        coff: unsafe { std::slice::from_raw_parts(coff.as_ptr(), coff.len()) },
        kc,
        nclicks: nclicks as u32,
        extras: &extras,
        taken: &taken,
        cap,
        wide: wide_ok(),
        broken: &broken,
        edge: Parts(edge.as_mut_ptr()),
    };
    let env = &env;
    let mut gen = ToGeneral::new();
    for (i, &(un, t, pc)) in now.iter().enumerate() {
        if i == 0 || now[i - 1].0 >> 9 != un >> 9 {
            gen.next_user();
        }
        unsafe { gen.event(env, &mut workers[0], un, t, pc) };
    }

    let mut tls: Vec<Tls> = Vec::new();
    let next = AtomicUsize::new(0);
    let nextc = AtomicUsize::new(0);
    let gate = std::sync::Barrier::new(nthreads);
    let made = std::sync::Mutex::new(Vec::new());
    let t_setup = t0.elapsed();
    {
        let (next, nextc, gate, cbase, made) = (&next, &nextc, &gate, &cbase[..], &made);
        std::thread::scope(|sc| {
            for (ti, (w, st)) in workers.iter_mut().zip(cs.iter_mut()).enumerate() {
                let pcoff = &pcoff;
                sc.spawn(move || unsafe {
                    // The click index first: every bucket of events reads it.
                    let tc = Instant::now();
                    let mut x = Tls::new(cap, ti, ctx);
                    let (mut cnt, mut tmp) = (Vec::new(), Vec::new());
                    loop {
                        let c = nextc.fetch_add(1, Relaxed);
                        if c >= nxc {
                            break;
                        }
                        let off = std::slice::from_raw_parts_mut(pcoff.0.add(c * gper), gper);
                        click_bucket(c, env, off, env.kc as *mut Kc, cbase[c], &mut cnt, &mut tmp, w);
                    }
                    drop((cnt, tmp));
                    x.tm[4] += tc.elapsed().as_nanos() as u64;
                    gate.wait();
                    loop {
                        let b = next.fetch_add(1, Relaxed);
                        if b >= nb || env.broken.load(Relaxed) || sh.links.full.load(Relaxed) {
                            break;
                        }
                        event_bucket(b, env, &mut x, st, w);
                    }
                    made.lock().unwrap().push(x);
                });
            }
        });
    }
    tls.append(&mut made.into_inner().unwrap());
    tls.sort_by_key(|x| x.ti);
    let t_buckets = t0.elapsed();
    if broken.load(Relaxed) {
        mem::release(kc as *mut u8, kc_bytes);
        return false;
    }

    // Join the pieces of runs that cross buckets. Between buckets the user number must
    // not go down either.
    let all: Vec<Parts<Edges>> = tls.iter_mut().map(|x| Parts(&mut x.edges as *mut Edges)).collect();
    let g_of = |id: u32| unsafe { (&(*all[(id >> 24) as usize].0).pieces)[(id & 0x00FF_FFFF) as usize].g };
    let mut ids: Vec<u32> = Vec::new();
    let mut groups: Vec<(usize, usize)> = Vec::new();
    let mut open = 0usize;
    for e in edge.iter() {
        let [head, tail] = *e;
        if head == NONE {
            continue;
        }
        if ids.len() > open {
            let (gp, gh) = (g_of(ids[open]), g_of(head));
            if gh < gp {
                mem::release(kc as *mut u8, kc_bytes);
                return false;
            }
            if gh != gp {
                groups.push((open, ids.len()));
                open = ids.len();
            }
        }
        ids.push(head);
        if tail != head {
            groups.push((open, ids.len()));
            open = ids.len();
            ids.push(tail);
        }
    }
    if ids.len() > open {
        groups.push((open, ids.len()));
    }
    {
        let next = AtomicUsize::new(0);
        let (gate1, gate2) = (std::sync::Barrier::new(nthreads), std::sync::Barrier::new(nthreads));
        let (next, ids, groups, all, gate1, gate2) = (&next, &ids[..], &groups[..], &all[..], &gate1, &gate2);
        std::thread::scope(|sc| {
            for ((x, w), st) in tls.iter_mut().zip(workers.iter_mut()).zip(cs.iter_mut()) {
                sc.spawn(move || unsafe {
                    let t = Instant::now();
                    loop {
                        let k = next.fetch_add(1, Relaxed);
                        if k >= groups.len() || sh.links.full.load(Relaxed) {
                            break;
                        }
                        joined(&ids[groups[k].0..groups[k].1], env, all, x, st, w);
                    }
                    gate1.wait();
                    if x.ti == 0 {
                        // Odd-ID events of user numbers that had no run at all, and
                        // those users' clicks.
                        let ex = env.extras;
                        for a in 0..ex.len() {
                            if (a == 0 || ex[a - 1].0 >> 9 != ex[a].0 >> 9) && !env.taken[a].load(Relaxed) {
                                let g = (ex[a].0 >> 9) as u32;
                                let more = extras_of(env, g);
                                x.gen.next_user();
                                general_extras(env, x, w, more);
                                pool_clicks(env, w, g);
                            }
                        }
                    }
                    gate2.wait();
                    w.finish(sh);
                    // This thread's campaign counts join the general pipeline's.
                    fold_counts(&x.acc, st, env.ctx);
                    // The click index is done with. On small pages each thread returns
                    // its share now, so that exit does not have to, on one thread; on
                    // huge pages there is next to nothing to return and it is left.
                    if !kc_huge {
                        let part = (kc_bytes / nthreads) & !4095;
                        let lo = part * x.ti;
                        let hi = if x.ti + 1 == nthreads { kc_bytes } else { lo + part };
                        mem::release((env.kc as *mut u8).add(lo), hi - lo);
                    }
                    x.tm[5] += t.elapsed().as_nanos() as u64;
                });
            }
        });
    }
    let sum = |k: usize| tls.iter().map(|x| x.cnt[k]).sum::<u64>();
    let t_all = t0.elapsed();
    log(&format!(
        "stage: {} events in {} whole persons ({} sessions), {} events the general way, {} runs across buckets; setup {:.3}s, buckets {:.3}s, joining {:.3}s",
        sum(0),
        sum(2),
        sum(3),
        sum(1),
        groups.len(),
        t_setup.as_secs_f64(),
        (t_buckets - t_setup).as_secs_f64(),
        (t_all - t_buckets).as_secs_f64()
    ));
    if std::env::var_os("SS_TIME").is_some() {
        let ev = (sum(0) + sum(1)).max(1) as f64;
        let tm = |k: usize| tls.iter().map(|x| x.tm[k]).sum::<u64>() as f64;
        eprintln!(
            "  stage ns per event per thread: click index {:.1}, place {:.1}, read out {:.1}, cut {:.1}, runs {:.1}, joined runs {:.1} (wide kernels: {})",
            tm(4) / ev,
            tm(0) / ev,
            tm(1) / ev,
            tm(2) / ev,
            tm(3) / ev,
            tm(5) / ev,
            env.wide
        );
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Clk;

    struct Rnd(u64);
    impl Rnd {
        fn below(&mut self, m: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % m
        }
    }

    fn ctx(cats: &[Vec<u8>]) -> Ctx<'_> {
        // A 14-day window starting at a midnight: times are ms since 7 days before it.
        Ctx { ndays: 14, dayoff: -(WEEK as i64), organic: 37, day0: 20_000, cats }
    }

    /// Sessions of time-sorted events, the plain way: (start, end, events, purchases).
    fn sessions(ev: &[(u32, u32)]) -> (Vec<u32>, Vec<u32>, Vec<u32>, Vec<(u32, u32)>) {
        let (mut st, mut en, mut sn, mut pur) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (i, &(t, pc)) in ev.iter().enumerate() {
            if i == 0 || t - ev[i - 1].0 > MIN30 as u32 {
                st.push(t);
                en.push(t);
                sn.push(0);
            }
            *en.last_mut().unwrap() = t;
            *sn.last_mut().unwrap() += 1;
            if pc != 0 {
                pur.push((st.len() as u32 - 1, pc));
            }
        }
        (st, en, sn, pur)
    }

    fn events(r: &mut Rnd, n: usize) -> Vec<(u32, u32)> {
        let mut ev: Vec<(u32, u32)> = (0..n)
            .map(|_| {
                // Clustered times, with gaps around the 30-minute line.
                let base = WEEK + r.below(40) as u32 * 7_200_000 * 4;
                let t = base + r.below(4) as u32 * (MIN30 as u32 - 1 + r.below(3) as u32) + r.below(3) as u32 * 1000;
                let pc = if r.below(5) == 0 { (1 + r.below(50_000) as u32) | ((r.below(5) as u32) << 24) } else { 0 };
                (t.min(WEEK + 14 * DAY - 1), pc)
            })
            .collect();
        ev.sort_unstable();
        ev
    }

    #[test]
    fn added_events_resessionize() {
        let mut r = Rnd(0x1234_5678_9abc_def1);
        for round in 0..20_000 {
            let n = 1 + r.below(if round % 50 == 0 { 300 } else { 24 }) as usize;
            let ev = events(&mut r, n);
            // Any subset first, the rest added one by one in any order.
            let mut keep: Vec<(u32, u32)> = Vec::new();
            let mut later: Vec<(u32, u32)> = Vec::new();
            for (i, &e) in ev.iter().enumerate() {
                if i == 0 || r.below(4) != 0 {
                    keep.push(e);
                } else {
                    later.push(e);
                }
            }
            if later.is_empty() {
                continue;
            }
            for i in (1..later.len()).rev() {
                later.swap(i, r.below(i as u64 + 1) as usize);
            }
            let (st, en, sn, pur) = sessions(&keep);
            let mut s = Scr { st, en, sn, pur, ..Scr::default() };
            for &(t, pc) in &later {
                add_event(&mut s, t, pc);
            }
            let (st, en, sn, mut pur) = sessions(&ev);
            assert_eq!((&s.st, &s.sn, &s.en), (&st, &sn, &en), "round {round}");
            let mut got = s.pur.clone();
            assert!(got.windows(2).all(|w| w[0].0 <= w[1].0), "purchases by session");
            got.sort_unstable();
            pur.sort_unstable();
            assert_eq!(got, pur, "round {round}");
        }
    }

    #[test]
    fn person_matches_the_general_pipeline() {
        let cats: Vec<Vec<u8>> = [&b"apparel"[..], b"books", b"garden", b"toys", b"unknown"].iter().map(|c| c.to_vec()).collect();
        let ctx = ctx(&cats);
        let cells = (ctx.organic as usize + 1) * ctx.ndays;
        let mut r = Rnd(0x9abc_def1_1234_5678);
        let x = Tls::new(64, 0, &ctx);
        let wides: &[bool] = if wide_ok() { &[false, true] } else { &[false] };
        for round in 0..6_000 {
            // Mostly small persons; some with more than 64 and more than 256 sessions.
            let n = match round % 200 {
                0 => 3000,
                1 | 2 => 600,
                _ => 1 + r.below(40) as usize,
            };
            let mut ev = events(&mut r, n);
            if round % 200 == 0 {
                // Spread out: one session per event.
                for (i, e) in ev.iter_mut().enumerate() {
                    e.0 = (WEEK as u64 + i as u64 * (MIN30 as u64 + 1 + (i as u64 % 3))).min(u32::MAX as u64) as u32;
                }
                ev.retain(|e| e.0 < WEEK + 14 * DAY);
            }
            let (st, _, sn, pur) = sessions(&ev);
            let nclicks = match round % 7 {
                0 => 0,
                1 => 300,
                _ => r.below(30) as usize,
            };
            let mut ids: Vec<Id> = Vec::new();
            let mut kc: Vec<Kc> = Vec::new();
            for _ in 0..nclicks {
                // Around session starts: at them, a week before, 1 ms either side.
                let s0 = st[r.below(st.len() as u64) as usize];
                let ts = match r.below(8) {
                    0 => s0,
                    1 => s0 + 1,
                    2 => s0 - WEEK,
                    3 => s0 - WEEK - 1,
                    4 => s0 - r.below(WEEK as u64) as u32,
                    5 => r.below((21 * DAY) as u64) as u32,
                    _ => s0 - r.below(3_600_000) as u32,
                };
                let camp = if r.below(3) == 0 { r.below(3) as u32 } else { r.below(ctx.organic as u64) as u32 };
                let alias = r.below(4) as u32;
                let id = Id::intern(format!("k{}", r.below(1_000_000)).as_bytes());
                kc.push(Kc { ts, ca: (camp << 9) | alias, j: ids.len() as u32 });
                ids.push(id);
                if r.below(10) == 0 {
                    // A copy of the click, and another click at the same instant.
                    kc.push(Kc { ts, ca: (camp << 9) | alias, j: ids.len() as u32 });
                    ids.push(id);
                    let other = Id::intern(format!("k{}", r.below(1_000_000)).as_bytes());
                    kc.push(Kc { ts, ca: (r.below(ctx.organic as u64) as u32) << 9 | alias, j: ids.len() as u32 });
                    ids.push(other);
                }
            }
            let allowed = 0b1011u32;
            let name = user_text(r.below(1 << 40));
            // The general pipeline's person(): events and the person's clicks by time.
            let mut want = CS::new(cells);
            let evs: Vec<u64> = ev.iter().map(|e| ((e.0 as u64) << 32) | e.1 as u64).collect();
            let mut cl: Vec<Clk> = kc.iter().filter(|k| (allowed >> (k.ca & 511)) & 1 != 0).map(|k| Clk { ts: k.ts, camp: k.ca >> 9, cid: ids[k.j as usize] }).collect();
            cl.sort_by_key(|c| c.ts);
            crate::person(&mut want, name, &evs, &cl, &ctx);
            for &wide in wides {
                let mut got = CS::new(cells);
                let mut s = Scr { st: st.clone(), sn: sn.clone(), pur: pur.clone(), ..Scr::default() };
                unsafe {
                    person_rows(&mut got, &x.acc, &ctx, name, &mut s, &kc, allowed, wide, |j| ids[j as usize]);
                    fold_counts(&x.acc, &mut got, &ctx);
                    let rows = (ctx.organic as usize + 1) << x.acc.wsh;
                    std::ptr::write_bytes(x.acc.rev, 0, rows);
                    std::ptr::write_bytes(x.acc.sess, 0, rows);
                    std::ptr::write_bytes(x.acc.assist, 0, rows);
                    std::ptr::write_bytes(x.acc.purch, 0, rows);
                }
                let (a, b) = (&got.rows, &want.rows);
                assert_eq!(a.n, b.n, "round {round} wide {wide}");
                assert_eq!((&a.dict, &a.runs, a.ndict), (&b.dict, &b.runs, b.ndict), "round {round} wide {wide}");
                assert_eq!((&a.days, &a.sessions, &a.events), (&b.days, &b.sessions, &b.events), "round {round} wide {wide}");
                assert_eq!((&a.revenue, &a.top_bits, &a.tops), (&b.revenue, &b.top_bits, &b.tops), "round {round} wide {wide}");
                assert_eq!(got.credit, want.credit, "credit, round {round} wide {wide}");
                assert_eq!(got.assist, want.assist, "assists, round {round} wide {wide}");
            }
        }
    }

    #[test]
    fn kernels_agree() {
        if !wide_ok() {
            return;
        }
        let cats: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
        let ctx = ctx(&cats);
        let cap = 1024usize;
        let (mut a, mut b) = (Tls::new(cap, 0, &ctx), Tls::new(cap, 1, &ctx));
        let mut r = Rnd(0x0f1e_2d3c_4b5a_6978);
        for round in 0..3_000 {
            // A table as `place` leaves it: runs of users, times mostly rising, some
            // vacant, invalid and odd slots, far aliases, purchases, order breaks.
            let (mut g, mut t) = (r.below(1000), WEEK);
            let sparse = round % 5 == 0;
            for at in 0..cap {
                if r.below(12) == 0 {
                    g = if r.below(60) == 0 { g.saturating_sub(1 + r.below(3)) } else { g + 1 + r.below(3) };
                    t = WEEK + r.below(1000) as u32;
                }
                t = match r.below(40) {
                    0 => t + MIN30 as u32 + r.below(3) as u32 - 1,
                    1 => t.saturating_sub(r.below(2000) as u32).max(1),
                    _ => t + r.below(3) as u32 * 1000,
                };
                let alias = match r.below(30) {
                    0 => 15 + r.below(400),
                    1 => 14,
                    _ => r.below(3),
                };
                let buy = r.below(20) == 0;
                let slot = match r.below(if sparse { 3 } else { 60 }) {
                    0 => VACANT,
                    1 => Slot { a: INVALID | r.below(1 << 40), t: r.below(1 << 31) as u32, ing: 5 },
                    2 if !sparse => Slot { a: ODDU | r.below(1 << 23), t: t | if buy { BUY } else { 0 }, ing: 5 },
                    _ => Slot { a: (g << 9) | alias, t: t | if buy { BUY } else { 0 }, ing: 5 },
                };
                unsafe {
                    for x in [&mut a, &mut b] {
                        *x.slots.add(at) = slot;
                        *x.pcs.add(at) = if buy { 7 + at as u32 } else { 0xDEAD };
                    }
                }
            }
            for x in [&mut a, &mut b] {
                x.uns.clear();
                x.esc.clear();
                x.odd.clear();
            }
            unsafe {
                let (na, nb) = (readout_plain(&mut a, cap), readout_wide(&mut b, cap));
                assert_eq!(na, nb, "round {round}");
                let col = |p: *mut u32, n: usize| std::slice::from_raw_parts(p, n).to_vec();
                assert_eq!(col(a.t, na), col(b.t, nb), "times, round {round}");
                assert_eq!(col(a.u, na), col(b.u, nb), "users, round {round}");
                assert_eq!((&a.esc, &a.odd), (&b.esc, &b.odd), "round {round}");
                for i in 0..na {
                    if *a.t.add(i) & BUY != 0 {
                        assert_eq!(*a.pc.add(i), *b.pc.add(i), "price, round {round}");
                    }
                }
                for at in 0..cap {
                    let (sa, sb) = (*a.slots.add(at), *b.slots.add(at));
                    assert!(sa.a == !0 && sa.ing == !0 && sb.a == !0 && sb.ing == !0, "table left vacant");
                }
                if na == 0 {
                    continue;
                }
                let (ca, cb) = (cut_plain(&mut a, na), cut_wide(&mut b, nb));
                assert_eq!((ca.ns, ca.np, ca.down), (cb.ns, cb.np, cb.down), "round {round}");
                assert_eq!(col(a.ss, ca.ns + 1), col(b.ss, cb.ns + 1), "sessions, round {round}");
                assert_eq!(col(a.pi, ca.np), col(b.pi, cb.np), "purchases, round {round}");
                assert_eq!(a.uns, b.uns, "order breaks, round {round}");
            }
        }
    }

    #[test]
    fn names_order_like_text() {
        let mut r = Rnd(0x2468_ace1_1357_9bdf);
        let mut ns: Vec<u64> = (0..4000).map(|i| if i % 3 == 0 { r.below(40_000) } else { r.below(1 << 40) }).collect();
        ns.extend([0, 1, 35, 36, 1295, 1296, (1 << 40) - 1]);
        for &x in &ns {
            for &y in ns.iter().take(300) {
                let (a, b) = (user_text(x), user_text(y));
                assert_eq!(text_key(x) < text_key(y), a.less(&b), "{x} {y}");
            }
        }
        assert_eq!(user_text(0).bytes(), b"u0");
        assert_eq!(user_text(36 * 36 + 35).bytes(), b"u10z");
    }
}
