//! After the parse: event-ID dedup (phase B) and, per person bucket, sessions,
//! credit, assists and user rows (phase C), on the records phase A filed.
//!
//! Dedup does not touch the person records: a losing copy's position in its
//! person bucket goes on a list, and phase C drops the listed records while it
//! reads the bucket, through a bitmap of the bucket's positions.

use crate::id::Id;
use crate::{begin_person, flush, Clk, Ctx, Defer, Shared, Worker, CS, DAY, MIN30, NOSLOT, PCH, WEEK};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

/// CPU time of this thread so far, in ns.
pub fn thread_cpu_ns() -> u64 {
    #[cfg(unix)]
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts);
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }
    #[cfg(not(unix))]
    0
}

/// Thread CPU time spent in dedup and in the person buckets, summed over threads (ns).
pub static CPU_B: AtomicU64 = AtomicU64::new(0);
pub static CPU_C: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// Losing copies, per dedup thread and person bucket: positions in the bucket's
/// records, counted over its chunks in phase C's reading order (worker by worker,
/// chunk by chunk), PCH positions per chunk.
pub struct Kills {
    lists: Vec<Vec<Vec<u32>>>,
}

/// For each event chunk (slot / PCH): its person bucket << 32 | its first position
/// in that bucket. Every chunk of the event arena is PCH records, PCH-aligned.
fn chunk_map(sh: &Shared, workers: &[Worker], pb: usize) -> Vec<u64> {
    let top = workers.iter().flat_map(|w| w.pchunks.iter().flatten()).map(|&s| s as usize / PCH + 1).max().unwrap_or(0);
    let mut m = vec![0u64; top];
    for p in 0..pb {
        let mut ord = 0u64;
        for w in workers {
            for &s in &w.pchunks[p] {
                debug_assert!(s as usize % PCH == 0);
                m[s as usize / PCH] = (p as u64) << 32 | ord * PCH as u64;
                ord += 1;
            }
        }
    }
    let _ = sh;
    m
}

/// Keys ahead of the current one whose table slot is loaded early.
const KAHEAD: usize = 12;

/// Phase B: per key bucket, the copy with the smallest ingest_ts wins (rules.md
/// "Events" step 2); the others are listed for phase C to drop.
pub fn dedup(sh: &Shared, workers: &[Worker], nthreads: usize, pb: usize, db: usize) -> Kills {
    let cmap = chunk_map(sh, workers, pb);
    let mut lists: Vec<Vec<Vec<u32>>> = (0..nthreads).map(|_| (0..pb).map(|_| Vec::new()).collect()).collect();
    let next = AtomicUsize::new(0);
    let done = std::sync::Barrier::new(nthreads);
    let (next, done, cmap) = (&next, &done, &cmap);
    std::thread::scope(|sc| {
        for (ti, kl) in lists.iter_mut().enumerate() {
            sc.spawn(move || {
                // Once every thread is done with the keys, each gives back its share.
                let c0 = thread_cpu_ns();
                let _free = Defer(|| {
                    CPU_B.fetch_add(thread_cpu_ns() - c0, Relaxed);
                    done.wait();
                    sh.dk.release_part(ti, nthreads);
                });
                let mut tab: Vec<[u64; 2]> = Vec::new();
                let mut used: Vec<u64> = Vec::new();
                let mut kill = |slot: u32| {
                    if slot != NOSLOT {
                        let m = unsafe { *cmap.get_unchecked(slot as usize / PCH) };
                        unsafe { kl.get_unchecked_mut((m >> 32) as usize) }.push(m as u32 | (slot & (PCH as u32 - 1)));
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
                    let tp = tab.as_mut_ptr();
                    for w in workers {
                        for chunk in w.key_chunks(d) {
                            let n = chunk.len();
                            let cp = chunk.as_ptr();
                            for j in 0..n {
                                unsafe {
                                    if j % 4 == 0 {
                                        // The key stream itself, a kilobyte ahead (chunks are 16 KB).
                                        prefetch(cp.wrapping_add(j + 64));
                                    }
                                    if j + KAHEAD < n {
                                        prefetch(tp.add((chunk.get_unchecked(j + KAHEAD)[0] >> shift) as usize));
                                    }
                                    let [id, v] = *chunk.get_unchecked(j);
                                    let mut s = (id >> shift) as usize;
                                    loop {
                                        let (word, bit) = (s >> 6, 1u64 << (s & 63));
                                        if *used.get_unchecked(word) & bit == 0 {
                                            *used.get_unchecked_mut(word) |= bit;
                                            *tp.add(s) = [id, v];
                                            break;
                                        }
                                        let o = &mut *tp.add(s);
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
                                        s = (s + 1) & mask;
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    });
    Kills { lists }
}

#[inline(always)]
fn tsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(not(target_arch = "x86_64"))]
    0
}

/// Time stamp counter ticks per section of phase C, summed over threads: bitmap,
/// event count, click count, person list, click filing, event filing, persons
/// (of which: click order, sessions and rows, assists of persons with many clicks).
pub static SECT: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];

#[inline(always)]
fn lap(t: &mut u64, acc: &mut u64) {
    let n = tsc();
    *acc += n - *t;
    *t = n;
}

/// One phase C thread's working memory.
#[derive(Default)]
pub struct Scr {
    off: Vec<u32>,  // per person of the bucket: start of their events (then, while filling, the next free place)
    coff: Vec<u32>, // the same for clicks
    ev: Vec<u64>,
    cl: Vec<Clk>,
    dead: Vec<u64>,  // bitmap over the bucket's record positions
    stamp: Vec<u32>, // per campaign: the last session it assisted
    sid: u32,
    who: Vec<u32>, // the bucket's persons with events, in order
    ck: Vec<u64>,  // the current person's clicks, `ts << 32 | campaign`, in crediting order
    keys: Vec<u64>,
    rtmp: Vec<u64>,
    lo: Vec<i64>, // per campaign: the interval being merged (assists_many)
    hi: Vec<i64>,
    open: Vec<u32>,
    occ: Vec<u64>,  // per half-hour slot: whether the current person has an event in it
    agg: Vec<Agg>,  // per occupied slot: that person's events in it
    purch: Vec<u64>, // the current person's purchases, `t << 32 | pc`
    words: (usize, usize), // the words of `occ` events can fall in
    sfirst: Vec<u32>, // assists_many: per slot, the first session starting in it or later
    nday: Vec<u32>,   // assists_many: per session, the first later session on another day
    sect: [u64; 10],
}

/// A person's events in one half-hour slot.
#[derive(Clone, Copy, Default)]
struct Agg {
    lo: u32,
    hi: u32,
    n: u32,
}

/// Persons with at most this many clicks get their assists from a window over
/// their time-ordered clicks; the rest by merging each campaign's intervals.
const SMALL: usize = 64;

/// Half-hour slots of time (ms since the click window's start, below 2^31). Two
/// events of a person in one slot are at most 30 minutes apart, so they are in one
/// session; a session ends between two slots exactly when the first event of the
/// later one is more than 30 minutes after the last event of the earlier one. Two
/// sessions never start in one slot.
const SLOT: u32 = MIN30 as u32;
const NSLOT: usize = (1usize << 31) / SLOT as usize + 1;

#[inline(always)]
fn slot(t: u32) -> usize {
    (t / SLOT) as usize
}

/// Phase C for one person bucket, a range of person numbers: counting-sort its
/// events and clicks by person, then run the rules person by person.
pub fn bucket(st: &mut CS, sc: &mut Scr, sh: &Shared, workers: &[Worker], kills: &Kills, ctx: &Ctx, p: usize) {
    let n_ev: usize = workers.iter().map(|w| w.event_count(p)).sum();
    if n_ev == 0 {
        return;
    }
    let mut tc = tsc();
    let first = (p as u32) << sh.pshift; // the bucket's first person number
    let np = 1usize << sh.pshift;
    // The losing copies' positions, as a bitmap.
    let nchunks: usize = workers.iter().map(|w| w.pchunks[p].len()).sum();
    sc.dead.clear();
    sc.dead.resize(nchunks * PCH / 64, 0);
    for kl in &kills.lists {
        for &x in &kl[p] {
            unsafe { *sc.dead.get_unchecked_mut(x as usize >> 6) |= 1 << (x & 63) };
        }
    }
    lap(&mut tc, &mut sc.sect[0]);
    // Events per person, then each person's start in `ev`.
    sc.off.clear();
    sc.off.resize(np + 1, 0);
    let mut base = 0usize;
    for w in workers {
        for chunk in w.event_chunks(sh, p) {
            let bits = &sc.dead[base / 64..];
            for (k, e) in chunk.iter().enumerate() {
                // A dead copy counts for nobody (the count is shifted out of the last slot).
                let dead = unsafe { *bits.get_unchecked(k >> 6) } >> (k & 63) & 1 != 0;
                let i = if dead { np } else { (e.who - first) as usize };
                unsafe { *sc.off.get_unchecked_mut(i) += 1 };
            }
            base += PCH;
        }
    }
    let mut sum = 0u32;
    for x in sc.off[..np].iter_mut() {
        let n = *x;
        *x = sum;
        sum += n;
    }
    sc.off[np] = sum;
    lap(&mut tc, &mut sc.sect[1]);
    // Clicks per person; a click of a person with no surviving event credits nothing.
    sc.coff.clear();
    sc.coff.resize(np + 1, 0);
    for w in workers {
        for chunk in w.click_chunks(p) {
            for c in chunk {
                let i = (c.who - first) as usize;
                let has = unsafe { *sc.off.get_unchecked(i + 1) != *sc.off.get_unchecked(i) };
                unsafe { *sc.coff.get_unchecked_mut(if has { i } else { np }) += 1 };
            }
        }
    }
    let mut csum = 0u32;
    for x in sc.coff[..np].iter_mut() {
        let n = *x;
        *x = csum;
        csum += n;
    }
    sc.coff[np] = csum;
    lap(&mut tc, &mut sc.sect[2]);
    // The persons with events, in order (their names are prefetched from this list).
    sc.who.clear();
    for i in 0..np {
        if unsafe { *sc.off.get_unchecked(i + 1) != *sc.off.get_unchecked(i) } {
            sc.who.push(i as u32);
        }
    }
    lap(&mut tc, &mut sc.sect[3]);
    // File the clicks first: it needs the event starts untouched to tell who has events.
    sc.cl.clear();
    sc.cl.resize(csum as usize, Clk { ts: 0, camp: 0, cid: Id::EMPTY });
    for w in workers {
        for chunk in w.click_chunks(p) {
            for c in chunk {
                let i = (c.who - first) as usize;
                unsafe {
                    if *sc.off.get_unchecked(i + 1) != *sc.off.get_unchecked(i) {
                        let at = sc.coff.get_unchecked_mut(i);
                        *sc.cl.get_unchecked_mut(*at as usize) = Clk { ts: c.ts, camp: c.camp, cid: c.cid };
                        *at += 1;
                    }
                }
            }
        }
    }
    lap(&mut tc, &mut sc.sect[4]);
    if sc.ev.len() < sum as usize {
        sc.ev.resize(sum as usize, 0);
    }
    let mut base = 0usize;
    for w in workers {
        for chunk in w.event_chunks(sh, p) {
            let bits = &sc.dead[base / 64..];
            for (k, e) in chunk.iter().enumerate() {
                let dead = unsafe { *bits.get_unchecked(k >> 6) } >> (k & 63) & 1 != 0;
                if !dead {
                    unsafe {
                        let at = sc.off.get_unchecked_mut((e.who - first) as usize);
                        *sc.ev.get_unchecked_mut(*at as usize) = ((e.t as u64) << 32) | e.pc as u64;
                        *at += 1;
                    }
                }
            }
            base += PCH;
        }
    }
    lap(&mut tc, &mut sc.sect[5]);
    // After filing, off[i] and coff[i] are the ends of person i's rows; the starts
    // are the previous person's ends.
    let ev = std::mem::take(&mut sc.ev);
    let cl = std::mem::take(&mut sc.cl);
    let who = std::mem::take(&mut sc.who);
    if sc.stamp.len() <= ctx.organic as usize {
        sc.stamp.resize(ctx.organic as usize + 1, 0);
    }
    if sc.occ.is_empty() {
        sc.occ.resize(NSLOT / 64 + 1, 0);
        sc.agg.resize(NSLOT, Agg::default());
        // Valid events lie in [window_start, window_end): these words of `occ`.
        sc.words = (slot(WEEK as u32) / 64, slot((WEEK as u64 + sh.evspan - 1) as u32) / 64 + 1);
    }
    const NAHEAD: usize = 8;
    for &i in who.iter().take(NAHEAD) {
        sh.links.prefetch_slot(first + i);
    }
    for (k, &i) in who.iter().enumerate() {
        if let Some(&j) = who.get(k + NAHEAD) {
            sh.links.prefetch_slot(first + j);
        }
        let i = i as usize;
        let (es, ee) = (if i == 0 { 0 } else { sc.off[i - 1] as usize }, sc.off[i] as usize);
        let (cs, ce) = (if i == 0 { 0 } else { sc.coff[i - 1] as usize }, sc.coff[i] as usize);
        let name = sh.links.name(first + i as u32);
        person(st, sc, name, &ev[es..ee], &cl[cs..ce], ctx);
    }
    sc.ev = ev;
    sc.cl = cl;
    sc.who = who;
    lap(&mut tc, &mut sc.sect[6]);
    for (a, x) in SECT.iter().zip(sc.sect.iter_mut()) {
        a.fetch_add(*x, Relaxed);
        *x = 0;
    }
}

/// A person's clicks as `ts << 32 | campaign`, in crediting order: by time, and
/// within one instant the smallest click_id last, so the last click at or before a
/// session start is the one that session credits (rules.md "Clicks" step 4).
fn order_clicks(cl: &[Clk], keys: &mut Vec<u64>, rtmp: &mut Vec<u64>, out: &mut Vec<u64>) {
    out.clear();
    if cl.len() == 1 {
        out.push((cl[0].ts as u64) << 32 | cl[0].camp as u64);
        return;
    }
    keys.clear();
    keys.extend(cl.iter().enumerate().map(|(j, c)| (c.ts as u64) << 32 | j as u64));
    crate::sort_events(keys, rtmp);
    let n = keys.len();
    let mut a = 0;
    while a < n {
        let t = keys[a] >> 32;
        let mut b = a + 1;
        while b < n && keys[b] >> 32 == t {
            b += 1;
        }
        if b - a > 1 {
            // Clicks at one instant (copies of a click, or the generator's same-millisecond
            // pairs): the largest click_id first. Insertion sort; these groups are small.
            let g = &mut keys[a..b];
            for x in 1..g.len() {
                let mut y = x;
                while y > 0 && cl[g[y - 1] as u32 as usize].cid.less(&cl[g[y] as u32 as usize].cid) {
                    g.swap(y - 1, y);
                    y -= 1;
                }
            }
        }
        a = b;
    }
    out.extend(keys.iter().map(|&k| (k >> 32) << 32 | cl[k as u32 as usize].camp as u64));
}

/// One person's events and clicks (both unordered): sessions, credit, assists,
/// daily rows (rules.md "Events" steps 5 to 7 and "Clicks" steps 4 to 7).
#[inline]
fn person(st: &mut CS, sc: &mut Scr, name: Id, ev: &[u64], cl: &[Clk], ctx: &Ctx) {
    let mut tp = tsc();
    let mut ck = std::mem::take(&mut sc.ck);
    if cl.is_empty() {
        ck.clear();
    } else {
        order_clicks(cl, &mut sc.keys, &mut sc.rtmp, &mut ck);
    }
    lap(&mut tp, &mut sc.sect[7]);
    // The person's events by half-hour slot: first and last time and count per
    // occupied slot; purchases aside.
    let mut purch = std::mem::take(&mut sc.purch);
    purch.clear();
    for &e in ev {
        let t = (e >> 32) as u32;
        let s = slot(t);
        let (w, b) = (s >> 6, 1u64 << (s & 63));
        unsafe {
            let o = sc.occ.get_unchecked_mut(w);
            let seen = *o & b != 0;
            *o |= b;
            let a = sc.agg.get_unchecked_mut(s);
            if seen {
                a.lo = a.lo.min(t);
                a.hi = a.hi.max(t);
                a.n += 1;
            } else {
                *a = Agg { lo: t, hi: t, n: 1 };
            }
        }
        if e as u32 & 0x00FF_FFFF != 0 {
            purch.push(e);
        }
    }
    if purch.len() > 1 {
        purch.sort_unstable();
    }
    begin_person(st, name);
    let mut sp = Sess { small: ck.len() <= SMALL, ..Sess::default() };
    st.starts.clear();
    st.days.clear();
    st.catrev.clear();
    let mut cur_day: i64 = -1;
    let mut prev_max = i64::MIN / 2;
    let (mut start, mut sday, mut scnt, mut srev) = (0i64, 0i64, 0u32, 0u64);
    let mut pi = 0;
    // The occupied slots in time order.
    for w in sc.words.0..sc.words.1 {
        let mut bits = unsafe { *sc.occ.get_unchecked(w) };
        if bits == 0 {
            continue;
        }
        unsafe { *sc.occ.get_unchecked_mut(w) = 0 };
        while bits != 0 {
            let s = w * 64 + bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let a = unsafe { *sc.agg.get_unchecked(s) };
            if a.lo as i64 - prev_max > MIN30 {
                if scnt > 0 {
                    end_session(st, sc, &mut sp, &ck, ctx, start, sday, scnt, srev);
                }
                start = a.lo as i64;
                sday = (start + ctx.dayoff).div_euclid(DAY);
                if sday != cur_day {
                    if cur_day >= 0 {
                        flush(st, name, cur_day + ctx.day0, sp.us, sp.ue, sp.ur, ctx);
                    }
                    cur_day = sday;
                    sp.us = 0;
                    sp.ue = 0;
                    sp.ur = 0;
                }
                scnt = 0;
                srev = 0;
            }
            scnt += a.n;
            prev_max = a.hi as i64;
            // This slot's purchases.
            while pi < purch.len() && slot((purch[pi] >> 32) as u32) == s {
                let pc = purch[pi] as u32;
                let (price, cat) = (pc & 0x00FF_FFFF, pc >> 24);
                srev += price as u64;
                match st.catrev.iter_mut().find(|c| c.0 == cat) {
                    Some(c) => c.1 += price as u64,
                    None => st.catrev.push((cat, price as u64)),
                }
                pi += 1;
            }
        }
    }
    debug_assert_eq!(pi, purch.len());
    end_session(st, sc, &mut sp, &ck, ctx, start, sday, scnt, srev);
    flush(st, name, cur_day + ctx.day0, sp.us, sp.ue, sp.ur, ctx);
    sc.purch = purch;
    lap(&mut tp, &mut sc.sect[8]);
    if !sp.small {
        assists_many(st, sc, &ck, ctx.ndays);
    }
    lap(&mut tp, &mut sc.sect[9]);
    sc.ck = ck;
}

/// A person's running state across sessions: the day row so far and where the
/// click pointers are.
#[derive(Default)]
struct Sess {
    us: u32,
    ue: u32,
    ur: u64,
    cj: usize, // clicks at or before the current session's start
    ct: usize, // clicks more than 7 days before it
    small: bool,
}

/// One finished session: its day row, credit, and (for a person with few clicks) assists.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn end_session(st: &mut CS, sc: &mut Scr, sp: &mut Sess, ck: &[u64], ctx: &Ctx, start: i64, day: i64, n: u32, rev: u64) {
    let ndays = ctx.ndays;
    sp.us += 1;
    sp.ue += n;
    sp.ur += rev;
    // Credit: the latest click at or before the start, within 7 days.
    let mut cj = sp.cj;
    while cj < ck.len() && (ck[cj] >> 32) as i64 <= start {
        cj += 1;
    }
    sp.cj = cj;
    let camp = if cj > 0 && start - (ck[cj - 1] >> 32) as i64 <= WEEK { ck[cj - 1] as u32 } else { ctx.organic };
    let c = &mut st.credit[camp as usize * ndays + day as usize];
    c[0] += 1 + (((rev > 0) as u64) << 32);
    c[1] += rev;
    if sp.small {
        // Assists: every campaign of a click in [start - 7 days, start], once.
        let mut ct = sp.ct;
        while ct < cj && ((ck[ct] >> 32) as i64) < start - WEEK {
            ct += 1;
        }
        sp.ct = ct;
        if ct < cj {
            sc.sid = sc.sid.wrapping_add(1);
            let sid = sc.sid;
            for &x in &ck[ct..cj] {
                let s = unsafe { sc.stamp.get_unchecked_mut(x as u32 as usize) };
                if *s != sid {
                    *s = sid;
                    st.assist[x as u32 as usize * ndays + day as usize] += 1;
                }
            }
        }
    } else {
        st.starts.push(start);
        st.days.push(day as u32);
    }
}

/// Assists of a person with many clicks: each campaign's clicks, in time order,
/// make merged intervals [t, t + 7 days], and the sessions starting inside an
/// interval are counted per day. Sessions are found through a table from half-hour
/// slot to the first session starting in it or later (at most one starts per slot).
fn assists_many(st: &mut CS, sc: &mut Scr, ck: &[u64], ndays: usize) {
    if sc.lo.len() < sc.stamp.len() {
        sc.lo.resize(sc.stamp.len(), 0);
        sc.hi.resize(sc.stamp.len(), 0);
    }
    let ns = st.starts.len();
    if sc.sfirst.len() < NSLOT + 1 {
        sc.sfirst.resize(NSLOT + 1, 0);
    }
    {
        let mut i = ns;
        for s in (0..=NSLOT).rev() {
            while i > 0 && slot(st.starts[i - 1] as u32) >= s {
                i -= 1;
            }
            sc.sfirst[s] = i as u32;
        }
    }
    sc.nday.clear();
    sc.nday.resize(ns, ns as u32);
    for i in (0..ns.saturating_sub(1)).rev() {
        sc.nday[i] = if st.days[i + 1] != st.days[i] { i as u32 + 1 } else { sc.nday[i + 1] };
    }
    sc.sid = sc.sid.wrapping_add(1);
    let sid = sc.sid;
    let mut open = std::mem::take(&mut sc.open);
    open.clear();
    let (sfirst, nday) = (&sc.sfirst, &sc.nday);
    let count = |st: &mut CS, camp: usize, lo: i64, hi: i64| {
        let s = &st.starts;
        // The first session starting at or after lo, and the first after hi.
        let mut i0 = sfirst[slot(lo as u32)] as usize;
        if i0 < ns && s[i0] < lo {
            i0 += 1;
        }
        let mut i1 = if hi >= 1 << 31 { ns } else { sfirst[slot(hi as u32)] as usize };
        if i1 < ns && s[i1] <= hi {
            i1 += 1;
        }
        while i0 < i1 {
            let j = (nday[i0] as usize).min(i1);
            st.assist[camp * ndays + st.days[i0] as usize] += (j - i0) as u32;
            i0 = j;
        }
    };
    for &x in ck {
        let (t, camp) = ((x >> 32) as i64, x as u32 as usize);
        if sc.stamp[camp] == sid {
            if t <= sc.hi[camp] {
                sc.hi[camp] = t + WEEK;
                continue;
            }
            count(st, camp, sc.lo[camp], sc.hi[camp]);
        } else {
            sc.stamp[camp] = sid;
            open.push(camp as u32);
        }
        sc.lo[camp] = t;
        sc.hi[camp] = t + WEEK;
    }
    for &camp in &open {
        let camp = camp as usize;
        count(st, camp, sc.lo[camp], sc.hi[camp]);
    }
    sc.open = open;
}
