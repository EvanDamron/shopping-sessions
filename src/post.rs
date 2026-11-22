//! After the parse: event-ID dedup (phase B) and, per person bucket, sessions,
//! credit, assists and user rows (phase C), on the records phase A filed.
//!
//! Dedup does not touch the person records: a losing copy's position in its
//! person bucket goes on a list, and phase C drops the listed records while it
//! reads the bucket, through a bitmap of the bucket's positions.

use crate::id::Id;
use crate::{begin_person, flush, sort_events, Clk, Ctx, Defer, Shared, Worker, CS, DAY, MIN30, NOSLOT, PCH, WEEK};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

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
                let _free = Defer(|| {
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
                            for j in 0..n {
                                unsafe {
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

/// One phase C thread's working memory.
#[derive(Default)]
pub struct Scr {
    off: Vec<u32>,  // per person of the bucket: start of their events (then, while filling, the next free place)
    coff: Vec<u32>, // the same for clicks
    ev: Vec<u64>,
    cl: Vec<Clk>,
    rtmp: Vec<u64>,
    dead: Vec<u64>,  // bitmap over the bucket's record positions
    stamp: Vec<u32>, // per campaign: the last session it assisted
    sid: u32,
    who: Vec<u32>, // the bucket's persons with events, in order
}

/// Persons with at most this many clicks get their assists from a window over
/// their time-ordered clicks; the rest by merging each campaign's intervals.
const SMALL: usize = 64;

/// Phase C for one person bucket, a range of person numbers: counting-sort its
/// events and clicks by person, sort each person's by time, and run the rules.
pub fn bucket(st: &mut CS, sc: &mut Scr, sh: &Shared, workers: &[Worker], kills: &Kills, ctx: &Ctx, p: usize) {
    let n_ev: usize = workers.iter().map(|w| w.event_count(p)).sum();
    if n_ev == 0 {
        return;
    }
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
    // The persons with events, in order (their names are prefetched from this list).
    sc.who.clear();
    for i in 0..np {
        if unsafe { *sc.off.get_unchecked(i + 1) != *sc.off.get_unchecked(i) } {
            sc.who.push(i as u32);
        }
    }
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
    sc.ev.clear();
    sc.ev.resize(sum as usize, 0);
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
    // After filing, off[i] and coff[i] are the ends of person i's rows; the starts
    // are the previous person's ends.
    let mut ev = std::mem::take(&mut sc.ev);
    let mut cl = std::mem::take(&mut sc.cl);
    let who = std::mem::take(&mut sc.who);
    if sc.stamp.len() <= ctx.organic as usize {
        sc.stamp.resize(ctx.organic as usize + 1, 0);
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
        person(st, sc, name, &mut ev[es..ee], &mut cl[cs..ce], ctx);
    }
    sc.ev = ev;
    sc.cl = cl;
    sc.who = who;
}

/// Click order for crediting: by time, and within one instant the smallest click_id
/// last, so the last click at or before a session start is the one it credits.
#[inline]
fn click_order(a: &Clk, b: &Clk) -> std::cmp::Ordering {
    a.ts.cmp(&b.ts).then_with(|| {
        if b.cid.less(&a.cid) {
            std::cmp::Ordering::Less
        } else if a.cid.less(&b.cid) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    })
}

/// One person's events and clicks (unsorted): sessions, credit, assists, daily rows
/// (rules.md "Events" steps 5 to 7 and "Clicks" steps 4 to 7).
#[inline]
fn person(st: &mut CS, sc: &mut Scr, name: Id, ev: &mut [u64], cl: &mut [Clk], ctx: &Ctx) {
    if ev.len() >= 1024 {
        sort_events(ev, &mut sc.rtmp);
    } else {
        ev.sort_unstable();
    }
    if cl.len() > 1 {
        cl.sort_unstable_by(click_order);
    }
    let ndays = ctx.ndays;
    begin_person(st, name);
    let small = cl.len() <= SMALL;
    st.starts.clear();
    st.days.clear();
    let mut k = 0;
    let (mut cj, mut ct) = (0usize, 0usize);
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
        // Credit: the latest click at or before the start, within 7 days (the smallest
        // click_id of that instant is the last of it in `cl`).
        while cj < cl.len() && (cl[cj].ts as i64) <= start {
            cj += 1;
        }
        let camp = if cj > 0 && start - cl[cj - 1].ts as i64 <= WEEK { cl[cj - 1].camp } else { ctx.organic };
        let c = &mut st.credit[camp as usize * ndays + day as usize];
        c[0] += 1 + ((purch as u64) << 32);
        c[1] += rev;
        if small {
            // Assists: every campaign of a click in [start - 7 days, start], once.
            while ct < cj && (cl[ct].ts as i64) < start - WEEK {
                ct += 1;
            }
            if ct < cj {
                sc.sid = sc.sid.wrapping_add(1);
                let sid = sc.sid;
                for x in &cl[ct..cj] {
                    let s = unsafe { sc.stamp.get_unchecked_mut(x.camp as usize) };
                    if *s != sid {
                        *s = sid;
                        st.assist[x.camp as usize * ndays + day as usize] += 1;
                    }
                }
            }
        } else {
            st.starts.push(start);
            st.days.push(day as u32);
        }
    }
    flush(st, name, cur_day + ctx.day0, us, ue, ur, ctx);
    if small {
        return;
    }

    // Assists of a person with many clicks: per campaign, merge [t, t + 7d]
    // intervals; count the sessions inside them per day.
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
