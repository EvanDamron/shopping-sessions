//! Timeline trace (test hook, SS_TL=path): spans per thread, written at exit as
//! lines "tid stage start end" (OS thread id, ns of CLOCK_MONOTONIC), for
//! tools that draw which thread does what when. Off by default; then each call
//! is one relaxed load.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};

pub static ON: AtomicBool = AtomicBool::new(false);
const CAP: usize = 1 << 16;
static N: AtomicUsize = AtomicUsize::new(0);
static REC: [[AtomicU64; 3]; CAP] = [const { [const { AtomicU64::new(0) }; 3] }; CAP];

thread_local! {
    static TID: u64 = tid();
}

/// The OS thread id (Linux); elsewhere a per-thread counter, which is enough to tell lanes apart.
#[cfg(target_os = "linux")]
fn tid() -> u64 {
    unsafe { libc::gettid() as u64 }
}

#[cfg(not(target_os = "linux"))]
fn tid() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Relaxed)
}

/// Now in ns (CLOCK_MONOTONIC), or 0 when tracing is off.
#[inline]
pub fn now() -> u64 {
    if !ON.load(Relaxed) {
        return 0;
    }
    let mut t = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) };
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}

/// Records a span of stage `what` on this thread, from `t0` (from `now()`) to now.
#[inline]
pub fn span(what: u8, t0: u64) {
    if !ON.load(Relaxed) {
        return;
    }
    let t1 = now();
    let i = N.fetch_add(1, Relaxed);
    if i < CAP {
        REC[i][0].store(TID.with(|t| *t) << 8 | what as u64, Relaxed);
        REC[i][1].store(t0, Relaxed);
        REC[i][2].store(t1, Relaxed);
    }
}

pub fn init() {
    if std::env::var_os("SS_TL").is_some() {
        ON.store(true, Relaxed);
    }
}

/// Writes the spans to the file named by SS_TL.
pub fn dump() {
    let Some(p) = std::env::var_os("SS_TL") else { return };
    let mut s = String::new();
    for r in REC.iter().take(N.load(Relaxed).min(CAP)) {
        let k = r[0].load(Relaxed);
        s.push_str(&format!("{} {} {} {}\n", k >> 8, k & 255, r[1].load(Relaxed), r[2].load(Relaxed)));
    }
    let _ = std::fs::write(p, s);
}
