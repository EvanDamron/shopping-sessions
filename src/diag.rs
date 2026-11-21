//! Scorer diagnostics (this branch only): environment and micro-timings to stderr.

use std::io::Read;
use std::time::Instant;

fn show(label: &str, path: &str, max: usize) {
    match std::fs::read_to_string(path) {
        Ok(s) => eprintln!("DIAG {label}: {}", s.trim().chars().take(max).collect::<String>().replace('\n', " | ")),
        Err(e) => eprintln!("DIAG {label}: unreadable ({e})"),
    }
}

/// One extra campaign_daily row: (campaign_id text, channel text, four numbers).
pub type Row = (String, String, [u64; 4]);

fn read1(path: &str) -> String {
    std::fs::read_to_string(path).map(|s| s.trim().replace('\n', " ")).unwrap_or_else(|_| "NA".into())
}

fn touch_ms(bytes: usize, threads: usize, huge: bool) -> u64 {
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(move || unsafe {
                let n = bytes / threads;
                let p = libc::mmap(std::ptr::null_mut(), n + (2 << 20), libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) as usize;
                if p as isize == -1 {
                    return;
                }
                let a = (p + (2 << 20) - 1) & !((2 << 20) - 1);
                #[cfg(target_os = "linux")]
                if huge {
                    libc::madvise(a as *mut libc::c_void, n, libc::MADV_HUGEPAGE);
                }
                let _ = huge;
                let mut i = 0;
                while i < n {
                    (a as *mut u8).add(i).write_volatile(1);
                    i += 4096;
                }
            });
        }
    });
    t0.elapsed().as_millis() as u64
}

/// Short labeled facts as extra campaign_daily rows (most valuable first), also logged.
pub fn rows(data: &std::path::Path, nthreads: usize) -> Vec<Row> {
    let mut out: Vec<Row> = Vec::new();
    let cpu = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = cpu.lines().find(|l| l.starts_with("model name")).and_then(|l| l.split(':').nth(1)).unwrap_or("NA").trim().to_string();
    let flags = cpu.lines().find(|l| l.starts_with("flags")).unwrap_or("").to_string();
    let has = |f: &str| flags.split_whitespace().any(|x| x == f) as u64;
    let nproc = cpu.matches("processor\t").count() as u64;
    let mhz = cpu.lines().find(|l| l.starts_with("cpu MHz")).and_then(|l| l.split(':').nth(1)).and_then(|x| x.trim().parse::<f64>().ok()).unwrap_or(0.0) as u64;
    out.push((format!("DIAG1 cpu={model}"), format!("mhz={mhz}"), [nproc, mhz, has("avx512f"), has("avx512bw")]));
    let thp = format!("{}/{}", read1("/sys/kernel/mm/transparent_hugepage/enabled"), read1("/sys/kernel/mm/transparent_hugepage/defrag"));
    let thp_code = if thp.starts_with("[always]") { 1 } else if thp.contains("[madvise]") { 2 } else if thp.contains("[never]") { 3 } else { 0 };
    out.push((format!("DIAG2 thp={thp}"), "thp".into(), [thp_code, 0, 0, 0]));
    let f1 = touch_ms(1 << 30, 1, false);
    let f8 = touch_ms(1 << 30, nthreads, false);
    let h1 = touch_ms(1 << 30, 1, true);
    let h8 = touch_ms(1 << 30, nthreads, true);
    out.push((format!("DIAG3 fault_1GB_ms 1t={f1} {nthreads}t={f8} huge1t={h1} huge{nthreads}t={h8}"), "fault".into(), [f1, f8, h1, h8]));
    out.push((
        format!("DIAG4 flags avx2:{} avx512f:{} bw:{} vbmi2:{} bmi2:{}", has("avx2"), has("avx512f"), has("avx512bw"), has("avx512_vbmi2"), has("bmi2")),
        "flags".into(),
        [has("avx2"), has("avx512f"), has("avx512_vbmi2"), has("bmi2")],
    ));
    let cg_cpu = read1("/sys/fs/cgroup/cpu.max");
    let cg_mem = read1("/sys/fs/cgroup/memory.max");
    let cpus = read1("/sys/fs/cgroup/cpuset.cpus.effective");
    let par = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0) as u64;
    out.push((format!("DIAG5 nproc={nproc} par={par} cpu.max={cg_cpu} mem={cg_mem} cpus={cpus}"), "cgroup".into(), [nproc, par, 0, 0]));
    let bi = std::fs::read_to_string("/proc/buddyinfo");
    let mut free2m = 0u64;
    if let Ok(s) = &bi {
        for line in s.lines() {
            let c: Vec<u64> = line.split_whitespace().skip(4).filter_map(|x| x.parse().ok()).collect();
            for (o, &k) in c.iter().enumerate().skip(9) {
                free2m += k << (o - 9);
            }
        }
    }
    let memfree = std::fs::read_to_string("/proc/meminfo").ok().and_then(|s| s.lines().find(|l| l.starts_with("MemFree")).map(|l| l.to_string())).unwrap_or_default();
    out.push((format!("DIAG6 buddyinfo={} free2M={free2m} {memfree}", bi.is_ok() as u8), "mem".into(), [bi.is_ok() as u64, free2m, 0, 0]));
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    let fs = |m: &str| mounts.lines().find(|l| l.split_whitespace().nth(1) == Some(m)).and_then(|l| l.split_whitespace().nth(2)).unwrap_or("NA").to_string();
    out.push((format!("DIAG7 fs data={} out={} scratch={} root={}", fs("/data"), fs("/output"), fs("/scratch"), fs("/")), "fs".into(), [0; 4]));
    let t0 = Instant::now();
    let mut total = 0u64;
    let mut buf = vec![0u8; 64 << 10];
    if let Ok(rd) = std::fs::read_dir(data.join("events")) {
        for e in rd.flatten() {
            if let Ok(mut f) = std::fs::File::open(e.path()) {
                while let Ok(k) = f.read(&mut buf) {
                    if k == 0 {
                        break;
                    }
                    total += k as u64;
                }
            }
        }
    }
    let rd_ms = t0.elapsed().as_millis() as u64;
    let kver = read1("/proc/sys/kernel/osrelease");
    let init = read1("/proc/cmdline");
    let ioa = init.split_whitespace().find(|x| x.starts_with("init_on_alloc")).unwrap_or("init_on_alloc=NA").to_string();
    out.push((format!("DIAG8 read_dev_ms={rd_ms} bytes={total} kernel={kver} {ioa}"), "io".into(), [rd_ms, total, 0, 0]));
    for r in &out {
        eprintln!("{} | {} | {:?}", r.0, r.1, r.2);
    }
    out
}

pub fn report(data: &std::path::Path, nthreads: usize) {
    let t = Instant::now();
    if let Ok(s) = std::fs::read_to_string("/proc/cpuinfo") {
        let model = s.lines().find(|l| l.starts_with("model name")).unwrap_or("");
        let flags = s.lines().find(|l| l.starts_with("flags")).unwrap_or("");
        let has = |f: &str| flags.split_whitespace().any(|x| x == f);
        eprintln!(
            "DIAG cpu: {model} | procs {} | avx2 {} avx512f {} avx512bw {} avx512vbmi2 {} bmi2 {} mhz {}",
            s.matches("processor").count(),
            has("avx2"),
            has("avx512f"),
            has("avx512bw"),
            has("avx512_vbmi2"),
            has("bmi2"),
            s.lines().find(|l| l.starts_with("cpu MHz")).unwrap_or("")
        );
    }
    eprintln!("DIAG available_parallelism: {:?}, NCPU {nthreads}", std::thread::available_parallelism());
    show("cpu.max", "/sys/fs/cgroup/cpu.max", 100);
    show("cpuset", "/sys/fs/cgroup/cpuset.cpus.effective", 100);
    show("memory.max", "/sys/fs/cgroup/memory.max", 100);
    show("thp enabled", "/sys/kernel/mm/transparent_hugepage/enabled", 100);
    show("thp defrag", "/sys/kernel/mm/transparent_hugepage/defrag", 100);
    show("buddyinfo", "/proc/buddyinfo", 600);
    show("meminfo", "/proc/meminfo", 700);
    show("cmdline", "/proc/cmdline", 400);
    show("version", "/proc/version", 200);
    if let Ok(m) = std::fs::read_to_string("/proc/mounts") {
        for l in m.lines().filter(|l| l.contains(" /data") || l.contains(" /output") || l.contains(" /scratch") || l.contains(" / ")) {
            eprintln!("DIAG mount: {l}");
        }
    }
    // Fresh anonymous memory: 1 GB touched by one thread, then by all threads.
    for threads in [1usize, nthreads] {
        let t0 = Instant::now();
        std::thread::scope(|sc| {
            for _ in 0..threads {
                sc.spawn(move || {
                    let n = (1usize << 30) / threads;
                    let mut v: Vec<u8> = Vec::with_capacity(n);
                    unsafe {
                        let p = v.as_mut_ptr();
                        let mut i = 0;
                        while i < n {
                            p.add(i).write(1);
                            i += 4096;
                        }
                        v.set_len(n);
                    }
                    std::hint::black_box(&v);
                });
            }
        });
        eprintln!("DIAG touch 1 GB fresh memory with {threads} threads: {:.3} s", t0.elapsed().as_secs_f64());
    }
    // Reading the event files: one pass with read() into 64 KB.
    let t0 = Instant::now();
    let mut total = 0usize;
    let mut buf = vec![0u8; 64 << 10];
    if let Ok(rd) = std::fs::read_dir(data.join("events")) {
        for e in rd.flatten() {
            if let Ok(mut f) = std::fs::File::open(e.path()) {
                while let Ok(k) = f.read(&mut buf) {
                    if k == 0 {
                        break;
                    }
                    total += k;
                }
            }
        }
    }
    eprintln!("DIAG read events {total} bytes single thread: {:.3} s", t0.elapsed().as_secs_f64());
    eprintln!("DIAG report took {:.3} s", t.elapsed().as_secs_f64());
}
