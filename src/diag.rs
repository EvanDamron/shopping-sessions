//! Scorer diagnostics (this branch only): environment and micro-timings to stderr.

use std::io::Read;
use std::time::Instant;

fn show(label: &str, path: &str, max: usize) {
    match std::fs::read_to_string(path) {
        Ok(s) => eprintln!("DIAG {label}: {}", s.trim().chars().take(max).collect::<String>().replace('\n', " | ")),
        Err(e) => eprintln!("DIAG {label}: unreadable ({e})"),
    }
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
