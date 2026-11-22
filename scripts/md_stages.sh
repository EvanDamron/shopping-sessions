#!/bin/sh
# Phase-A stage costs on x86, single thread: STAGE 0 = scan only, 1 = +timestamps, 2 = +hashes,
# 3 = +link lookup (no prefetch ring), 99 = full. Temporary tree; nothing committed.
R=$HOME/buildathon/shopping
rm -rf $R/stagetree && cp -r $R/repo $R/stagetree && cd $R/stagetree && python3 - <<'PY'
p='src/main.rs'; s=open(p).read()
old="""            scan::for_records::<7>(ech[t], |f| {
                if f[0].is_empty() {
                    return;
                }"""
new="""            let stage: u32 = std::env::var("STAGE").ok().and_then(|v| v.parse().ok()).unwrap_or(99);
            scan::for_records::<7>(ech[t], |f| {
                if f[0].is_empty() || stage == 0 {
                    return;
                }
                if stage == 1 { std::hint::black_box((ts::parse_ingest(f[3]), ts::parse_ts(f[2]))); return; }
                if stage == 2 { std::hint::black_box((ts::parse_ingest(f[3]), ts::parse_ts(f[2]), hh(f[0]), hh(f[1]))); return; }
                if stage == 3 { std::hint::black_box((ts::parse_ingest(f[3]), ts::parse_ts(f[2]), hh(f[0]))); let (a, b) = hh(f[1]); std::hint::black_box(links.person_h(f[1], a, b)); return; }"""
assert s.count(old)==1; s=s.replace(old,new)
open(p,'w').write(s)
PY
docker run --rm --cpuset-cpus 0-3,20-23 -v $R/stagetree:/src -v $R/data/p20:/data:ro -v $R/cargo:/usr/local/cargo/registry -w /src -e CARGO_TARGET_DIR=/tmp/t ss-perfbox sh -c "
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release -q 2>&1 | grep -E '^error'; mkdir -p /tmp/o
for st in 0 1 2 3 99; do for i in 1 2; do printf \"stage \$st: \"; STAGE=\$st NCPU=1 OUTPUT_DIR=/tmp/o /tmp/t/release/sessions 2>&1 | grep -E 'mapped|phase A' | awk '{print \$2}' | tr -d ']s' | tr '\n' ' ' | awk '{printf \"%.3f\n\", \$2-\$1}'; done; done"
