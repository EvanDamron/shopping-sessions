#!/bin/sh
# perf profile on Mystik-Dan (x86, our CPU share). Usage: md_perf.sh DATASET [NCPU] [EXTRA_RUSTFLAGS]
D=${1:-p20}; T=${2:-8}
R=$HOME/buildathon/shopping
docker run --rm --privileged --cpuset-cpus 0-3,20-23 -v "$R/repo":/src -v "$R/data/$D":/data:ro -v "$R/cargo":/usr/local/cargo/registry \
  -w /src -e CARGO_TARGET_DIR=/tmp/t ss-perfbox sh -c "
RUSTFLAGS='-C target-cpu=x86-64-v3 -C force-frame-pointers=yes' cargo build --release -q 2>&1 | grep -E '^error' ; mkdir -p /tmp/o
NCPU=$T OUTPUT_DIR=/tmp/o /tmp/t/release/sessions >/dev/null 2>&1
NCPU=$T OUTPUT_DIR=/tmp/o perf stat -e task-clock,page-faults,context-switches /tmp/t/release/sessions 2>&1 | grep -E 'task-clock|page-faults|elapsed|user|sys|wrote'
NCPU=$T OUTPUT_DIR=/tmp/o perf record -F 3000 -o /tmp/perf.data /tmp/t/release/sessions >/dev/null 2>&1
perf report -i /tmp/perf.data --sort dso,symbol --percent-limit 0.7 --stdio 2>/dev/null | grep -E '^ +[0-9]' | sed 's/std::sys::backtrace::__rust_begin_short_backtrace:://' | cut -c1-150 | head -35"
