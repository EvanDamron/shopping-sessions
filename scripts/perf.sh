#!/bin/sh
# Profile the pipeline on the ss-p20 Docker volume with perf (needs the perfbox image).
docker run --rm --privileged -v "$PWD":/src -v ss-p20:/data:ro -v sscargo:/usr/local/cargo/registry -w /src -e CARGO_TARGET_DIR=/tmp/t perfbox sh -c '
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release -q 2>/dev/null; mkdir -p /tmp/o
for i in 1 2; do NCPU=8 OUTPUT_DIR=/tmp/o /tmp/t/release/sessions 2>&1 | tail -1; done
NCPU=8 OUTPUT_DIR=/tmp/o perf record -F 2000 -g -o /tmp/perf.data /tmp/t/release/sessions 2>&1 | grep wrote
perf report -i /tmp/perf.data --no-children --percent-limit 1 --stdio 2>/dev/null | grep -E "^ +[0-9]" | sed "s/std::sys::backtrace::__rust_begin_short_backtrace:://" | cut -c1-150 | head -30'
