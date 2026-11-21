#!/bin/sh
# Instruction/cycle counts (stable under machine load) for N runs on the ss-p20 volume.
N=${1:-3}
docker run --rm --privileged -v "$PWD":/src -v ss-p20:/data:ro -v sscargo:/usr/local/cargo/registry -w /src -e CARGO_TARGET_DIR=/tmp/t perfbox sh -c "
cargo build --release -q 2>/dev/null; mkdir -p /tmp/o
for i in \$(seq $N); do NCPU=8 OUTPUT_DIR=/tmp/o perf stat -x, -e instructions,cycles,task-clock,page-faults /tmp/t/release/sessions 2>&1 | grep -E 'instructions|cycles|task-clock|page-faults|wrote' | awk -F, '{printf \"%s %s | \", \$3==\"\"?\$1:\$3, \$1}'; echo; done"
