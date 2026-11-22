#!/bin/sh
# Counters for git REF (A) vs the synced work tree (B), NCPU threads, N runs each (x86).
N=${1:-3}; T=${2:-1}; REF=${3:-HEAD}; D=${4:-p20}
R=$HOME/buildathon/shopping
rm -rf $R/abA && mkdir -p $R/abA && (cd $R/repo && git archive "$REF" | tar -x -C $R/abA)
docker run --rm --privileged --cpuset-cpus 0-3,20-23 -v $R/abA:/srcA -v $R/work:/srcB -v $R/data/$D:/data:ro -v $R/cargo:/usr/local/cargo/registry ss-perfbox sh -c "
export RUSTFLAGS='-C target-cpu=x86-64-v3'
cd /srcA && CARGO_TARGET_DIR=/tmp/tA cargo build --release -q 2>&1 | grep -E '^error'; cd /srcB && CARGO_TARGET_DIR=/tmp/tB cargo build --release -q 2>&1 | grep -E '^error' -A5; mkdir -p /tmp/o
for i in \$(seq $N); do for v in A B; do printf \"\$v \"; NCPU=$T OUTPUT_DIR=/tmp/o perf stat -x, -e cycles,instructions,task-clock /tmp/t\$v/release/sessions 2>&1 | grep -E 'cycles|instructions|task-clock' | awk -F, '{printf \"%s=%.0f \", \$3, \$1/1e6}'; echo; done; done" | sort
