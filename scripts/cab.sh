#!/bin/sh
# A/B in the Linux VM: HEAD (A) vs working tree (B), alternating, NCPU=${NCPU:-8}. Usage: cab.sh [RUNS]
N=${1:-5}
rm -rf /tmp/cab && mkdir -p /tmp/cab/A && git archive HEAD | tar -x -C /tmp/cab/A
docker run --rm -e NC=${NCPU:-8} -v "$PWD":/src -v /tmp/cab/A:/srcA -v ss-p20:/data:ro -v sscargo:/usr/local/cargo/registry perfbox sh -c "
cd /srcA && CARGO_TARGET_DIR=/tmp/tA cargo build --release -q 2>/dev/null; cd /src && CARGO_TARGET_DIR=/tmp/tB cargo build --release -q 2>/dev/null; mkdir -p /tmp/o
for i in \$(seq $N); do for v in A B; do printf \"\$v \"; NCPU=\$NC OUTPUT_DIR=/tmp/o /tmp/t\$v/release/sessions 2>&1 | awk '{printf \"%s \", \$2}'; echo; done; done" | sort
