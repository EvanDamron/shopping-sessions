#!/bin/sh
# Build in a Linux container and time N runs on the ss-p20 volume: cbench.sh [N] [ENV=VAL...]
N=${1:-5}; shift
ENVS=""; for e in "$@"; do ENVS="$ENVS -e $e"; done
docker run --rm $ENVS -v "$PWD":/src -v ss-p20:/data:ro -v sscargo:/usr/local/cargo/registry -w /src -e CARGO_TARGET_DIR=/tmp/t perfbox sh -c "
cargo build --release -q 2>/dev/null; mkdir -p /tmp/o
for i in \$(seq $N); do NCPU=8 OUTPUT_DIR=/tmp/o /tmp/t/release/sessions 2>&1 | awk '{printf \"%s \", \$2}'; echo; done" | sort -t' ' -k7 -n
