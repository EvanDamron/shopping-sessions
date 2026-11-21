#!/bin/sh
# A/B timing: binary built from git REF (default HEAD) vs the working tree, alternating runs.
# Usage: ab.sh [REF] [RUNS] [DATASET]
REF=${1:-HEAD}; N=${2:-7}; D=${3:-p20}
W=$(mktemp -d)
git archive "$REF" | tar -x -C "$W" && (cd "$W" && CARGO_TARGET_DIR=/tmp/ab-target cargo build --release -q 2>/dev/null) && cp /tmp/ab-target/release/sessions /tmp/ab-A
cargo build --release -q 2>/dev/null && cp target/release/sessions /tmp/ab-B
rm -rf "$W"
for i in $(seq $N); do
  for v in A B; do
    rm -rf out/ab; /usr/bin/time -p env NCPU=8 DATA_DIR=data/$D OUTPUT_DIR=out/ab /tmp/ab-$v 2>&1 >/dev/null | awk -v v=$v '/^real/{r=$2}/^user/{u=$2}/^sys/{s=$2}END{print v, r, u+s}'
  done
done | sort | awk '{a[$1]=a[$1]" "$2; c[$1]=c[$1]" "$3} END{for(k in a){print k" wall:"a[k]"  cpu:"c[k]}}' | sort
