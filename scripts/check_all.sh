#!/bin/sh
# Fixtures, dev answer, and the reference outputs for seeds 21-23 (v5 data, if generated).
set -u
BIN=${BIN:-target/release/sessions}
PY="uv run -q --with polars python"
$PY scripts/run_fixtures.py fixtures -- $BIN 2>&1 | tail -1
rm -rf out/dev && mkdir -p out && DATA_DIR=data/dev OUTPUT_DIR=out/dev $BIN 2>/dev/null && $PY scripts/compare.py out/dev starter/dev-answer/dev-answer | tail -1
for s in 21 22 23; do
  [ -d data/ref-s$s ] || continue
  rm -rf out/s$s && DATA_DIR=data/s$s OUTPUT_DIR=out/s$s $BIN 2>/dev/null && printf "s$s: " && $PY scripts/compare.py out/s$s data/ref-s$s | tail -1
done
