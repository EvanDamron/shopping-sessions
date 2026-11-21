#!/bin/sh
# Native timing: bench.sh DATASET [runs]
D=${1:-p20}; N=${2:-3}
for i in $(seq $N); do rm -rf out/$D; NCPU=8 DATA_DIR=data/$D OUTPUT_DIR=out/$D target/release/sessions 2>&1 | awk '{printf "%s %s %s | ", $2, $3, $4}'; echo; done
