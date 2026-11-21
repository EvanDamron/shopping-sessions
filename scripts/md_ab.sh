#!/bin/sh
# x86 A/B on Mystik-Dan: git REF (A) vs the synced work tree (B). Min of N runs per phase.
# Usage (on Mystik-Dan): md_ab.sh [N] [NCPU] [REF] [DATASET]
N=${1:-5}; T=${2:-1}; REF=${3:-HEAD}; D=${4:-p20}
R=$HOME/buildathon/shopping
rm -rf $R/abA && mkdir -p $R/abA && (cd $R/repo && git fetch -q && git archive "origin/main" >/dev/null 2>&1; git archive "$REF" | tar -x -C $R/abA)
docker run --rm --cpuset-cpus 0-3,20-23 -v $R/abA:/srcA -v $R/work:/srcB -v $R/data/$D:/data:ro -v $R/cargo:/usr/local/cargo/registry ss-perfbox sh -c "
export RUSTFLAGS='-C target-cpu=x86-64-v3'
cd /srcA && CARGO_TARGET_DIR=/tmp/tA cargo build --release -q 2>&1 | grep -E '^error'; cd /srcB && CARGO_TARGET_DIR=/tmp/tB cargo build --release -q 2>&1 | grep -E '^error' -A5; mkdir -p /tmp/o
for i in \$(seq $N); do for v in A B; do printf \"\$v \"; NCPU=$T OUTPUT_DIR=/tmp/o /tmp/t\$v/release/sessions 2>&1 | grep -E '^\[' | awk '{printf \"%s \", \$2}' | tr -d ']s'; echo; done; done" | awk '
{ v=$1; n=NF-1; prev=0; for (i=2;i<=NF;i++){ d=$i-prev; prev=$i; k=v SUBSEP (i-1); if (!(k in m) || d<m[k]) m[k]=d; } if (!(v in tot) || $NF<tot[v]) tot[v]=$NF; cols=n }
END { for (v in tot) { printf "%s total %.3f | phases:", v, tot[v]; for (i=1;i<=cols;i++) printf " %.3f", m[v SUBSEP i]; printf "\n" } }' | sort
