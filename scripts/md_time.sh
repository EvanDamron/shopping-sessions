#!/bin/sh
# Time the image on Mystik-Dan with scorer-like flags on our CPU share.
# Usage: md_time.sh IMAGE DATASET [RUNS]   (datasets live in ~/buildathon/shopping/data)
IMAGE=$1; D=$2; N=${3:-3}
DATA=$HOME/buildathon/shopping/data/$D
OUT=$HOME/buildathon/shopping/out/$D
for i in $(seq $N); do
  rm -rf "$OUT" && mkdir -p "$OUT" && chmod 777 "$OUT"
  s=$(date +%s%N)
  docker run --rm --cpuset-cpus 0-3,20-23 --memory 24g --memory-swap 24g --network none --read-only \
    --tmpfs /tmp:rw,size=64m,mode=1777 --tmpfs /scratch:rw,size=1g,mode=1777 --cap-drop ALL \
    --security-opt no-new-privileges --pids-limit 4096 --user 1000:1000 \
    -v "$DATA":/data:ro -v "$OUT":/output \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
    "$IMAGE" 2>&1 | awk '{printf "%s ", $2}'
  e=$(date +%s%N)
  echo " | docker wall $(( (e - s) / 1000000 )) ms"
done
