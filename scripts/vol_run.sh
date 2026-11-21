#!/bin/sh
# Like docker_run.sh but reads /data from a named Docker volume (local disk in the VM).
# Usage: vol_run.sh VOLUME [IMAGE] [MEM]
VOL=$1; IMAGE=${2:-sessions:arm}; MEM=${3:-7g}
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 --cpus 8 \
  --memory "$MEM" --memory-swap "$MEM" --user 1000:1000 \
  -v "$VOL":/data:ro --tmpfs /output:rw,size=2g,mode=1777 --tmpfs /scratch:rw,size=64m,mode=1777 \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
  "$IMAGE"
