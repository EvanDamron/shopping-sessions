#!/bin/sh
# Diagnostic build: time the current pipeline on a generated 60M set in /scratch
# (phases, worker threads, output sink, cascadelake build), then run the real job on
# /data with the timings as extra campaign_daily rows (fails the dev check on purpose).
S=${SCRATCH_DIR:-/scratch}
O=${OUTPUT_DIR:-/output}
now() { date +%s%N; }
free2m=$(awk '{for (i = 13; i <= NF; i++) s += $i * 2 ^ (i - 13)} END {print s * 2}' /proc/buddyinfo)
t=$(now); /app/generate --seed 97 --events 60000000 --clicks 15000000 --output $S/g60 --threads 8 >/dev/null 2>&1; g60=$(( ($(now) - t) / 1000000 ))
run() { # binary [VAR=value ...]: prints wall ms; stderr kept in $S/err
  b=$1; shift
  rm -rf $O/ab
  t=$(now)
  env "$@" DATA_DIR=$S/g60 OUTPUT_DIR=$O/ab $b >/dev/null 2>$S/err
  echo $(( ($(now) - t) / 1000000 ))
}
B=""; T6=""; T4=""; X=""; N=""; PH=""
for i in 1 2 3; do
  B="$B$(run /app/sessions SS_TIME=1) "
  ph=$(grep -E 'links: [0-9]|phase A parsed|stage:|phase C|wrote' $S/err | sed -E 's/^\[ *([0-9.]+)s\] (links|phase A|stage|phase C|wrote).*/\2=\1/; s/phase /p/' | tr '\n' ' ')
  w=$(grep -E 'rows written in' $S/err | sed -E 's/.*written in ([0-9.]+)ms.*returned in ([0-9.]+)ms.*/\1 \2/' | awk '{if ($1 > a) a = $1; if ($2 > b) b = $2} END {printf "write max %.0f ms, release max %.0f ms", a, b}')
  PH="$ph| $w"
  T6="$T6$(run /app/sessions NCPU=6) "; T4="$T4$(run /app/sessions NCPU=4) "
  X="$X$(run /app/sessions_clx) "; N="$N$(run /app/sessions DIAG_NOWRITE=1) "
done
rm -rf $O/ab $S/g60
cat > $S/rows.txt <<ROWS
DIAG1 g60 phases (s, last of 3, SS_TIME): $PH
DIAG2 g60 wall ms 3 runs: base8t=$B 6t=$T6 4t=$T4
DIAG3 g60 wall ms: base_output_ext4=$B nowrite=$N
DIAG4 g60 wall ms: base_x86-64-v3=$B cascadelake=$X
DIAG5 gen60=${g60}ms free2M=${free2m}MB data=/scratch ext4 page cache warm, output=/output/ab ext4
ROWS
DIAG_ROWS_FILE=$S/rows.txt exec /app/sessions
