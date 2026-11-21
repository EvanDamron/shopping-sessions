#!/bin/sh
# Diagnostic build: time binaries on generated 20M and 60M sets in /scratch, then run
# the real job on /data with the timings as extra campaign_daily rows (fails the dev
# check on purpose, which is how the rows come back).
S=${SCRATCH_DIR:-/scratch}
now() { date +%s%N; }
free2m=$(awk '{for (i = 13; i <= NF; i++) s += $i * 2 ^ (i - 13)} END {print s * 2}' /proc/buddyinfo)
t=$(now); /app/generate --seed 99 --events 20000000 --clicks 5000000 --output $S/g20 --threads 8 >/dev/null 2>&1; g20=$(( ($(now) - t) / 1000000 ))
t=$(now); /app/generate --seed 98 --events 60000000 --clicks 15000000 --output $S/g60 --threads 8 >/dev/null 2>&1; g60=$(( ($(now) - t) / 1000000 ))
run() { # binary dataset [VAR=value ...]: prints wall ms
  b=$1; d=$2; shift 2
  rm -rf $S/o
  t=$(now)
  env "$@" DATA_DIR=$S/$d OUTPUT_DIR=$S/o $b >/dev/null 2>$S/err
  echo $(( ($(now) - t) / 1000000 ))
}
A=""; B=""; A6=""; B6=""; C6=""; D6=""
for i in 1 2 3; do
  A="$A$(run /app/sessions_old g20) "; B="$B$(run /app/sessions g20) "
done
for i in 1 2 3; do
  A6="$A6$(run /app/sessions_old g60) "; B6="$B6$(run /app/sessions g60) "
  C6="$C6$(run /app/sessions g60 GLIBC_TUNABLES=) "; D6="$D6$(run /app/sessions g60 SS_HUGE=0) "
done
run /app/sessions g60 >/dev/null
ph=$(grep -E 'links: [0-9]|phase A parsed|stage:|phase B|phase C|wrote' $S/err | sed -E 's/^\[ *([0-9.]+)s\] (links|phase A|stage|phase B|phase C|wrote).*/\2=\1/; s/phase /p/' | tr '\n' ' ')
rm -rf $S/o $S/g20 $S/g60
cat > $S/rows.txt <<ROWS
DIAG1 gen20=${g20}ms gen60=${g60}ms free2M=${free2m}MB nproc=$(nproc)
DIAG2 g20 old56effae=$A new77385d7=$B
DIAG3 g60 old56effae=$A6 new77385d7=$B6
DIAG4 g60 new_noMallocTHP=$C6 new_SS_HUGE0=$D6
DIAG5 g60 new phases: $ph
ROWS
DIAG_ROWS_FILE=$S/rows.txt exec /app/sessions
