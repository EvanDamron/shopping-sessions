#!/bin/sh
# Single-thread x86 profile with key helpers forced out of line (temporary copy of the tree).
D=${1:-p20}; T=${2:-1}
R=$HOME/buildathon/shopping
rm -rf $R/proftree && cp -r $R/repo $R/proftree && cd $R/proftree && python3 - <<'PY'
for p,fn in [('src/ts.rs','fn parse_iso('),('src/ts.rs','pub fn parse_f3('),('src/ts.rs','pub fn parse_ingest('),('src/main.rs','fn hh('),('src/main.rs','fn person_h('),('src/main.rs','fn person(st: &mut CS'),('src/main.rs','    fn find(&mut self, ph: u64)'),('src/main.rs','fn parse_price(')]:
    s=open(p).read()
    i=s.index(fn); j=s.rindex('#[inline',0,i); k=s.index(']',j)
    s=s[:j]+'#[inline(never)]'+s[k+1:]
    open(p,'w').write(s)
PY
docker run --rm --privileged --cpuset-cpus 0-3,20-23 -v $R/proftree:/src -v $R/data/$D:/data:ro -v $R/cargo:/usr/local/cargo/registry -w /src -e CARGO_TARGET_DIR=/tmp/t ss-perfbox sh -c "
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release -q 2>&1 | grep -E '^error'; mkdir -p /tmp/o
NCPU=$T OUTPUT_DIR=/tmp/o perf record -F 2000 -o /tmp/perf.data /tmp/t/release/sessions 2>&1 | grep -E 'phase|wrote' | awk '{printf \"%s \", \$2}'; echo
perf report -i /tmp/perf.data --sort dso,symbol --percent-limit 0.5 --stdio 2>/dev/null | grep -E '^ +[0-9]' | sed 's/std::sys::backtrace::__rust_begin_short_backtrace:://' | cut -c1-140 | head -40"
