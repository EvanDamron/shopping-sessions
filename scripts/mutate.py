# mutate.py SRC DST [PERCENT] [SEED]: a copy of a dataset with a share of its event and click
# lines damaged one byte-level edit at a time (quotes, separators, digits, lengths), to exercise
# the per-line decoder's shape checks. Link and lookup files are copied as they are.
#
# The edits do not keep the input guarantees of rules.md (a damaged ingest_ts, say), so the
# output is only compared between two readings of the same lines (SS_DECCHECK=1, or the same
# binary with the decoder on and off), never against a reference answer.
import sys, os, glob, random, shutil

src, dst = sys.argv[1], sys.argv[2]
pct = float(sys.argv[3]) if len(sys.argv) > 3 else 3.0
rnd = random.Random(int(sys.argv[4]) if len(sys.argv) > 4 else 7)
shutil.rmtree(dst, ignore_errors=True)
for d in ("events", "clicks", "links"):
    os.makedirs(os.path.join(dst, d))
for f in ("products.csv", "campaigns.csv", "manifest.json"):
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
for p in glob.glob(os.path.join(src, "links", "*.csv")):
    shutil.copy(p, os.path.join(dst, "links", os.path.basename(p)))

CH = b'",\r 0123456789abcxyzEeUuTtZz:+-._"\x00"",,\n'
TS = [b"2026-13-01T00:00:00Z", b"2026-02-30T00:00:00Z", b"2027-02-29T10:00:00Z", b"2028-02-29T10:00:00Z",
      b"2026-10-03T24:00:00Z", b"2026-10-03T23:60:00Z", b"2026-10-03T23:59:60Z", b"2026-10-03t23:58:10Z",
      b"2026-10-03T23:58:10z", b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"2026-10-03T23:58:10.5Z",
      b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200",
      b"2026-10-03T23:58:10-00:00", b"2026-10-03T23:58:10+00:00", b"1791071890000.0", b"1.79e12", b"-5", b"",
      b"0001791071890000", b"86400000", b"99999999999999999999999", b"179107189000", b"17910718900000",
      b"\xd9\xa2\xd9\xa5\xd9\xa0", b"2026-10-03T23:58:10Z\r", b"2026-1o-03T23:58:10Z", b"2026-10-0ST23:58:10Z"]


def near(ts):
    """The line's own timestamp with one character changed or its form's edge poked."""
    t = bytearray(ts)
    if not t:
        return bytes(t)
    i = rnd.randrange(len(t))
    t[i] = rnd.choice(b"0123456789:-+TZtz .9")
    return bytes(t)


def edit(line, nf):
    k = rnd.randrange(14)
    f = line.split(b",")
    if not line.isascii():
        return line
    if k == 0 and line:                       # replace one byte
        i = rnd.randrange(len(line))
        return line[:i] + bytes([rnd.choice(CH)]) + line[i + 1:]
    if k == 1:                                # insert one byte
        i = rnd.randrange(len(line) + 1)
        return line[:i] + (b"\xc3\xa9" if rnd.random() < 0.05 else bytes([rnd.choice(CH)])) + line[i:]
    if k == 2 and line:                       # delete one byte
        i = rnd.randrange(len(line))
        return line[:i] + line[i + 1:]
    if len(f) != nf:
        return line + b","
    j = rnd.randrange(nf)
    if k == 3:                                # quote a field
        f[j] = b'"' + f[j] + b'"'
    elif k == 4:                              # half a quote, or a quote in the middle
        f[j] = rnd.choice([b'"' + f[j], f[j] + b'"', f[j][:1] + b'"' + f[j][1:], b'"', b'""', b'"""', b'""' + f[j]])
    elif k == 5:                              # a timestamp from the list of bad ones, in a timestamp field
        tj = rnd.choice([2, 3]) if nf == 7 else 3
        f[tj] = rnd.choice(TS) if rnd.random() < 0.6 else near(f[tj].strip(b'"'))
        if rnd.random() < 0.2:
            f[tj] = b'"' + f[tj] + b'"'
    elif k == 6:                              # empty a field
        f[j] = rnd.choice([b"", b'""'])
    elif k == 7:                              # a long field: the line no longer fits 96 bytes
        f[j] = f[j] + b"x" * rnd.choice([1, 8, 30, 64, 100, 300])
    elif k == 8:                              # an ID that is nearly the generator's form
        ij = rnd.choice([0, 1])
        v = f[ij].strip(b'"')
        f[ij] = rnd.choice([v[:1] + b"0" + v[1:], v.upper(), v[:1], v + b"0", b"x" + v[1:], v[:1] + b"zzzzzzzz",
                            v[:1] + v[2:], v[:-1] + b"_", b" " + v, v + b" ", v[:1] + b"0", v[:1] + b"00"])
        if rnd.random() < 0.3:
            f[ij] = b'"' + f[ij] + b'"'
    elif k == 9:                              # drop or add a field
        if rnd.random() < 0.5:
            del f[j]
        else:
            f.insert(j, rnd.choice([b"", b"x", b'""']))
    elif k == 10 and nf == 7:                 # type variants
        f[4] = rnd.choice([b"Purchase", b"cart ", b"VIEW", b"purchase", b"view", b"cart", b"purchas", b"purchases", b"vie", b"views", b'"purchase"', b'"view"'])
    elif k == 11 and nf == 7:                 # price variants
        f[6] = rnd.choice([b"0", b"0250", b"10000000", b"10000001", b"12.5", b"-3", b"+3", b"1e3", b" 12", b"12\r", b'"77"', b'"', b"99999999999999999999"])
    elif k == 12:                             # swap two fields
        i2 = rnd.randrange(nf)
        f[j], f[i2] = f[i2], f[j]
    else:                                     # an empty line, or one of only separators
        return rnd.choice([b"", b"," * (nf - 1), b'"' * 2, b"\r"])
    return b",".join(f)


def run(folder, nf):
    n = m = 0
    counts = {}
    if nf == 4:
        for p in glob.glob(os.path.join(src, folder, "*.csv")):
            with open(p, "rb") as fh:
                for l in fh:
                    k = l.split(b",")[0].strip(b'"')
                    counts[k] = counts.get(k, 0) + 1
    for p in sorted(glob.glob(os.path.join(src, folder, "*.csv"))):
        with open(p, "rb") as fh:
            header = fh.readline()
            lines = fh.read().split(b"\n")
        last = lines.pop() if lines else b""
        out = []
        for l in lines:
            n += 1
            # Copies of a click must stay identical (which copy counts is not defined
            # otherwise), so only clicks without copies are edited.
            if rnd.random() * 100 < pct and (nf == 7 or counts.get(l.split(b",")[0].strip(b'"')) == 1):
                l = edit(l, nf)
                m += 1
                if rnd.random() < 0.1 and l.isascii():  # (a second edit must not cut a character in two)
                    l = edit(l, nf)
            out.append(l)
        with open(os.path.join(dst, folder, os.path.basename(p)), "wb") as fh:
            fh.write(header)
            fh.write(b"\n".join(out))
            # most files end with a newline; some do not, and some end with a blank line
            r = rnd.random()
            fh.write(b"" if (r < 0.2 and out) else (b"\n\n" if r < 0.3 else b"\n"))
            if last:
                fh.write(last)
    print(folder, n, "lines,", m, "edited")


run("events", 7)
run("clicks", 4)
