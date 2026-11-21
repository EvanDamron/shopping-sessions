#!/usr/bin/env python3
"""Correctness-first reference implementation of files/rules.md.

Independent of the Rust implementation under src/. Used only to cross-check
it on generated datasets.

Usage: DATA_DIR=... OUTPUT_DIR=... python reference.py [DATA_DIR OUTPUT_DIR]
Run with: uv run -q --with polars python reference/reference.py

All strings are handled as raw bytes so every comparison is byte-wise and no
value is ever normalized. CSV lines are parsed by hand per the rules' dialect.
"""
import glob
import json
import os
import re
import sys
from bisect import bisect_left, bisect_right
from collections import defaultdict

MS_DAY = 86_400_000
GAP_MS = 1_800_000
LATE_MS = 86_400_000
LOOKBACK_MS = 7 * MS_DAY
MAX_PRICE = 10_000_000

# ---------------------------------------------------------------- CSV dialect

QUOTE = 0x22


def parse_line(line, nfields):
    """Return the list of field values (bytes; b'' is null), or None if malformed."""
    if not line:
        return None  # an empty line is malformed
    parts = line.split(b",")
    if len(parts) != nfields:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            # Quotes are allowed only wrapping the whole field, with none inside.
            if len(p) >= 2 and p[0] == QUOTE and p[-1] == QUOTE and b'"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p)
    return out


def iter_file_rows(path, nfields):
    """Yield parsed data rows of one CSV file (header skipped; malformed dropped)."""
    with open(path, "rb") as f:
        data = f.read()
    if not data:
        return
    lines = data.split(b"\n")  # only \n ends a line; \r is ordinary
    if lines and lines[-1] == b"":
        lines.pop()  # the terminating newline of the last line
    for line in lines[1:]:  # first line is the header
        row = parse_line(line, nfields)
        if row is not None:
            yield row


def iter_dir_rows(folder, nfields):
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        if os.path.isfile(path):
            yield from iter_file_rows(path, nfields)


# ---------------------------------------------------------------- timestamps

F1 = re.compile(rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z")
F2 = re.compile(rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})")
F3 = re.compile(rb"[0-9]+")
DIGITS = re.compile(rb"[0-9]+")


def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def days_in_month(y, m):
    if m == 2:
        return 29 if is_leap(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31


def days_from_civil(y, m, d):
    """Days since 1970-01-01 for the proleptic Gregorian date (Howard Hinnant)."""
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def civil_ms(g):
    y, mo, d, h, mi, s = (int(x) for x in g[:6])
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= days_in_month(y, mo)):
        return None
    if h > 23 or mi > 59 or s > 59:
        return None
    return ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(v):
    """Return epoch ms (int, possibly huge) or None if the timestamp is malformed."""
    if not v:
        return None
    m = F1.fullmatch(v)
    if m:
        return civil_ms(m.groups())
    m = F2.fullmatch(v)
    if m:
        base = civil_ms(m.groups())
        if base is None:
            return None
        oh, om = int(m.group(8)), int(m.group(9))
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if m.group(7) == b"+" else base + off
    if F3.fullmatch(v):
        return int(v)
    return None


# ---------------------------------------------------------------- main


def main():
    data_dir = os.environ.get("DATA_DIR")
    out_dir = os.environ.get("OUTPUT_DIR")
    if len(sys.argv) >= 3:
        data_dir, out_dir = sys.argv[-2], sys.argv[-1]
    if not data_dir or not out_dir:
        sys.exit("need DATA_DIR and OUTPUT_DIR")

    with open(os.path.join(data_dir, "manifest.json"), "rb") as f:
        manifest = json.load(f)
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())
    assert ws is not None and we is not None, manifest

    # products / campaigns (clean, but parse with the same dialect)
    products = {}
    for row in iter_file_rows(os.path.join(data_dir, "products.csv"), 2):
        products[row[0]] = row[1]
    campaigns = {}
    for row in iter_file_rows(os.path.join(data_dir, "campaigns.csv"), 2):
        campaigns[row[0]] = row[1]

    # ---- links: union-find over IDs
    parent = {}

    def find(x):
        root = x
        while True:
            p = parent.get(root)
            if p is None or p == root:
                break
            root = p
        # path compression
        while x != root:
            p = parent.get(x)
            if p is None or p == x:
                break
            parent[x] = root
            x = p
        return root

    for a, b in iter_dir_rows(os.path.join(data_dir, "links"), 2):
        if not a or not b:
            continue
        ra, rb = find(a), find(b)
        if ra == rb:
            parent.setdefault(ra, ra)
            continue
        # keep the byte-wise smallest ID as root, so the root is the person's name
        if ra < rb:
            parent[rb] = ra
            parent.setdefault(ra, ra)
        else:
            parent[ra] = rb
            parent.setdefault(rb, rb)

    name_cache = {}

    def person(uid):
        n = name_cache.get(uid)
        if n is None:
            n = find(uid) if uid in parent else uid
            name_cache[uid] = n
        return n

    # ---- events: parse, dedup (smallest ingest_ts; first seen on ties)
    kept = {}  # event_id -> (ingest_ms, row)
    bad_ingest = 0
    for row in iter_dir_rows(os.path.join(data_dir, "events"), 7):
        eid = row[0]
        if not eid:
            continue
        ing = F1.fullmatch(row[3])
        ing_ms = civil_ms(ing.groups()) if ing else None
        if ing_ms is None:
            bad_ingest += 1
            continue
        prev = kept.get(eid)
        if prev is None or ing_ms < prev[0]:
            kept[eid] = (ing_ms, row)
    if bad_ingest:
        print(f"warning: {bad_ingest} event lines with invalid ingest_ts skipped", file=sys.stderr)

    # ---- validate and drop late; group by person
    VALID_TYPES = (b"view", b"cart", b"purchase")
    by_person = defaultdict(list)  # person -> list of (ts, price or 0, is_purchase, product_id)
    for ing_ms, row in kept.values():
        _eid, uid, ts_raw, _ing, etype, pid, price_raw = row
        if not uid or not pid or etype not in VALID_TYPES:
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (ws <= ts < we):
            continue
        price = 0
        is_purchase = etype == b"purchase"
        if is_purchase:
            if not DIGITS.fullmatch(price_raw):
                continue
            price = int(price_raw)
            if not (1 <= price <= MAX_PRICE):
                continue
        if ing_ms - ts > LATE_MS:
            continue
        by_person[person(uid)].append((ts, price, is_purchase, pid))
    del kept

    # ---- clicks: parse, dedup (any copy), validate; group by person
    seen_clicks = set()
    clicks_by_person = defaultdict(list)  # person -> list of (ts, click_id, campaign_id)
    cws = ws - LOOKBACK_MS
    for row in iter_dir_rows(os.path.join(data_dir, "clicks"), 4):
        cid, uid, camp, ts_raw = row
        if not cid or cid in seen_clicks:
            continue
        seen_clicks.add(cid)  # copies are identical: the first is as good as any
        if not uid or camp not in campaigns:
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (cws <= ts < we):
            continue
        clicks_by_person[person(uid)].append((ts, cid, camp))
    del seen_clicks

    click_index = {}
    for p, cl in clicks_by_person.items():
        cl.sort()  # by ts, then click_id byte-wise
        click_index[p] = ([c[0] for c in cl], cl)

    # ---- sessionize, attribute, aggregate
    # user_daily: (person, day) -> [sessions, events, revenue, {category: cents}]
    user_daily = {}
    # campaign_daily: (campaign, day) -> [sessions, purchasing, revenue, assisted]
    camp_daily = {}

    def emit_session(p, start, evs):
        day = start // MS_DAY
        n = len(evs)
        rev = 0
        cats = None
        for _ts, price, is_p, pid in evs:
            if is_p:
                rev += price
                if cats is None:
                    cats = {}
                c = products.get(pid, b"unknown")
                cats[c] = cats.get(c, 0) + price
        has_purchase = cats is not None

        u = user_daily.get((p, day))
        if u is None:
            u = [0, 0, 0, {}]
            user_daily[(p, day)] = u
        u[0] += 1
        u[1] += n
        u[2] += rev
        if cats:
            ucats = u[3]
            for c, v in cats.items():
                ucats[c] = ucats.get(c, 0) + v

        # attribution
        credited = b"organic"
        assists = ()
        ci = click_index.get(p)
        if ci is not None:
            tss, cl = ci
            hi = bisect_right(tss, start)  # clicks with ts <= start
            lo = bisect_left(tss, start - LOOKBACK_MS)  # clicks with ts >= start - 7d
            if hi > lo:
                t = tss[hi - 1]
                first = bisect_left(tss, t, lo, hi)
                credited = cl[first][2]  # latest ts, smallest click_id
                assists = {cl[i][2] for i in range(lo, hi)}

        k = (credited, day)
        cd = camp_daily.get(k)
        if cd is None:
            cd = [0, 0, 0, 0]
            camp_daily[k] = cd
        cd[0] += 1
        cd[1] += 1 if has_purchase else 0
        cd[2] += rev
        for camp in assists:
            k = (camp, day)
            cd = camp_daily.get(k)
            if cd is None:
                cd = [0, 0, 0, 0]
                camp_daily[k] = cd
            cd[3] += 1

    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        cur = [evs[0]]
        start = evs[0][0]
        prev_ts = evs[0][0]
        for e in evs[1:]:
            if e[0] - prev_ts > GAP_MS:
                emit_session(p, start, cur)
                cur = [e]
                start = e[0]
            else:
                cur.append(e)
            prev_ts = e[0]
        emit_session(p, start, cur)

    write_outputs(out_dir, user_daily, camp_daily, campaigns)


def write_outputs(out_dir, user_daily, camp_daily, campaigns):
    import polars as pl

    def dec(b):
        return b.decode("utf-8")

    u_rows = {"user_id": [], "day": [], "sessions": [], "events": [], "revenue_cents": [], "top_category": []}
    for (p, day), (s, n, rev, cats) in user_daily.items():
        top = None
        if cats:
            best = max(cats.values())
            top = dec(min(c for c, v in cats.items() if v == best))
        u_rows["user_id"].append(dec(p))
        u_rows["day"].append(day)
        u_rows["sessions"].append(s)
        u_rows["events"].append(n)
        u_rows["revenue_cents"].append(rev)
        u_rows["top_category"].append(top)
    udf = pl.DataFrame(
        u_rows,
        schema={"user_id": pl.String, "day": pl.Int32, "sessions": pl.Int64, "events": pl.Int64,
                "revenue_cents": pl.Int64, "top_category": pl.String},
    ).with_columns(pl.col("day").cast(pl.Date))

    c_rows = {"campaign_id": [], "channel": [], "day": [], "sessions": [], "purchasing_sessions": [],
              "revenue_cents": [], "assisted_sessions": []}
    for (camp, day), (s, ps, rev, a) in camp_daily.items():
        c_rows["campaign_id"].append(dec(camp))
        c_rows["channel"].append("organic" if camp == b"organic" else dec(campaigns[camp]))
        c_rows["day"].append(day)
        c_rows["sessions"].append(s)
        c_rows["purchasing_sessions"].append(ps)
        c_rows["revenue_cents"].append(rev)
        c_rows["assisted_sessions"].append(a)
    cdf = pl.DataFrame(
        c_rows,
        schema={"campaign_id": pl.String, "channel": pl.String, "day": pl.Int32, "sessions": pl.Int64,
                "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64},
    ).with_columns(pl.col("day").cast(pl.Date))

    for name, df in (("user_daily", udf), ("campaign_daily", cdf)):
        folder = os.path.join(out_dir, name)
        os.makedirs(folder, exist_ok=True)
        df.write_parquet(os.path.join(folder, "part-0.parquet"))


if __name__ == "__main__":
    main()
