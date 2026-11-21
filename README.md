# Shopping Sessions — Kentucky Fried Evan

Entry for the Databricks "Shopping Sessions" track of the Builders Buildathon: a
single Rust binary (no engine, three small crates) that turns the click-stream CSVs
in `/data` into `/output/user_daily/` and `/output/campaign_daily/` (Parquet).

## How it works

1. **Links** (while products/campaigns load on another thread): link lines are
   parsed in parallel, IDs interned per hash shard, and joined with a lock-free
   union-find that always hangs the larger name under the smaller, so every root is
   its person's byte-wise smallest ID. The result is one huge-page hash table from
   each linked ID to its person.
2. **Phase A**: event and click files are memory-mapped and split into line-aligned
   4 MB chunks. A SIMD scanner (AVX2 on x86, NEON on arm) finds commas, newlines
   and quotes 64 bytes at a time and applies the exact CSV dialect. Each line is
   validated (SWAR timestamp parsing, ASCII-only digits) and resolved to its person
   through a prefetched lookup. Valid events go to a person partition as 24-byte
   records; every event-id copy also goes to an event-id bucket. Partitions live in
   per-thread huge-page arenas.
3. **Phase B**: per event-id bucket, the copy with the smallest `ingest_ts` wins and
   losing copies are marked deleted in their person partition. An invalid winner
   still blocks later valid copies.
4. **Phase C**: per person partition (largest first), events are grouped by person,
   sorted by time and cut into sessions. Each session is credited to the latest
   click within 7 days (ties: smallest `click_id`), and assists are counted per
   campaign by merging `[click, click + 7 days]` intervals.
5. Both tables are written by a small hand-written Parquet writer (plain encoding,
   no compression), one user file per thread.

Identity of event and user IDs inside hash tables uses a 64-bit hash plus an
independent 32-bit check hash.

## Testing

    make check        # 36 fixtures + dev answer + extra seeds vs. reference/reference.py
    make fixtures     # the same fixtures through the Docker image
    cargo test --release   # timestamp parser and CSV scanner differential tests

`reference/reference.py` is an independent, slow Python implementation of the
rules used to cross-check other generated seeds (`data/ref-sN`).
