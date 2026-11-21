# Shopping Sessions — Kentucky Fried Evan

Entry for the Databricks "Shopping Sessions" track of the Builders Buildathon: a
single Rust binary that turns the click-stream CSVs in `/data` into
`/output/user_daily/` and `/output/campaign_daily/` (Parquet).

## How it works

1. **Links**: link lines are parsed in parallel, IDs interned per hash shard, and
   joined with a lock-free union-find that always hangs the larger name under the
   smaller, so every root is its person's byte-wise smallest ID.
2. **Phase A**: event and click files are memory-mapped and split into line-aligned
   chunks. Each line is parsed with the exact CSV dialect and timestamp rules,
   validated, and resolved to its person. Valid events go to a person partition
   (32-byte records); every event-id copy also goes to an event-id bucket.
3. **Phase B**: per event-id bucket, the copy with the smallest `ingest_ts` wins;
   losing copies are marked deleted in their person partition. An invalid winner
   still blocks later valid copies.
4. **Phase C**: per person partition, events are sorted by (person, ts) and cut
   into sessions; each session is credited to the latest click within 7 days, and
   assists are counted per campaign by merging `[click, click + 7 days]` intervals.
5. Both tables are written as uncompressed Parquet, one user file per thread.

## Testing

    make check        # 36 fixtures + dev answer + extra seeds vs. reference/reference.py
    make fixtures     # the same fixtures through the Docker image

`reference/reference.py` is an independent, slow Python implementation of the
rules used to cross-check other generated seeds.
