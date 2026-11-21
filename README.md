# Shopping Sessions — Kentucky Fried Evan

Entry for the Databricks "Shopping Sessions" track of the Builders Buildathon: a
single Rust binary that turns the click-stream CSVs in `/data` into
`/output/user_daily/` and `/output/campaign_daily/` (Parquet).

## How it works

1. **Links**: link lines are parsed in parallel and both IDs interned in one shared
   lock-free table, joined by a concurrent union-find that always hangs the larger
   name under the smaller, so every root is its person's byte-wise smallest ID. A
   person is numbered by the table slot of that name; IDs no link mentions are added
   when an event or click first uses them.
2. **Phase A**: event and click files are read with `pread` into a 64 KB buffer per
   thread (nothing is mapped). Separators are indexed and quotes validated with bit
   masks 64 bytes at a time; each line is then parsed with the exact CSV dialect and
   timestamp rules, validated, and resolved to its person. A valid event becomes a
   12-byte record in its person's bucket; every event-id copy also leaves a 16-byte
   key in an event-id bucket. Records reach their buckets through write-combining
   buffers and streaming stores.
3. **Phase B**: per event-id bucket, the copy with the smallest `ingest_ts` wins;
   losing copies are marked dead in their person bucket. An invalid winner still
   blocks later valid copies.
4. **Phase C**: per person bucket (a range of person numbers), events are grouped by
   counting sort, sorted by time and cut into sessions; each session is credited to
   the latest click within 7 days, and assists are counted per campaign by merging
   `[click, click + 7 days]` intervals. User rows are encoded straight into Parquet
   pages.
5. Both tables are written as uncompressed Parquet, one user file per thread. Record
   memory is returned to the kernel on all threads before exit.

IDs of up to 15 bytes are held inline as 16-byte values, longer ones interned, so
user, click, campaign and product IDs always compare exactly. Event IDs are
deduplicated by a 64-bit hash within one of up to 1,024 hash buckets.

## Testing

    make check        # 36 fixtures + dev answer + extra seeds vs. reference/reference.py
    make fixtures     # the same fixtures through the Docker image

`reference/reference.py` is an independent, slow Python implementation of the
rules used to cross-check other generated seeds.
