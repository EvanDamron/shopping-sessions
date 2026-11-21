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
5. Both tables are written as uncompressed Parquet, one user file per thread; user
   columns are dictionary encoded with RLE/bit-packed indices. Record
   memory is returned to the kernel on all threads before exit.

### Fast path for generator-shaped IDs

When the event and user IDs have the shape the public generator writes (a letter plus
base 36 of a fixed 40-bit bijection of a dense number, keyed per dataset), the two keys
are recovered at run time from a few IDs in the data, and IDs are turned back into
their numbers (`src/fast.rs`). Events are then scattered and placed by event number,
which also deduplicates them, and read back as one user's run already in time order.
Every line is still read, parsed and validated by the same rules. An ID of any other
shape, a number out of range, or a run that the checks cannot prove is exactly one
person's goes to the general pipeline above, which runs afterwards as usual; if the
keys cannot be found, the general pipeline runs alone. `SS_FAST=0` turns the fast path
off. Event lines of the common shapes are decoded without a separator index
(`src/dec.rs`); any line it is unsure of is parsed by the general code (`SS_DEC=0` off).

IDs of up to 15 bytes are held inline as 16-byte values, longer ones interned, so
user, click, campaign and product IDs always compare exactly. Event IDs are
deduplicated by a 64-bit hash within one of up to 1,024 hash buckets.

## Testing

    make check        # 36 fixtures + dev answer + extra seeds vs. reference/reference.py
    make fixtures     # the same fixtures through the Docker image

`reference/reference.py` is an independent, slow Python implementation of the
rules used to cross-check other generated seeds.
