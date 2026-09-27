# Read path

A history request is answered from a snapshot: the engine briefly takes the
ingestion lock, pins the current manifest generation and shares references to
the metric's unflushed records. The rest runs without locks.

1. **Choose the level.** `LAST_VALUE` returns the in-memory last sample.
   `FLEX_TIMELINE` chooses the raw level or the largest aggregate level
   within `interval_max`; `AGGREGATE` decomposes the interval over several
   levels.
2. **Walk the index** of that stream to the blocks overlapping the requested
   window. Index pages are cached (8192 pages, shared). Raw queries also fetch
   the neighbouring blocks for boundary values; aggregate queries do not.
3. **Fetch blocks.** Blocks already decoded are served from a shared 128 MiB
   cache keyed by content hash. Missing blocks in the same object are coalesced
   into one range GET (gaps up to 64 KiB, up to 8 MiB per request, 8 requests in
   parallel); each block is verified by its SHA-256 and decoded in parallel.
4. **Add unflushed records** (records being uploaded first, then pending ones)
   and build the response.

Limits per request: `query_max_rows` output rows and 256 MiB of decoded
records. A query does not hold the ingestion lock during I/O, and a running
checkpoint does not block queries.

## Why layout matters

The number of range GETs of a cold query is the number of distinct, non-adjacent
byte ranges it needs. Blocks of one stream written by many checkpoints live in
many objects; compaction's *level locality* rewrites consecutive blocks of a
metric level into one contiguous section, so a typical `FLEX_TIMELINE`
request needs one or two GETs (see [Maintenance](maintenance.md)).
