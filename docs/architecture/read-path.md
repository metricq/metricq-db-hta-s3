# Read path

A history request is answered from a snapshot: the engine briefly takes the
ingestion lock, pins the current manifest generation and shares references to
the metric's unflushed records. The rest runs without locks.

1. **Choose the level.** `LAST_VALUE` returns the in-memory last sample.
   `FLEX_TIMELINE` chooses the raw level or the largest aggregate level
   within `interval_max`; `AGGREGATE` uses the raw level's index (below).
2. **Walk the index** of that stream to the blocks overlapping the requested
   window. Index pages are cached (8192 pages, shared). Raw queries also fetch
   the neighbouring blocks for boundary values.
3. **Fetch blocks.** Blocks already decoded are served from a shared 128 MiB
   cache keyed by content hash. Missing blocks in the same object are coalesced
   into one range GET (gaps up to 64 KiB, up to 8 MiB per request, 8 requests in
   parallel); each block is verified by its SHA-256 and decoded in parallel.
4. **Add unflushed records** (records being uploaded first, then pending ones)
   and build the response.

A single `AGGREGATE` takes the stored aggregates of index entries whose
values, and the durations preceding them, lie inside the window: whole
subtrees for the middle of the window. It descends only along the two window
borders and reads the border blocks, in one round trip, so its cost does not
grow with the window length. This plays the role of the HTA decomposition
over levels (Ilsche 2020, Figure 4.5), whose border reads would cost one block
per level and border here.

Limits: before reading any block, a query sizes its read from the index
(records per block). It fails if the response would exceed
`query_max_response_bytes` (estimated at 13 bytes per raw value and 53 per
aggregate; the encoded response is checked exactly before it is returned),
and it reserves memory for the decoded records (128 bytes each, held twice)
from `query_memory_bytes`, shared by all running queries. A query that does
not fit waits until others finish, within the request timeout; only its first
reservation waits, so queries holding memory never wait for each other. The
executable passes the same limit to `metricq-go`, which replaces any larger
reply by an error response: RabbitMQ closes the channel on an oversized
message, and the unacknowledged request would otherwise be redelivered and
interrupt the whole session, including data consumption, again and again. A query does not hold the ingestion lock during I/O, and a running
checkpoint does not block queries.

## Why layout matters

The number of range GETs of a cold query is the number of distinct, non-adjacent
byte ranges it needs. Blocks of one stream written by many checkpoints live in
many objects; compaction's *level locality* rewrites consecutive blocks of a
metric level into one contiguous section, so a typical `FLEX_TIMELINE`
request needs one or two GETs (see [Maintenance](maintenance.md)).
