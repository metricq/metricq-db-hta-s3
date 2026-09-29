# Tuning

Start with the defaults of the executable. Change a parameter only when the
[dashboard](monitoring.md) shows the symptom it addresses, and change one at a
time. All effective values are exported as `metricq_db_config{parameter=…}`,
so the dashboard draws them next to the measured values.

## Symptom → parameter

| Symptom (dashboard panel / metric) | Likely cause | What to change |
| --- | --- | --- |
| **Backpressure** = 1, `ingest_backpressure_events_total` rising, RabbitMQ queue grows | WAL or ingest memory limit reached: checkpoints too slow or S3 down | Check *Checkpoint errors* and *Object store → Errors* first. If S3 is healthy: raise `wal_high_bytes`/`wal_hard_bytes` (disk) or `ingest_memory_limit_bytes` (memory). |
| *Deliveries per fsync* p50 ≈ 1 while samples/s is high | batches too small, fsync-bound | Raise `ingest_prefetch` (50–200 is a good range). |
| *Deliveries per fsync* p95 = prefetch and backpressure events | batches reach the ingest memory limit | Lower `ingest_prefetch` or raise `ingest_memory_limit_bytes`. |
| *WAL fsync latency* p99 > 10 ms | slow WAL disk | Move the WAL to an SSD; batching (prefetch) amortizes it. |
| Many *Checkpoints by trigger* `wal`/`object_target` per minute, *Blocks written* mostly `partial` | holding disabled or too small | Enable/raise `hold_max_age_seconds`; check `hold_memory_bytes`. |
| Checkpoints `hold_budget`, *Held memory* at the hold budget | held set larger than `hold_memory_bytes` | Raise `hold_memory_bytes` (and `ingest_memory_limit_bytes` above it) if memory allows. |
| *Oldest held stream* sawtooth reaches `hold_max_age_seconds`, `hold_age` checkpoints | normal | — Increase `hold_max_age_seconds` to reduce fragments further; costs replay time and memory. |
| *Checkpoint duration* p95 grows | S3 PUT latency, many streams | Check *Request latency*; larger `checkpoint_unsaved_bytes` makes fewer, larger checkpoints. |
| *Fragmentation* (small blocks) keeps rising, *Blocks in and out* flat | compaction cannot keep up | Raise `compaction_io_bytes_per_second`, `compaction_cycle_max_seconds`; lower `compaction_cycle_interval_seconds`; reduce fragment creation via holding. |
| *Jobs*: `catalog budget` events | inventories of mixed objects are large | Usually self-adjusting (*source-object limit* drops). Persistent: holding reduces mixed objects. |
| *Compaction job pending* stays 1, *Last successful job* grows | jobs fail repeatedly | Look at the logs (`compaction failed`) and *Object store → Errors*. |
| *Stored bytes*: dead share high and not shrinking | reclamation too slow or disabled | Lower `compaction_reclaim_dead_fraction` (more rewriting) or raise the rate limit. |
| Cold queries slow, many GETs per query | fragmented layout | Ensure locality is enabled; lower `compaction_locality_min_ranges`; give compaction more rate. |
| `query_errors_total`: "exceeds maximum rows" | client requests too many raw points | Raise `query_max_rows` or ask clients for aggregate levels. |
| Process memory high | held records, caches | Lower `hold_memory_bytes`, `ingest_memory_limit_bytes`; see [Sizing](sizing.md#memory). |

## Parameters by area

### Ingestion

- **`ingest_prefetch`** — upper bound of deliveries per WAL fsync. Too small: one fsync
  per few deliveries, throughput bound by disk latency. Too large: a batch
  alone can exceed the ingest memory limit and forces an inline checkpoint.
  Measured optimum 50–200 for 500-sample chunks.
- **`ingest_memory_limit_bytes`** — memory for records not yet in blocks. Reaching it
  refuses deliveries (backpressure) until a checkpoint frees memory.

### WAL

- **`wal_target_bytes`** — a checkpoint starts when the active segment reaches
  it. Lower: more frequent, smaller checkpoints, less replay after a crash.
- **`wal_high_bytes`** — ingestion stops above it. The difference to the target
  is the buffer for slow or failing checkpoints: at *R* bytes/s of WAL, the
  database rides out about (high − target) / *R* seconds of S3 outage before
  pushing back to RabbitMQ.
- **`wal_hard_bytes`** — absolute bound, must fit on the WAL disk.

### Checkpoints and holding

- **`checkpoint_unsaved_bytes`** — records that exist only in the WAL (not in blocks,
  not in held deltas) trigger a checkpoint at this size. Larger: fewer
  checkpoints and manifests, more replay.
- **`hold_max_age_seconds`** — maximum time a stream is kept in memory before being
  written as a partial block. The main lever against fragmentation: fragments
  per hour ≈ active streams × 3600 / `hold_max_age_seconds`. Costs memory (records of
  the last `hold_max_age_seconds` of slow streams) and delta traffic.
- **`hold_memory_bytes`** — memory budget for held records. Exceeding it writes the
  largest streams early. Size it above the steady-state held set (see
  *Held memory*).
- **`hold_expiry_interval_seconds`** — groups age-triggered checkpoints; larger
  values mean fewer checkpoints, streams are written up to this much later.

### Compaction

- **`compaction_cycle_interval_seconds`**, **`compaction_cycle_max_seconds`** — how often and how long
  compaction runs. Duty cycle ≈ cycle_max / cycle_interval.
- **`compaction_io_bytes_per_second`** — rate limit of maintenance I/O, protects S3 and
  queries. The main lever for compaction throughput.
- **`compaction_job_max_blocks`**, **`compaction_job_max_bytes`** — job size. Larger jobs amortize the
  per-job metadata cost; each job takes a catalog snapshot and one publication.
- **`compaction_merge_cooldown_seconds`** — do not merge objects younger than this; avoids
  re-merging a growing tail repeatedly.
- **`compaction_output_object_bytes`** — output pack size. Larger packs mean fewer objects; a pack
  is deleted only when all its blocks are dead.
- **`compaction_reclaim_dead_fraction`** — rewrite objects with at least this share of dead bytes.
  Lower reclaims space sooner at the price of more copying.
- **`compaction_locality_min_ranges`**, **`compaction_locality_disabled`** — lay out consecutive blocks
  of a metric level contiguously so a `FLEX_TIMELINE` query needs one or two
  GETs.

## Reading the fragmentation panels

`storage_small_blocks` counts every block below 1024 records, including the
newest partial block of each stream, which cannot be merged. A stable value
around the number of streams is healthy; only a steady rise means compaction
falls behind. `checkpoint_blocks_total{size="partial"}` is the inflow of new
fragments; `compaction_input_blocks_total − compaction_output_blocks_total`
is the outflow.

### Compaction throughput and metadata

`compaction_continuous=true` wakes the background worker after a successful
checkpoint and resumes a remaining backlog after the cycle start window.
The default is false. Enable it when the idle interval limits backlog removal;
the shared I/O limiter remains active across consecutive cycles. WAL pressure,
a due checkpoint, interrupted jobs, no useful work and cancellation stop the
continuation. More CPU time can then be spent on maintenance; check query and
ACK latency as well as completed jobs.

The compactor shares a 32 MiB decoded catalog LRU across phases, prefetches
known needed index pages with coalesced checked ranges, and publishes several
consecutive metric/level sections per locality job within the same source
budgets. Merge compression uses at most eight workers inside the single
coordinator. It never groups by time windows.

With a positive merge cooldown, a partial block with at least 256 records gains
at least 25% before another merge. A prefix that cannot fit its next block is
finished immediately. Sparse tails become eligible after at least one hour
(or four cooldown periods, whichever is greater) without newer source data.
Zero cooldown keeps aggressive consolidation. This limits repeated tail writes,
while allowing a query to temporarily read additional fragments.

Large catalog inventories use immutable pages of up to 128 descriptors. Editing
a few blocks no longer rewrites the complete inventory, but may increase PUT
count. Inventory packs upload through a bounded pipeline with at most four
in-flight PUTs. Preparation and staging-key registration stay serial; no pack
shares inventory pages between owners. Normal packs target 4 MiB, so four
uploads plus the currently prepared pack retain roughly 20 MiB of encoded
payload; buffer capacity and encoding scratch use additional memory. A single
exceptional page may reach 32 MiB; this is a per-pack limit,
not a total memory limit. Errors cancel the queue, join all workers and prevent
publication; failed PUTs are not retried. The catalog is written only after all
inventory uploads succeed. Partly live metadata packs are retained until their
last page is retired.
The throughput measurement (`measurements/compaction-throughput.md`) records
both bytes and requests; it is a local S3 fragmentation fixture, not a sustained
production capacity guarantee.

The inventory upload comparison (`measurements/inventory-upload-pipeline.md`)
measures the four-slot pipeline separately: catalog-phase latency decreases,
while total local compaction/GC time remains effectively unchanged.

Data blocks and index pages use a versioned binary codec with gzip BestSpeed.
Legacy Gob blocks remain readable and are rewritten only by ordinary appends or
compaction. Fixed fields avoid per-block Gob schemas; full raw blocks can still
be slightly larger. See the codec measurements (`measurements/binary-block-codec.md`)
for allocation, byte-volume and compaction results.
