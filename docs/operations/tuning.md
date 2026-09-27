# Tuning

Start with the defaults of the executable. Change a parameter only when the
[dashboard](monitoring.md) shows the symptom it addresses, and change one at a
time. All effective values are exported as `metricq_db_config{parameter=…}`,
so the dashboard draws them next to the measured values.

## Symptom → parameter

| Symptom (dashboard panel / metric) | Likely cause | What to change |
| --- | --- | --- |
| **Backpressure** = 1, `backpressure_events_total` rising, RabbitMQ queue grows | WAL or builder limit reached: checkpoints too slow or S3 down | Check *Checkpoint errors* and *Object store → Errors* first. If S3 is healthy: raise `wal_high_bytes`/`wal_hard_bytes` (disk) or `builder_hard_bytes` (memory). |
| *Deliveries per fsync* p50 ≈ 1 while samples/s is high | batches too small, fsync-bound | Raise `prefetch` (50–200 is a good range). |
| *Deliveries per fsync* p95 = prefetch and backpressure events | batches reach the builder limit | Lower `prefetch` or raise `builder_hard_bytes`. |
| *WAL fsync latency* p99 > 10 ms | slow WAL disk | Move the WAL to an SSD; batching (prefetch) amortizes it. |
| Many *Checkpoints by trigger* `wal`/`object_target` per minute, *Blocks written* mostly `partial` | holding disabled or too small | Enable/raise `hold_seconds`; check `hold_bytes`. |
| Checkpoints `hold_budget`, *Held memory* at the hold budget | held set larger than `hold_bytes` | Raise `hold_bytes` (and `builder_hard_bytes` above it) if memory allows. |
| *Oldest held stream* sawtooth reaches `hold_seconds`, `hold_age` checkpoints | normal | — Increase `hold_seconds` to reduce fragments further; costs replay time and memory. |
| *Checkpoint duration* p95 grows | S3 PUT latency, many streams | Check *Request latency*; larger `object_target_bytes` makes fewer, larger checkpoints. |
| *Fragmentation* (small blocks) keeps rising, *Blocks in and out* flat | compaction cannot keep up | Raise `compaction.bytes_per_second`, `max_cycle_seconds`; lower `interval_seconds`; reduce fragment creation via holding. |
| *Jobs*: `catalog budget` events | inventories of mixed objects are large | Usually self-adjusting (*source-object limit* drops). Persistent: holding reduces mixed objects. |
| *Compaction job pending* stays 1, *Last successful job* grows | jobs fail repeatedly | Look at the logs (`compaction failed`) and *Object store → Errors*. |
| *Stored bytes*: dead share high and not shrinking | reclamation too slow or disabled | Lower `compaction.dead_fraction` (more rewriting) or raise the rate limit. |
| Cold queries slow, many GETs per query | fragmented layout | Ensure locality is enabled; lower `compaction.locality_min_ranges`; give compaction more rate. |
| `query_errors_total`: "exceeds maximum rows" | client requests too many raw points | Raise `max_query_rows` or ask clients for aggregate levels. |
| Process memory high | held records, caches | Lower `hold_bytes`, `builder_hard_bytes`; see [Sizing](sizing.md#memory). |

## Parameters by area

### Ingestion

- **`prefetch`** — upper bound of deliveries per WAL fsync. Too small: one fsync
  per few deliveries, throughput bound by disk latency. Too large: a batch
  alone can exceed the builder limit and forces an inline checkpoint.
  Measured optimum 50–200 for 500-sample chunks.
- **`builder_hard_bytes`** — memory for records not yet in blocks. Reaching it
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

- **`object_target_bytes`** — records that exist only in the WAL (not in blocks,
  not in held deltas) trigger a checkpoint at this size. Larger: fewer
  checkpoints and manifests, more replay.
- **`hold_seconds`** — maximum time a stream is kept in memory before being
  written as a partial block. The main lever against fragmentation: fragments
  per hour ≈ active streams × 3600 / `hold_seconds`. Costs memory (records of
  the last `hold_seconds` of slow streams) and delta traffic.
- **`hold_bytes`** — memory budget for held records. Exceeding it writes the
  largest streams early. Size it above the steady-state held set (see
  *Held memory*).
- **`hold_expiry_batch_seconds`** — groups age-triggered checkpoints; larger
  values mean fewer checkpoints, streams are written up to this much later.

### Compaction

- **`interval_seconds`**, **`max_cycle_seconds`** — how often and how long
  compaction runs. Duty cycle ≈ max_cycle / interval.
- **`bytes_per_second`** — rate limit of maintenance I/O, protects S3 and
  queries. The main lever for compaction throughput.
- **`max_blocks`**, **`max_job_bytes`** — job size. Larger jobs amortize the
  per-job metadata cost; each job takes a catalog snapshot and one publication.
- **`cooldown_seconds`** — do not merge objects younger than this; avoids
  re-merging a growing tail repeatedly.
- **`object_bytes`** — output pack size. Larger packs mean fewer objects; a pack
  is deleted only when all its blocks are dead.
- **`dead_fraction`** — rewrite objects with at least this share of dead bytes.
  Lower reclaims space sooner at the price of more copying.
- **`locality_min_ranges`**, **`disable_locality`** — lay out consecutive blocks
  of a metric level contiguously so a `FLEX_TIMELINE` query needs one or two
  GETs.

## Reading the fragmentation panels

`small_data_blocks` counts every block below 1024 records, including the
newest partial block of each stream, which cannot be merged. A stable value
around the number of streams is healthy; only a steady rise means compaction
falls behind. `checkpoint_blocks_total{size="partial"}` is the inflow of new
fragments; `compaction_input_blocks_total − compaction_output_blocks_total`
is the outflow.
