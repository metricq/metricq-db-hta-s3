# Troubleshooting

Run with `-v info` (or `debug`) for more log output; logs go to standard error.

## The database does not become ready

`/readyz` returns 503 until the manager sent a configuration and the engine
opened.

| Log message | Cause and fix |
| --- | --- |
| connection errors to RabbitMQ, process exits | Wrong `--server` or broker not reachable. In containers use `wait_for_rabbitmq_url` or a restart policy. |
| `manager config requires metrics` | The CouchDB `config` document for the token is missing or has no `metrics`. |
| `ambiguous input "…"` | Two metrics in the configuration use the same `input`. |
| `WAL already in use` | Another process (or a stale container) holds the WAL directory lock. |
| `WAL belongs to another backend namespace` | The WAL was written for another endpoint/region/bucket/prefix. Point the database back to its original storage; never copy WAL directories between databases. |
| `remote manifest is older than local WAL GC checkpoint` | The bucket prefix was restored from an older backup or belongs to another instance. Do not continue until the storage state is explained: continuing would lose data covered by the local checkpoint. |
| `changing HTA configuration for "…" requires migration` | `interval_min`, `interval_max` or `interval_factor` of an existing metric changed. Restore the old values; store differently aggregated data under a new metric name. |
| `WAL aggregation config changed for "…"` / `WAL metric … missing from configuration` | The WAL contains data for a metric whose configuration changed or disappeared. Restore the configuration, let the database checkpoint, then change it. |
| `recovered WAL exceeds configured limits` | WAL limits were reduced below the current WAL size. Restore the previous limits for one start. |
| `invalid engine options` / `invalid compaction options` / `hold_memory_bytes must be below ingest_memory_limit_bytes` | See constraints in [Configuration](configuration.md). |

## WAL does not replay

Messages such as `incomplete WAL payload at offset …`, `WAL checksum mismatch
at …` or `invalid WAL frame at …` mean the active WAL segment ends with a torn or
damaged frame, typically after a power loss. Startup refuses to discard data.

1. Stop the database. Inspect:

    ```sh
    metricq-db-hta-s3-wal-repair -wal-dir /var/lib/metricq-db-hta-s3/wal
    ```

    The report shows the first damaged frame, the last verified boundary
    (`valid_bytes`), the last sequence and a SHA-256 of the file.

2. If the damage is explained, truncate at the boundary, keeping a backup:

    ```sh
    metricq-db-hta-s3-wal-repair -wal-dir /var/lib/metricq-db-hta-s3/wal -apply \
      -expected-sha256 <sha256> -truncate-at <valid_bytes> \
      -backup /safe/place/ingest.wal.before-repair
    ```

The tool refuses to truncate if the file changed, the offset is not the
verified boundary, the backup fails, or the database still holds the lock.
**Discarded frames may contain acknowledged samples not yet in S3.** Frozen
segments (`ingest.wal.<sequence>`) were fully synced before rotation and are
not repaired by the tool.

## Backpressure

`metricq_db_ingest_backpressure` = 1: deliveries are refused and stay in RabbitMQ.

- **Checkpoints fail** (`checkpoint_errors_total`, *Object store → Errors*): S3 is
  down, credentials expired, quota exhausted. Acknowledged data is safe in the
  WAL; ingestion resumes by itself when checkpoints succeed. A full quota can
  be relieved by garbage collection, which deletes before it writes.
- **Checkpoints too slow**: check *Checkpoint duration* and S3 latency; see
  [Tuning](tuning.md).
- **Ingest memory limit**: *Held memory* at `ingest_memory_limit_bytes` — raise it or lower
  `hold_memory_bytes`/`ingest_prefetch`.

## Checkpoint errors: `another writer changed manifest`

A second process writes to the same bucket prefix. The database stops
ingesting to avoid corrupting the state. Make sure exactly one instance owns
the prefix, then restart.

## Compaction does not progress

- *Compaction job pending* = 1 for more than a few minutes: a job failed and
  waits for cleanup (one minute), or cleanup fails — check logs for
  `compaction recovery failed` and object store errors.
- *Jobs* shows only `no work` while *Fragmentation* rises: the fragments are in
  objects younger than `compaction_merge_cooldown_seconds`, or every stream has only its newest
  partial block (not mergeable). Both are normal.
- *Jobs* shows `catalog budget`: the job limit adapts; persistent occurrences
  indicate very large mixed objects (enable holding).
- Throughput at the rate limit (*Throughput* touches *rate limit*): raise
  `compaction_io_bytes_per_second`.

## Slow queries

- Many GETs per query: fragmented layout; compaction and level locality fix it
  over time. Check *Fragmentation* and *Jobs*.
- `history response ... would exceed query_max_response_bytes`: the response
  would not fit into one AMQP message; request a shorter range or a larger
  `interval_max`. Raising the limit also requires a larger `max_message_size`
  in RabbitMQ.
- `history query needs ... more than query_memory_bytes`: a single query would
  decode more records than all queries together may; narrow the time range or
  raise `query_memory_bytes` if memory allows.
- `waiting for query memory` / `query memory budget exhausted`: concurrent
  large queries use the budget; the request may be retried.

## Orphaned objects

A checkpoint or compaction that uploaded objects but never published a
manifest leaves unreferenced objects. Aborted compaction jobs are cleaned up
automatically; objects of failed checkpoints are not. They waste space but do
not affect correctness. Compare `metricq_db_storage_live_bytes` with the
bucket usage below the prefix to estimate them.

## Data safety checklist

- The WAL directory is on persistent storage and backed by the same host as the
  process.
- Exactly one instance per bucket prefix.
- Bucket versioning either disabled or with lifecycle expiration of noncurrent
  versions.
- Stop with SIGTERM and enough timeout for the final checkpoint.
