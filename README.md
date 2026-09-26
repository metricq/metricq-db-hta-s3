# metricq-db-hta-go

MetricQ history database in Go, using `metricq-go` for AMQP and immutable S3 objects for storage. It implements HTA's preaggregated hierarchy, all four history request types, a bounded local WAL and an integrated Prometheus endpoint.

The design follows the [object-store discussion](https://chatgpt.com/share/6ab62ce6-c8d8-83eb-98a7-c34138064738). Measurement timestamps close aggregation intervals; record volume and WAL pressure seal storage objects. Old RabbitMQ backlog is processed using measurement time, regardless of today's date.

## Build and run

Requires Linux and Go 1.25 or newer. Keep the extended `metricq-go` checkout alongside this repository; `go.mod` uses a local `replace` until that DB API is released.

```sh
go build -o metricq-db-hta-go ./cmd/metricq-db-hta-go
go build -o metricq-db-hta-wal-repair ./cmd/metricq-db-hta-wal-repair
cp configs/local.example.json config.json
```

Create an S3 bucket, edit `config.json`, and add the contents of `configs/manager.example.json` as the manager's CouchDB `config/db-hta-go` document. That example stores the existing `dummy.source` stream under the separate history name `dummy.source.go`. Never give two databases the same history binding unless intentionally requesting multiple responses.

Credentials use the standard AWS SDK credential chain. For a local S3 service:

```sh
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
./metricq-db-hta-go -config config.json
curl http://127.0.0.1:9090/metrics
```

The HTTP listener defaults to loopback. `/readyz` returns 200 after the engine has opened. SIGINT/SIGTERM stops AMQP workers and attempts a final checkpoint; if S3 is unavailable, acknowledged data remains in the local WAL for restart.

The S3 namespace must be exclusively owned by one database. The backend must support strongly consistent reads and conditional `PutObject` with `If-Match` and `If-None-Match`. Integration tests check both conditions before using the backend. Keys are prefixed by the configured `s3.prefix`.

For conditional manifest updates, the S3 adapter sends `If-Match` with the ETag's outer quotation marks removed. This is required by the tested Ceph endpoint and accepted by AWS S3 and MinIO. The S3 client makes one attempt per call, including conditional PUTs. A rejected PUT is returned as a conflict; an uncertain response is reconciled by reading the manifest back before any WAL reclamation.

## Storage and durability

1. Validate a chunk, filter points the legacy HTA would discard, and check the accepted points' estimated aggregation expansion against the capacity limits.
2. Append the accepted points as one checksummed WAL frame and `fsync` it. All accepted points in that AMQP chunk share one sync.
3. Apply it to per-metric aggregation states, then ACK the AMQP delivery.
4. At object-size or WAL-target thresholds, pack independently compressed blocks by canonical metric and HTA level into immutable data objects. Append their references to copy-on-write index pages and upload the index pack. Repeated gap aggregates are stored as runs.
5. Atomically publish the manifest with its sequence, per-stream index roots and open aggregation states using a conditional S3 PUT.
6. Persist the local GC checkpoint, then truncate and sync the WAL.

A lost manifest PUT response is reconciled by reading back the exact checkpoint before WAL reclamation. An incomplete trailing WAL frame or checksum error blocks startup and leaves the original bytes untouched for recovery. Committed sequences are skipped on replay. A local file lock excludes a second WAL writer, and a manifest version conflict fences competing remote writers. The WAL is bound to the backend identity and records each chunk's HTA configuration; startup rejects incompatible reuse or a remote manifest older than the local GC checkpoint.

### Explicit WAL repair

Stop the database before inspection. The separate repair executable takes the same exclusive WAL lock as the database and reports the first invalid frame, the end of the last verified frame, and a SHA-256 digest of the full WAL:

```sh
./metricq-db-hta-wal-repair -wal-dir /path/to/wal
```

If the report contains `damage`, investigate the failure and preserve the WAL and S3 state. To discard the damaged frame and everything after it, explicitly run the tool with the reported `sha256` and `valid_bytes`:

```sh
./metricq-db-hta-wal-repair -wal-dir /path/to/wal -apply \
  -expected-sha256 REPORTED_SHA256 -truncate-at REPORTED_VALID_BYTES \
  -backup /safe/path/ingest.wal.before-repair
```

The backup path must be new and have enough free space for the complete original WAL. The tool syncs that backup and its directory before truncating and syncing the live WAL. If the WAL changed, the offset is not the last verified boundary, the backup fails, or the database still owns the lock, it refuses to truncate. It never repairs automatically. A checksummed frame can still fail application-level replay because of configuration, manifest, or semantic state; this tool does not validate S3 state or make such a frame safe to discard. **The discarded suffix may contain ACKed points that have not reached S3.** Keep the backup until those points have been recovered or their loss has been explicitly accepted.

During a storage outage, the WAL can grow up to its high watermark (with a bounded chunk allowed beyond it, never beyond the hard limit). The pending delivery then remains unacknowledged, and bounded AMQP prefetch sends pressure back to RabbitMQ. History uses a separate channel. The local WAL must be on durable storage: losing that disk before checkpointing loses acknowledged samples.

`storage.Store` is the extension point for a future file backend. It requires a stable namespace identity, GET with an opaque version, and atomic conditional PUT. No file data backend is implemented yet; the existing local files are WAL and GC metadata only.

## Compatibility

Like the current C++ database, this implementation skips nonpositive timestamps, duplicates, out-of-order samples, NaN and infinity. These points are filtered **before** WAL append; every point in a successfully fsynced and acknowledged WAL frame must be applied during replay or startup fails without removing the WAL. Although the discussion permits non-strictly monotonic input, the legacy implementation actually accepts strictly increasing timestamps; first arrival at a timestamp wins, including AMQP redelivery.

Integrals use the **next** sample's value over the interval since the previous sample, in nanoseconds. The implementation preserves extended left timeline boundaries, partial initial buckets, absent final incomplete buckets, empty aggregates, negative-resolution requests, level fallback and legacy FLEX smoothing. As in the legacy DB, `metrics.<history-name>.input` selects the incoming MetricQ metric while the configuration key remains the name exposed by the history interface. The input binding is not part of the persisted HTA layout; changing it across a restart does not rename or reset stored history. Configuration can add metrics; changing existing aggregation parameters requires migration. Live removal or remapping of input bindings is currently rejected because the manager does not unbind the old routing key on resubscribe.

## Introspection

`/metrics` exports the Go/process collectors and `metricq_db_*` metrics, without per-metric labels:

| Area | Metrics |
| --- | --- |
| WAL pressure | `wal_bytes`, `wal_target_bytes`, `wal_high_bytes`, `wal_hard_limit_bytes`, `wal_pressure_ratio`, `backpressure` |
| WAL progress | `wal_head_sequence`, `checkpoint_sequence`, `wal_pending_frames`, `wal_oldest_timestamp_seconds`, `wal_replayed_frames_total`, `wal_errors_total`, `wal_sync_seconds` |
| Memory/ingestion | `builder_bytes`, `series`, `samples_total`, `samples_dropped_total` |
| Storage | `commits_total`, `commit_errors_total`, `last_commit_timestamp_seconds`, `objects_written_total`, `object_bytes_written_total`, `flush_seconds`, `store_get_seconds`, `store_put_seconds` |
| Queries | `queries_total`, `query_errors_total`, `query_seconds` |

For example, alert on `metricq_db_backpressure == 1` persisting for several minutes, or on `time() - metricq_db_wal_oldest_timestamp_seconds` while `metricq_db_wal_pending_frames > 0`. Histograms include `_bucket`, `_sum` and `_count` series. Ingestion counters include replay work; they are operational counters, not the total logical cardinality of stored data.

## Tests

```sh
go test -race ./...
(cd ../metricq-go && go test -race ./...)
```

The integration suite uses the existing development RabbitMQ/CouchDB/manager and starts an isolated legacy DB container. It creates uniquely named test metrics, configurations, AMQP queues and an S3 bucket, and removes those resources afterwards. Existing application metrics are not modified.

Start the auxiliary S3 test service:

```sh
docker compose -f compose.test.yml up -d
go test -race -tags=integration ./integration -count=1 -v
docker compose -f compose.test.yml down
```

Defaults and optional overrides:

| Variable | Default |
| --- | --- |
| `METRICQ_AMQP` | `amqp://admin:admin@localhost/` |
| `METRICQ_COUCHDB` | `http://admin:admin@localhost:5984` |
| `METRICQ_DOCKER_NETWORK` | `metricq_metricq-network` |
| `METRICQ_DOCKER_AMQP` | `amqp://admin:admin@rabbitmq-server/` |
| `METRICQ_LEGACY_IMAGE` | `metricq-db-hta` |
| `METRICQ_TEST_S3` | `http://localhost:19000` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `metricqtest` / `metricqtestsecret` (tests only) |

The parity matrix compares 1,725 old/new response pairs across live WAL data, WAL replay and recovery into an empty WAL directory from S3. It covers 100 Hz, irregular and daily streams, single-point and empty metrics, all request types, boundaries, gaps, duplicates, nonfinite values and smoothing. Floating-point fields use relative tolerance `1e-10`; counts, active times and timestamps must match exactly. A further storage-outage scenario checks backlog retention in RabbitMQ, query availability during pressure, resume, another S3 restart, and post-recovery aggregate parity.

The separate latency comparison follows the query matrix in Ilsche, *Energy Measurements of High Performance Computing Systems* (2020), §4.3.5: logarithmic query spans, aggregate and timeline requests, one or six metrics, and random windows repeated 20 times. It records end-to-end latency and S3 Range-GET counts and bytes, and checks response parity for every measured pair. Start the S3 test service as above, then run:

```sh
METRICQ_BENCH_OUTPUT=docs/latency-current.csv \
  go test -tags=integration ./integration -run '^TestRequestLatency$' -count=1
```

`METRICQ_BENCH_POINTS`, `METRICQ_BENCH_RATE_HZ`, `METRICQ_BENCH_SPANS`
(comma-separated seconds), `METRICQ_BENCH_REPETITIONS`, and
`METRICQ_BENCH_OBJECT_TARGET_BYTES` control the dataset and request matrix
(defaults: 20,000 points per metric, 10 samples/s, `1,10,100,1000` seconds,
20 repetitions, and 1 MiB objects). `METRICQ_BENCH_LOG_SPANS=min,max,count`
generates logarithmically spaced spans and accepts fractional seconds; it
takes precedence over `METRICQ_BENCH_SPANS`. `METRICQ_BENCH_HTA_MAX_SECONDS`
and `METRICQ_BENCH_WAL_TARGET_BYTES` set the hierarchy ceiling and WAL flush
target. `METRICQ_BENCH_INGEST_OUTPUT` writes publication and per-database
visibility rates to CSV. The default dataset is a development-machine
baseline, not a reproduction of the dissertation's six 1 kSa/s metrics over
one year. It measures only client end-to-end latency; matching server and
worker latency instrumentation is not yet present in both implementations.
The old DB stores files in the container's `/tmp`; the new DB uses the local
Docker S3 test service. See the [baseline and optimization notes](docs/latency-benchmark.md),
the [6-million-point run](docs/latency-long.md), and the
[132-million-point run over 50 durations](docs/latency-scale.md) for results.
The [checkpoint optimization report](docs/optimization.md) documents batched
index updates, aggregate tail consolidation, recovery checks and measurements.
The [metric-cardinality benchmark](docs/cardinality.md) compares many active
metrics with fixed buffer/cache limits, cold and warm queries, S3 write
amplification and a live Prometheus endpoint.

Unit tests cover crash windows, failed object/manifest writes, a lost successful PUT response, malformed/truncated/corrupt WALs, configuration/namespace guards, competing writers, query limits and ACK ordering.

## Current operational limits

This is the first implementation of the new storage format, not an in-place reader of legacy `.hta` files. There is no importer or automatic migration.

There is one writer per namespace. Ingestion and checkpoint publication are serialized; an S3 PUT can pause ingestion while its bounded attempt is in flight. Historical GETs use immutable snapshots and do not hold the ingestion lock. The `metricq-go` DB adapter uses separate AMQP channels for concurrent history workers, each with prefetch one and its own publisher confirms. The implementation does not yet have segmented incremental WAL GC, cross-message group commit, general orphan-object cleanup. It uses byte-range GETs on S3, a bounded cache of immutable index pages and a bounded root manifest; [storage v2](docs/storage-v2.md) describes the layout. Large deployments still need request-rate and latency benchmarks.

The canonical metric name is the storage and history identity; `input` is only the incoming MetricQ binding. Data blocks from different canonical metrics and HTA levels can share a sealed object while remaining independently readable. Aggregate tails are filled across checkpoints to avoid small historical blocks; fully retired packs are deleted after safe publication and release of the relevant query snapshots. An independent background compactor repacks partly live objects and merges adjacent small raw blocks. Its own schedule, rate and job limits are described in [compaction](docs/compaction-plan.md); GC also runs in that worker, outside the ingestion lock. The client derives `HistoryRequest.interval_max` from time range and display width; no separate pixel count is needed. `FLEX_TIMELINE` selects raw samples when this interval is below the metric's configured `interval_min`; otherwise it selects an available preaggregated level. Sample count does not affect that choice. Large raw scans are outside the expected workload.

Object target and builder accounting refer to estimated uncompressed record sizes; compressed S3 objects may be smaller. Queries have a configurable row cap and a 256 MiB decoded-object budget. The manifest format is version 2; there is no compatibility reader or migration for the earlier development format.
