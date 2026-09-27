# Code structure

```
cmd/metricq-db-hta-go/       executable: CLI, MetricQ adapter, Prometheus endpoint
cmd/metricq-db-hta-wal-repair/  offline WAL inspection and repair
cmd/check-ceph-s3/           probe for the S3 features the database needs
engine/                      storage engine (all persistence and queries)
hta/                         HTA aggregation of one metric (no I/O)
storage/                     object store interface and S3 implementation
integration/                 tests against RabbitMQ, CouchDB, the manager and S3
grafana/                     dashboard and its generator
docker/                      container entrypoint, development compose file
docs/                        this documentation (MkDocs, published by GitLab Pages)
measurements/                benchmark reports and design history
```

The MetricQ client is [`metricq-go`](https://github.com/metricq/metricq-go),
pinned in `go.mod` to the commit that introduced the batched data handler
(`DB.DataBatch`). To work on both at once, add
`replace github.com/metricq/metricq-go => ../metricq-go` locally and do not
commit it.

## Packages

**`hta`** implements the aggregation: `Series.Insert` consumes one sample and
emits completed records for every level through a callback. It has no
knowledge of storage and is deterministic, which WAL replay relies on.

**`storage`** defines `Store` (GET with version, conditional PUT) and optional
capabilities (`RangeGetter`, `Deleter`, `Lister`), and implements them for S3.
A different backend must preserve the conditional PUT semantics.

**`engine`** is the database. Its public surface is small: `Open`,
`IngestBatch`/`Ingest`, `Query`, `Flush`, `RunFlush`, `RunMaintenance`,
`CompactOnce`, `Reclaim`, `Configure`, `Close`, `NewMetrics` and the option
types.

| File | Contents |
| --- | --- |
| `engine.go` | `Engine`, `Options`, `Open` (manifest load, WAL replay), `IngestBatch`, `Flush` (checkpoint), `RunFlush` |
| `wal.go` | WAL segments, frames, rotation, replay; `repair.go` inspection and truncation |
| `pending.go` | Unflushed records per stream, shareable with query snapshots |
| `hold.go` | Holding streams: planning which records a checkpoint writes, held deltas, recovery |
| `index.go`, `index_tail.go`, `index_cache.go` | Copy-on-write block index per stream, pinned rightmost paths, page cache |
| `query.go`, `block_cache.go`, `ranges.go` | History requests, decoded block cache, coalesced range reads |
| `manifest_pages.go`, `publication.go` | Paged metadata (series state, stream roots, held state), manifest encoding and cloning |
| `catalog.go` | Object catalog and candidate tree |
| `compaction.go` | Compaction jobs: selection, copy, publication, abort and recovery, garbage collection |
| `locality.go` | Selection of level-local layout jobs |
| `maintenance.go` | Compaction options, trash journal, maintenance publication, `RunMaintenance`, catalog bootstrap |
| `gc.go` | Reference counting GC used without background maintenance |
| `metrics.go` | Prometheus metrics |

## Invariants to keep

- **ACK after fsync.** Nothing may change in-memory state or acknowledge a
  delivery before its WAL frame is durable (`IngestBatch`).
- **Immutable objects.** Every key except `manifest` is written with
  `If-None-Match: *` and never overwritten. New state means new objects plus a
  conditional manifest PUT.
- **Publication order.** Upload everything a manifest references before the
  manifest; release WAL segments or trash objects only after it.
- **Locks.** `publishMu` before `e.mu`; no object store I/O while holding
  `e.mu` except where documented; maintenance never takes `e.mu` for I/O.
- **Snapshots.** Queries and maintenance work on cloned manifests and pinned
  generations. Structures shared with snapshots (pending stream slices, cached
  index pages) are append-only or immutable.
- **Deletion only through the trash journal**, after the retiring publication,
  respecting generation pins.

## Conventions

- Errors that must stop the engine set `e.fatal`; transient errors are returned
  and retried by the caller.
- Size estimates (`pendingRecordBytes`, object targets) are estimates of
  uncompressed in-memory size, not of stored bytes.
- New limits get an option with a default, validation in `Open`, an entry in
  `setConfigMetrics`, and documentation in *Configuration* and *Tuning*.
- New metrics are added in `metrics.go`; regenerate the reference in
  *Monitoring* with `scripts/metrics-reference.py` and add a panel in
  `grafana/generate_dashboard.py` if operators need it.
