# Paged metadata and level locality

Implementation and measurements on 2026-09-27, AMD Ryzen 7 PRO 4750U,
local RustFS. No additional Go libraries. Baseline:
[capacity review at c0d3a3d](capacity-review-c0d3a3d.md).

## Implemented

- Series state and stream roots are stored in independent hash-partitioned
  immutable pages. A changed kind uploads one pack containing its changed pages
  plus a directory. The CAS manifest contains their references, not metric maps.
  Held-delta references/watermarks have a separate immutable metadata root.
- Maintenance reuses committed immutable Series and untouched per-metric root
  maps. Changed metric maps use copy-on-write. Encoding, uploads and the commit
  snapshot copy run outside `e.mu`; `publishMu` still serializes publications.
- The compactor coalesces physical input ranges across its bounded selected job,
  retains logical order and verifies every block hash. Pinned adjacency evidence
  avoids repeated neighbor lookups. Final exact source validation is retained.
- Catalog validation and retirement process each source inventory once rather
  than searching/splicing it once per input block.
- Age-only background flushes batch deadlines on a configurable 30-second
  cadence. Explicit flush and pressure paths remain immediate.
- Locality selects consecutive blocks of a canonical metric and HTA level,
  including full blocks, by physical job/object bounds. **No time windows.**
  New appends do not trigger rewrites of sealed locality sections. Ordinary
  partial-block merges and reclamation can still replace affected blocks.
- Locality jobs and stream roots awaiting layout inspection have separate
  Prometheus metrics. Inspection cursors are currently in memory; restart
  causes bounded index rescanning, not reading all historical data blocks.

The manifest CAS remains the durability boundary. A metadata upload failure
retains the acknowledged WAL prefix. Metadata objects become deletion candidates
only when their last reference disappears; query generation pins protect old
versions. Missing or corrupt persisted metadata aborts startup.

## Cold FLEX locality

Same 16,384 samples at 1 Hz; FLEX over 9000 seconds, interval_max 9 seconds,
exactly 1000 output positions. Ten cold-cache queries per stage, two repetitions.
This run explicitly uses the production 4 MiB compaction object target; the
original diagnostic used a 64 KiB test target. A 64 KiB target cannot hold this
255 KiB extent in one object. Therefore this is a before/after comparison within
one fixture, not a like-for-like latency comparison against the old report.

| Layout | Data GETs | Compressed bytes | Query median |
| --- | ---: | ---: | ---: |
| 16 checkpoints, before compaction | 9 | 254,915 | 6.8–7.0 ms |
| Same data, after compaction | 1 | 254,915 | 5.2–5.5 ms |

All answers match exactly, before/after and against the single-checkpoint
layout. Five maintenance jobs completed in the distributed layout. Storage is
in memory, with range-only copies and no simulated S3 latency; GET reduction is
the robust result, not a remote-network latency promise.

## Maintenance publication scaling

Synthetic mature state with raw plus eight aggregate roots per metric. One
metric's root changes per operation. The benchmark includes snapshot preparation,
all immutable metadata uploads to an in-memory store and final manifest encoding.
It does not include network latency. Five operations, two repetitions:

| Metrics | CAS manifest | Metadata PUT bytes/op | Time/op |
| --- | ---: | ---: | ---: |
| 150 | ~1.05 KB | ~6.8 KB | 0.58–1.95 ms |
| 1500 | ~1.05 KB | ~15.7 KB | 2.45–4.79 ms |
| 15000 | ~1.06 KB | ~52.7 KB | 12.7–13.0 ms |

The baseline encoded the full state: ~1.127 MB and 92–94 ms at 1500 metrics,
~11.31 MB and 444–504 ms at 15000. Those timings cover different operations and
were separate runs. The new benchmark still scans/copies top-level directories,
so CPU/memory work is not constant with cardinality. Its bytes no longer scale
with every metric on each small maintenance update.

## Real S3 compaction

Same fragmentation stress fixture as the baseline: 1500 metrics, 192,000
samples, eight checkpoints, holding disabled, 512-block jobs and a 64 MiB/s
maintenance budget. CPU profiling covers compaction plus GC only. No concurrent
input workload or Prometheus HTTP endpoint. The legacy parity test ran against
the same local services during the final measurement, so this is not a
controlled latency A/B experiment.

| Measurement | Baseline | New run |
| --- | ---: | ---: |
| Compaction and GC | 171.168 s | 95.801 s |
| Manifest PUT bytes | 126,177,366 | 398,509 |
| Stream-root metadata PUT bytes | inline above | 15,824,661 |
| Catalog PUT bytes | 64,345,875 | 77,153,909 |
| Data Range GETs | 50,723 | 604 |
| Data Range bytes | 22,169,090 | 26,885,745 |
| Index Range GETs | 38,586 | 26,230 |
| Catalog Range bytes | 108,580,504 | 137,158,941 |
| Raw blocks before → after | 12,000 → 1500 | 12,000 → 1500 |

The catalog is still expensive, and the new run does not improve its byte
volume. Coalescing trades modest extra range bytes for fewer requests. The
remaining 26k index GETs and catalog serialization are the next measured targets.
The CPU profile sampled 64.80 CPU seconds over 95.90 seconds; decode accounts
for 19.95 cumulative seconds, encode 12.50, and catalog reads 10.59. Inclusive
figures overlap and must not be added as independent shares.

All 192,000 raw samples remain present; the final data/index footprint is
6,747,005 bytes. All 24 comparisons after S3-only recovery passed. The separate
AMQP comparison against the old file-based DB passed **2875 response pairs**,
including all four request types, smoothing, boundary/gap cases, WAL restart,
S3-only restart, compaction, restart after compaction and outage backpressure.

[Complete S3 counters](compaction-capacity-paged-io.csv),
[layout/timing CSV](compaction-capacity-paged.csv), and
[raw focused results](paged-manifest-and-level-locality.txt).

## Tests and reproduction

New regression tests cover whole-level packing across million-second gaps,
unchanged sealed prefixes after more ingestion, recovery after compaction,
failed-job reselection, per-block corruption in a coalesced GET, metadata upload
failure with WAL recovery, startup failure on missing/corrupt pages, shared-page
GC and deadline batching without delaying pressure. Existing blocked-upload
concurrency tests now include the state/root metadata objects.

```sh
GOCACHE=/tmp/metricq-go-build-cache go test ./...
GOCACHE=/tmp/metricq-go-build-cache go test -race ./engine
GOCACHE=/tmp/metricq-go-build-cache go test -tags=review ./engine \
  -run '^TestReviewFullBlockQueryLocality$' -count=2 -v
GOCACHE=/tmp/metricq-go-build-cache go test -tags=review ./engine \
  -run '^$' -bench '^BenchmarkReviewPagedMaintenance$' -benchtime=5x -count=2
GOCACHE=/tmp/metricq-go-build-cache METRICQ_COMPACTION_METRICS=1500 \
  METRICQ_COMPACTION_OUTPUT=measurements/compaction-capacity-paged.csv \
  METRICQ_COMPACTION_CPU_PROFILE=/tmp/hta-compaction-paged.pprof \
  go test -tags=integration ./integration -run '^TestCompactionS3$' \
  -count=1 -v -timeout=15m
GOCACHE=/tmp/metricq-go-build-cache go test -tags=integration ./integration \
  -run '^TestLegacyRequestParity$' -count=1 -v -timeout=5m
```
