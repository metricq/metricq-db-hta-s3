# Capacity and history locality review at c0d3a3d

Measurements on 2026-09-27, AMD Ryzen 7 PRO 4750U, local RustFS.
Production code was not changed for this review. The new tests and counters
make the measurements repeatable without adding dependencies.

## Main finding

Holding streams addresses the creation of tiny blocks. The next structural
limit is the cost and frequency of metadata publication, together with many
small S3 reads during compaction. A second issue is independent of small
blocks: current compaction does not optimize fully live, full blocks for
history locality. A catalog with no candidates does not imply an efficient
history layout.

## Real S3 compaction measurement

The existing fixture ingests 1500 metrics with 128 samples each, in eight
checkpoints of 16 samples. It uses 512-input-block jobs, a 64 MiB/s maintenance
budget and local RustFS. Holding is **disabled** to generate fragmentation.
The measured interval includes bounded compaction jobs and GC, excluding
ingestion, baseline GC, layout audit and recovery verification. CPU profiling
was enabled only for that interval. There was no concurrent ingestion or
history load, and no Prometheus HTTP endpoint.

| Observation | Result |
| --- | ---: |
| Samples | 192,000 |
| Time for compaction and GC | 171.168 s |
| Passes to small-block threshold | 130 |
| Data/index bytes before / after | 21,841,352 / 6,737,293 |
| Raw blocks before / after | 12,000 / 1500 |
| Response pairs after S3-only recovery | 24, all matched |
| Manifest PUTs / bytes | 287 / 126,177,366 |
| Catalog PUTs / bytes | 1040 / 64,345,875 |
| New data-object PUT bytes | 14,023,960 |
| New index-object PUT bytes | 16,938,293 |
| Data Range GETs / bytes | 50,723 / 22,169,090 |
| Index Range GETs / bytes | 38,586 / 24,575,260 |
| Catalog Range GETs / bytes | 2306 / 108,580,504 |

All PUTs together wrote 225,018,201 bytes. Manifests alone account for 56.1%;
manifests plus catalog pages account for 84.7%. Data and index reads average
only hundreds of bytes per GET. The [complete I/O counters](compaction-capacity-c0d3a3d-io.csv)
include all measured categories. Summed request times overlap because requests
can run concurrently; they must not be added as elapsed runtime.

The [older fixture](compaction-optimized-s3.csv) took 162.958 s and reached the
threshold in 108 passes. This is a separate run, with randomized object names,
host/cache variation and different instrumentation. The new result does not
establish either a speedup or a causal regression. It also does not measure
the sustainable input rate with holding enabled.

CPU profile: 147.62 CPU seconds sampled during 171.33 wall seconds.
`encode` accounts for 26.02 cumulative seconds, `decode` for 23.93, and
`cloneManifest` for 5.04. Gob type decoding alone accounts for 13.55 s.
These inclusive figures overlap and must not be summed as independent shares.
Within `publishMaintenanceLocked`, manifest encoding accounts for 12.74 CPU
seconds. It still runs under `e.mu`, as do the pre/post-publication copies;
only the S3 PUT is unlocked. Queries acquire this same lock for snapshots,
and ingestion needs it for its durable commit.

## Manifest cardinality experiment

Synthetic mature manifests contain raw roots plus eight aggregate-level roots
per metric, active HTA state, and independent object keys/hashes. They omit
held-delta lists. This isolates scaling; it is not a measured production
manifest distribution. Five operations per benchmark, two runs:

| Metrics | Compressed manifest | Encode time | Clone time |
| --- | ---: | ---: | ---: |
| 150 | 0.113 MB | 11.2–11.4 ms | 0.47 ms |
| 1500 | 1.127 MB | 91.9–93.6 ms | 6.1–6.8 ms |
| 15000 | 11.31 MB | 444–504 ms | 60–64 ms |

Encoding allocates approximately 19–20 MB/op at 1500 metrics and 181 MB/op
at 15000. The real S3 fixture above averaged 0.44 MB per manifest, with fewer
populated roots. The manifest does not list every historical block: its major
growth dimension is metrics times levels, not individual historical samples.
Nevertheless every small maintenance publication rewrites all that state.

## Staggered hold expiry

1500 metrics already have durable held records; 120 metrics reach their hold
deadline one second apart. There is no new WAL input during the measurement.
The test advances an injected clock and calls the real NeedsFlush/Flush path.

| Check expiry | Manifest PUTs | Manifest bytes | Local elapsed time |
| --- | ---: | ---: | ---: |
| Every second, as the current ticker | 120 | 7,852,408 | 4.067 s |
| Every 30 seconds, experimental scheduling | 4 | 262,617 | 0.174 s |

Both cases finish with the same 4140 held records; all 120 expired metrics
have one raw block and retain both original samples. The second policy adds
up to 29 seconds to the hold deadline; it is not implemented in production.
This case demonstrates how a workload with staggered ages can make checkpoint
publication frequency dominate even when the data rate is low.

## History locality with full blocks

Same 16,384 samples at 1 Hz, holding and append-only aggregates enabled, one metric. One layout is written
over 16 checkpoints, the other in one checkpoint. The request is FLEX_TIMELINE
over 9000 seconds with interval_max=9 seconds: 9000 stored one-second buckets
are combined into **1000 display positions**. Every response matches exactly.
Shared data/index caches are cleared for each of ten queries per stage.
Storage is in-memory with proper range-only copies; no network latency is
simulated. Two repetitions:

| Layout | Data GETs | Compressed bytes | Median query time |
| --- | ---: | ---: | ---: |
| 16 checkpoints | 9 | 254,915 | 9.1–9.7 ms |
| After 32 CompactOnce calls | 9 | 254,915 | 8.9–9.5 ms |
| One checkpoint | 1 | 254,915 | 6.9–7.2 ms |

One maintenance job removes a candidate in the fragmented layout, then its
candidate count becomes zero. The history query still needs nine data GETs.
`candidateKey` considers dead bytes and data blocks with at most 512 records;
fully live 1024-record blocks are ineligible. `copyJob` combines small blocks
only while the result fits into one 1024-record block. It neither creates
stream extents across full blocks nor repacks arbitrary runs into full blocks
plus a remainder. The one-checkpoint layout demonstrates achievable request
locality, not a currently implemented compaction transformation. Reducing nine
requests to one does not imply nine times faster latency, since reads run in parallel.

## Recommended order

1. **Remove manifest serialization from the ingestion lock.** Capture an
   immutable publication state while holding the necessary publication lock,
   serialize outside `e.mu`, then retain conditional manifest publication and
   generation checks. Avoid cloning Roots merely to obtain Series. This is
   the smallest change to reduce maintenance-induced ACK/query latency spikes.
2. **Separate checkpoint state from maintenance roots.** Put HTA Series state
   and per-stream roots in immutable, paged structures. Keep the CAS manifest
   small: checkpoint sequence/state root, stream-index root, held-delta root,
   catalog roots and maintenance cursors. Compaction should update affected
   stream-root pages without retransmitting every metric's HTA state. The
   atomic root publication remains the durability boundary.
3. **Coalesce maintenance reads.** The query reader already groups physical
   ranges; `copyJob` still issues a GET for each source block. Group the bounded
   input set by object/offset before reading, then restore logical stream order
   and verify each checksum. Reuse the pinned selection's adjacency proof where
   safe, retaining source-identity validation at publication, rather than doing
   another indexNeighbor traversal for each merge partner.
4. **Coalesce hold deadlines.** Batch expiry-triggered checkpoints, with a
   documented small maximum delay. Size/WAL/memory pressure must remain able
   to force an immediate checkpoint. Durable held records need not cause one
   full manifest rewrite per stream deadline.
5. **Make locality an explicit compaction objective.** Select closed windows
   of a canonical metric and level even when their blocks are full and live.
   Pack each window's consecutive blocks contiguously; multiple windows/metrics
   may share one bounded object. Retain independent 1024-record blocks and
   checksums for narrow requests. Windows of roughly 8k–16k level records are a
   starting point to test, not a measured optimum. Coarse levels also need an
   age-based policy for sealing partial windows, since filling them can take
   years. Score expected GET reduction
   against bytes rewritten, and avoid rewriting an open window on every flush.
   Candidate/backlog metrics must distinguish reclaimable bytes, mergeable
   fragments, locality work and unmergeable tails.
6. **Reduce catalog update cost.** Current retirement repeatedly scans and
   splices full ObjectInfo.Blocks slices. Build one removal set per source
   object and filter once; consider paging large inventories independently of
   catalog directory pages. The adaptive byte budget improves progress but
   does not eliminate the underlying metadata work.

A further recovery concern from code inspection: a small surviving held record
can pin an entire held-delta object. Startup reads and decodes each whole delta,
then discards records already covered by watermarks. This can amplify recovery
reads relative to the live held set. It has not been performance-measured in
this review; measure it before adding a delta-compaction policy.

## Reproduction

Raw local results: [capacity-review-c0d3a3d.txt](capacity-review-c0d3a3d.txt).
S3 layout result: [compaction-capacity-c0d3a3d.csv](compaction-capacity-c0d3a3d.csv).

```sh
GOCACHE=/tmp/metricq-go-build-cache go test -tags=review ./engine \
  -run '^(TestReviewFullBlockQueryLocality|TestReviewHoldExpiryPublications)$' \
  -bench '^BenchmarkReviewManifest$' -benchtime=5x -count=2 -v

docker compose -f compose.test.yml up -d
GOCACHE=/tmp/metricq-go-build-cache METRICQ_COMPACTION_METRICS=1500 \
  METRICQ_COMPACTION_OUTPUT=docs/compaction-capacity-c0d3a3d.csv \
  METRICQ_COMPACTION_CPU_PROFILE=/tmp/hta-compaction-capacity.pprof \
  go test -tags=integration ./integration -run '^TestCompactionS3$' \
  -count=1 -v -timeout=15m
go tool pprof -top -cum /tmp/hta-compaction-capacity.pprof
```
