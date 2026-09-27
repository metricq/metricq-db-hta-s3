# Compaction throughput after shared caches and paged inventories

Measured on 2026-09-27 against the local RustFS S3 development endpoint.
Baseline: `8cc1f37`; implementation: `71231a5`, with subsequent bootstrap
page-bound validation and documentation fixes. Raw summaries and CSVs are in
[`compaction-throughput/`](compaction-throughput/).

## Workload and results

The existing S3 fixture generates 1,500 canonical metrics with 128 samples per
metric (192,000 samples), spread across eight flushes. Holding is disabled;
raw streams initially have eight fragments each. Maintenance uses 512 blocks
per job and a sustained 64 MiB/s budget. Timed work includes compaction and GC,
not ingestion. All runs converge from 12,000 to 1,500 raw blocks and compare
24 history responses after S3 recovery.

| Measurement | Baseline | Shared-cache intermediate | Final |
| --- | ---: | ---: | ---: |
| Compaction + GC wall time | 101.20 s | 37.80 s | 28.79 s |
| Catalog range reads | 2,410 | 35 | 71 |
| Catalog range-read bytes | 130,227,550 | 2,823,540 | 2,044,358 |
| Index range reads | 28,853 | 2,101 | 2,431 |
| Index range-read bytes | 22,457,348 | 48,023,636 | 45,822,103 |
| Catalog PUTs | 994 | 982 | 1,663 |
| Catalog PUT bytes | 68,352,652 | 60,556,213 | 8,968,321 |
| Bounded convergence passes | 121 | 116 | 117 |

The final run takes 71.6% less wall time, equivalent to approximately 3.5 times
the throughput for this fixture. Coalesced index prefetch reduces requests but
reads approximately twice as many index bytes. Paged inventories reduce catalog
write bytes by 86.9%, while producing more small objects, PUTs and GC deletes.
This is a latency/CPU improvement with an explicit request-count tradeoff.

A real HTTP Prometheus endpoint was scraped once per second during the
intermediate and final timed runs: 38 and 29 successful scrapes, respectively,
with zero failures. The baseline did not run this extra scrape fixture.

## Implementation

- A bounded 32 MiB shared decoded catalog cache reuses immutable pages between
  selection and publication; freshly written pages populate the cache.
- Bounded coalesced index prefetch replaces many individual GETs and reuses
  decoded output nodes when publishing.
- Large object inventories use pages of at most 128 block descriptors. Physical
  offset bounds make deletion local to changed pages. Inventory packs belong to
  one data/index object, so reclamation does not need a global reference scan.
- Locality jobs batch multiple contiguous metric/level sections within their
  existing shared limits. Placement optimizes adjacent records within a level;
  it does not partition data into time windows.
- Up to eight workers decode and encode merge groups concurrently, with ordered
  output assembly and job-bounded buffers.
- Large growing tails defer repeated merging until sufficient new records
  accumulate, a prefix is complete, or an age limit is reached.
- Optional `compaction_continuous` wakes maintenance after checkpoints and
  continues bounded cycles while work remains. It defaults to false and retains
  pacing, WAL pressure checks and cancellation.
- Phase, cache and deferred-merge metrics expose where maintenance spends time.

The growth policy and continuous scheduling were added after the final timed
run. They are inactive in this fixture (zero cooldown and continuous=false),
and have functional tests rather than a separate throughput claim.

## Query locality and validation

The cold FLEX locality fixture uses 16,384 samples and returns 1,000 rows for a
9,000-second range. With 16 flushes, maintenance reduces data GETs from nine to
one, reading the same 254,915 bytes. Its in-memory median changes from 10.49 ms
to 8.08 ms; these timings do not estimate remote S3 latency. Three compaction
jobs complete. The single-flush control already needs one GET.

`go test ./...`, `go vet ./...` and focused race checks pass. Tests cover shared
cache limits, checksum failure, coalesced reads, partial-bootstrap inventory
growth, page reuse and last-reference retirement, multi-stream locality, tail
merge policy, continuous scheduling, publication conflicts and crash recovery.
The legacy integration comparison passes 2,875 response pairs across all four
request types, hot queries, WAL/S3 restart, compaction and S3-outage backpressure.

WAL acknowledgement and the single manifest CAS publication remain the
durability boundary. New immutable inventory objects are written before their
references are published; superseded packs retire through the deletion journal.
No external Go dependencies were added.

## Limits and next bottleneck

These are individual local development runs, not statistically isolated repeated
trials. Some validation ran concurrently. The fixture deliberately stresses
fragmentation over a short history; it does not establish ten-year capacity or
steady-state capacity with concurrent ingestion and dashboard traffic. It also
does not replace the dissertation-style query-duration/resolution sweep.

Final phase sums include 8.08 s selection, 3.49 s copy/merge, 11.01 s preparation
(including 3.06 s index and 5.91 s catalog), and 15.27 s while publication is
locked. Phases overlap and must not be added. CPU profiling still shows material
encoding/decoding and Go garbage-collection costs. Catalog request overhead now
matters more than catalog byte volume: 1,663 PUTs consume 8.62 cumulative request
seconds, with another 7.80 cumulative seconds in catalog deletes.

The next useful experiment is bounded parallel upload of independent inventory
packs, keeping ownership per object and waiting for all uploads before CAS.
This can overlap S3 latency without introducing a global pack reference scan.
Then measure a compact binary metadata codec and allocation reductions. More
concurrent compaction coordinators would contend at publication and require
stronger conflict handling; the current implementation parallelizes work inside
a single coordinator instead.

## Reproduction

With the development S3 endpoint running and its test credentials configured:

```sh
GOCACHE=/tmp/metricq-go-build-cache \
METRICQ_COMPACTION_METRICS=1500 \
METRICQ_COMPACTION_OUTPUT=/tmp/compaction-throughput.csv \
METRICQ_COMPACTION_CPU_PROFILE=/tmp/compaction-throughput.pprof \
go test -tags integration ./integration -run '^TestCompactionS3$' \
  -v -count=1 -timeout 15m

GOCACHE=/tmp/metricq-go-build-cache \
go test -tags review ./engine -run '^TestReviewFullBlockQueryLocality$' \
  -v -count=1

GOCACHE=/tmp/metricq-go-build-cache \
go test -tags integration ./integration -run '^TestLegacyRequestParity$' \
  -v -count=1 -timeout 5m
```

`METRICQ_TEST_S3` defaults to `http://localhost:19000`. The S3 fixture creates
unique test buckets and cleans up its objects. For the baseline, run the same
compaction fixture in an isolated checkout of `8cc1f37`; its original fixture
does not contain the new phase/cache metrics or HTTP scrape helper.
