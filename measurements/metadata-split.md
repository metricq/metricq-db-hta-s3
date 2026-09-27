# Independent held metadata: implementation and measured tradeoffs

2026-09-27. Baseline: `3105957`; new storage implementation: `50d189a`.
The sustained fixture was added in `beb1497` and copied unchanged to an archive
of the baseline. Raw output is in [metadata-split/](metadata-split/).

## Workload and limits

Each run advances two simulated hours in 30-second batches and performs 240
actual checkpoints. WAL serialization, writes and fsyncs, compression, held
payloads, index/catalog updates, compaction and GC execute normally. The backend
is a checksummed range-capable in-memory object store **without network
latency**. These numbers measure engine work and byte/request amplification,
not sustained S3 or AMQP capacity. Timing runs overlapped other validation work
on the same machine; byte comparisons are more reliable than timing differences.

Both runs use 1500 canonical metric names, factor 10, minimum interval 1 s and
maximum 10^7 s; one-hour holding, 256 MiB held budget, 512 MiB ingest memory,
32/256/512 MiB WAL limits, append-only aggregates. Checkpoints are explicitly
triggered every 30 seconds rather than relying on the application's wakeup
loop. Compaction and reclamation each run once every ten simulated minutes,
with the test maintenance options (128 blocks/job, 64 KiB output target,
cooldown zero, I/O throttling effectively disabled). This is not a compaction
convergence/capacity proof. Final candidates: 20 for mixed, 0 for dense.

- **Dense:** 1500 metrics at 1 Hz, aligned arrivals: **10,800,000 points**.
- **Mixed:** 100 at 10 Hz, 900 at 1 Hz, 300 at 0.1 Hz, 150 at 1/min,
  40 at 1/hour and 10 at 1/day. Initial arrivals stagger across 30 seconds:
  **13,887,240 points**. Daily streams pin old held payloads. Their stream age
  still expires: they do not remain pinned forever.

A real Prometheus HTTP endpoint serves throughout ingestion and is scraped
once per wall-clock second plus a final scrape. Baseline/new scrapes: 93/87
mixed and 76/72 dense; all successful. The engine is then reopened with an empty
WAL directory against the same object store. Twelve FLEX history answers
(four metrics, three resolutions) must be protobuf-identical to their answers
before shutdown, including raw samples and the sparse daily metric.

## Written bytes over two simulated hours

Decimal MB; directories/root descriptors included. Main payload byte counts
are unchanged between baseline and new implementation.

| Kind | Mixed baseline | Mixed new | Dense baseline | Dense new |
| --- | ---: | ---: | ---: | ---: |
| `manifest` | 0.582 | 0.564 | 0.499 | 0.500 |
| `state/` | 93.647 | 93.645 | 89.845 | 89.865 |
| `roots/` | 13.836 | 13.831 | 7.105 | 7.099 |
| `held-state/` | 8.314 | 2.707 | 7.870 | 2.292 |
| These four metadata kinds | 116.379 | 110.746 | 105.319 | 99.756 |
| All PUT payloads, including compaction/catalog/trash | 887.910 | 883.283 | 936.342 | 930.698 |

Held metadata write volume falls **67.4% mixed / 70.9% dense**. Its PUT count
stays **240 in every case**, because changed pages and the held root share one
pack per checkpoint. Across the four metadata kinds the reduction is only
**4.8% / 5.3%**; across all written bytes roughly **0.5% / 0.6%**. This does not
remove the dominant Series or held-payload cost. Catalog request counts vary
with maintenance ordering and random object keys; no deterministic reduction
in total request count is claimed.

The earlier 90 MB/hour monolithic-manifest number is not today's CAS manifest:
the current CAS object is about 1–2 KB and writes about 0.25–0.29 MB/hour in
this fixture. Most active metadata bytes belong to `state/`.

## Recovery and retained bytes

| Cost | Mixed baseline | Mixed new | Dense baseline | Dense new |
| --- | ---: | ---: | ---: | ---: |
| S3-only startup reads | 135 | 157 | 128 | 140 |
| Startup bytes, MB | 159.609 | 159.812 | 180.255 | 180.395 |
| Startup wall time, seconds | 5.284 | 5.798 | 6.113 | 5.827 |
| Physically retained held metadata, MB | 0.065 | 0.387 | 0.061 | 0.338 |
| Total physically retained objects, MB | 561.017 | 561.350 | 628.456 | 628.741 |

The additional indirection adds 22/12 startup reads and about 0.20/0.14 MB.
Sibling pages in a pack are coalesced. Most startup bytes still come from held
payload objects, including records already written whose shared delta remains
needed by a cold stream. This change does not compact held payloads.

Partially live metadata packs retain old pages while another page in either
collection still needs the object. Retained metadata grows by about 0.3 MB in
these runs. GC frees an object after the final reference disappears; automatic
metadata repacking has not been added. Longer histories should monitor this
retention separately from the data/index catalog's live-byte metrics.

Batch ACK p50/p95, mixed: 122/161 ms baseline, 124/164 ms new; dense:
136/186 ms baseline, 125/164 ms new. Checkpoint p50/p95, mixed: 160/555 ms
versus 160/366 ms; dense: 140/441 ms versus 134/381 ms. Each ACK timing is the
whole batch's processing/fsync, not a single-sample latency. Shared-machine
noise and zero network latency prevent treating these differences as a
production latency guarantee.

## Optional Series patch journal decision

`TestReviewSeriesPatchOpportunity` evaluates absolute Last/changed-level
replacements after one hour at 1 Hz, with a 30-second update on all 1500 metrics.
It verifies exact reconstruction without re-running aggregates. Paged snapshots
encode about 372 KB; candidate patches about 280 KB: **24.6–24.7% codec savings**.
6000 of 7500 level states change. The estimate excludes periodic materialized
bases, journal roots, object retention and startup reads.

The optional journal is **deferred**. Its codec-only improvement does not yet
establish enough end-to-end benefit to justify another persistent dependency
chain and recovery/GC machinery. The current implementation keeps one bounded
snapshot reference for Series state. A journal should be evaluated separately
with bounded bases, dense/sparse fallback and real S3 startup/request costs;
this change does not claim to solve the `state/` write volume.

## Validation

- `go test ./...` and `go vet ./...` pass.
- Targeted race checks for held trees, metadata failures, dirty-set restoration
  and failed publication pass.
- 10,000 descriptors: an append writes **3 inventory pages, 0 watermark pages**.
  Recovery, deletion/root collapse, legacy migration, unknown versions, missing
  and corrupt metadata are covered.
- Shared-pack GC is tested across both collections, including final deletion
  and an age-only publication with unchanged WAL sequence.
- Lost manifest replies are reconciled by exact content; cancelled/failed
  publications retain acknowledged WAL. Concurrent samples/configuration changes
  survive a failed frozen checkpoint and a subsequent S3-only restart.
- `TestHeldMetadataS3Recovery` passes on the local S3 service, using actual
  range reads, CAS and GC with held payloads and multi-page watermarks; all four
  history request types remain identical after recovery.
- `TestLegacyRequestParity` passes: **2875 old-file/new-Go response pairs**,
  including hot queries, WAL restart, S3 restart, compaction, compacted restart
  and S3 outage/backpressure. This general parity fixture has holding disabled;
  held recovery has the separate coverage above.
- The existing cold FLEX locality check still goes from 9 data GETs to **1**
  after level-local compaction, with identical responses. Metadata hydration
  introduces no additional history-path reads.

Normal failed checkpoint output can still become unreachable before CAS and
is not globally orphan-scanned. This existing limitation is not repaired by
paging metadata. The deployment remains single-writer per store namespace.

## Reproduction

From the repository root, with the existing dependencies and local test services:

```sh
METRICQ_METADATA_REVIEW=1 METRICQ_METADATA_CASE=mixed \
METRICQ_METADATA_MAINTENANCE=1 GOCACHE=/tmp/metricq-go-build-cache \
go test -tags review ./engine -run '^TestReviewMetadataHourlyWorkload$' \
  -v -count=1 -timeout 25m
```

Replace `mixed` with `dense`; set `METRICQ_METADATA_HOURS` to increase duration.
Omit `METRICQ_METADATA_MAINTENANCE=1` for isolated checkpoint attribution.
For the baseline, archive `3105957` into a separate directory and copy
`engine/metadata_workload_review_test.go` from `beb1497` into that archive before
running the identical command. The fixture needs permission to listen locally
for its Prometheus endpoint.

```sh
go test -tags review ./engine -run \
  'TestReviewSeriesPatchOpportunity|TestReviewFullBlockQueryLocality' -v -count=1
go test -tags integration ./integration -run \
  'TestHeldMetadataS3Recovery|TestLegacyRequestParity' -v -count=1 -timeout 5m
```
