# Bounded inventory upload pipeline

Measured on 2026-09-27 against the local RustFS development S3 endpoint.
Serial reference: isolated archive of `07b770b`; pipeline: `5c72b56`.
[CSV data and sanitized summaries](inventory-upload-pipeline/) accompany this report.

## Change

Catalog pages already upload concurrently. The remaining serial inventory PUTs
now use a four-slot pipeline across changed objects and packs. Preparing and
encoding pages, allocating keys, registering compaction output for crash cleanup,
and updating manifest statistics remain on the caller goroutine. Completed pack
buffers are immutable and transferred to upload workers.

All inventory PUTs must succeed before catalog tree updates begin. The caller
joins all workers on success, upload failure, preparation failure or cancellation.
The first upload error cancels the pipeline and is preserved; no PUT is retried.
Failed preparations/publications retain the existing compaction cleanup protocol.
Manifest CAS and WAL acknowledgement semantics are unchanged.

Pack ownership remains per source object. Unchanged pages are reused, and packs
retire only after their last referenced page is gone. There is no global inventory
reference scan and no time-window partitioning. Query object layout is unchanged.

At most four uploads retain buffers, plus the current preparation buffer. Normal
packs target 4 MiB (roughly 20 MiB of encoded buffers including preparation).
An exceptional page can reach 32 MiB, so this is not a 20 MiB hard memory cap.
Buffer spare capacity, encoding scratch space and decoded inventory memory are
additional.

## Fresh S3 comparison

Both runs use 1,500 metrics, 192,000 samples, eight flushes, holding disabled,
512-block jobs and a 64 MiB/s maintenance budget. The timed region includes
compaction and GC. The pipeline run preceded the serial run; neither timed
region overlapped another throughput benchmark. Each is one trial.

| Measurement | Serial | Pipeline |
| --- | ---: | ---: |
| Compaction + GC | 29.87 s | 30.32 s |
| Catalog phase wall sum | 6.448 s | 4.132 s |
| Catalog phase per publishing job | 50.77 ms | 30.61 ms |
| Publishing jobs | 127 | 135 |
| Catalog PUTs | 1,677 | 1,781 |
| Catalog PUT bytes | 7,507,684 | 11,959,201 |
| Catalog PUT cumulative request time | 9.259 s | 9.547 s |
| Raw blocks before / after | 12,000 / 1,500 | 12,000 / 1,500 |
| Recovery response comparisons | 24 | 24 |
| Successful Prometheus scrapes | 31 | 31 |

Catalog-phase wall time falls by 35.9%, or 39.7% per publishing job. Total wall
time is effectively unchanged (pipeline +1.5%). This run does **not** establish
a higher overall compaction throughput. Job selection and resulting metadata
volume vary across runs because object IDs and map traversal order vary. The
pipeline run writes more catalog bytes and performs more jobs. One pair of
local trials cannot isolate a precise effect size or predict remote-S3 gains.
Both real HTTP Prometheus endpoints were scraped during the measurements with
zero failures.

The final pipeline phase sums include 9.16 s selection, 10.32 s preparation
(including 3.78 s index and 4.13 s catalog) and 15.52 s while publication is
locked. These phases overlap and must not be added. Catalog GC deletes consume
7.91 cumulative request seconds. CPU profiling still shows compression,
decoding, memory clearing/copying and garbage-collection scanning. Upload latency
is now better overlapped; selection/metadata allocation and reclamation remain
relevant limits to total throughput.

## Verification

All package tests, `go vet`, and focused race tests pass. New tests hold uploads
behind a barrier to prove four-way overlap and the publication barrier; they
inject a failure, check cancellation/joining, preserve the original error, check
registered output keys, recover complete inventories and ensure packs do not
share ownership. Existing crash-recovery, conflicting-flush and failed-publication
race tests also pass. The real S3 fixture validates 24 recovered response pairs
in each run. The separate legacy request parity run passes 2,875 response pairs across all
four request types, hot queries, WAL/S3 restart, compaction and S3-outage
backpressure.

No external Go dependencies or additional configuration knobs were introduced.

## Reproduction

```sh
GOCACHE=/tmp/metricq-go-build-cache \
METRICQ_COMPACTION_METRICS=1500 \
METRICQ_COMPACTION_OUTPUT=/tmp/inventory-upload.csv \
METRICQ_COMPACTION_CPU_PROFILE=/tmp/inventory-upload.pprof \
go test -tags integration ./integration -run '^TestCompactionS3$' \
  -v -count=1 -timeout 15m

GOCACHE=/tmp/metricq-go-build-cache \
go test -race ./engine -run \
  'TestCatalogInventoryUploads|TestInventoryUploadCancellation' \
  -count=1 -timeout 2m
```

For the serial reference, archive `07b770b` into a separate directory and run the
identical integration command there, sequentially. `METRICQ_TEST_S3` defaults to
`http://localhost:19000`; test buckets use unique names and are cleaned up.
