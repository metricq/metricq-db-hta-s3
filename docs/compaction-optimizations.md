# Compaction and append-only flush improvements

The original review findings are covered by production changes and regression
tests in `engine/compaction_optimization_test.go`.

- Selection gathers consecutive records from one canonical metric and HTA level
  through its time index. It can expand before as well as after a seed block;
  input count and byte limits remain bounded. A resumable cursor within each candidate object and bounded
  candidate scans prevent unrelated single blocks consuming an entire job budget.
  Cached roots known to contain only one block do not consume the stream-search
  budget. A later flush changes the immutable root, making that stream eligible
  for examination again; this shortcut uses the existing bounded index cache.
  The probe budget scales with the input-block limit (64–512 streams), with an
  independent index-operation stop, rather than restricting 512-block jobs to
  64 stream probes.
- Fully live fragmented aggregate packs qualify alongside raw packs. The copier
  merges maximal consecutive prefixes, preserving records and empty runs exactly.
- Redundant internal roots collapse and bounded sparse sibling leaves combine.
  Only reachable output pages enter the live catalog, including pages produced
  while contracting the index. Old query generations remain protected from GC.
- Job registration, abort and recovery metadata GET/PUT run outside the ingestion
  mutex. Once copying finishes, the bounded final metadata rewrite and publication
  serialize with flushes against the newest roots. A busy publisher no longer
  discards the copied outputs. Exact source identity validation remains mandatory.
- Shared immutable caches use content hashes, so byte-identical relocation keeps
  cached records/pages and does not consume a second cache entry.
- Catalog fanout increases from 16 to 64, with a 256 KiB estimated-descriptor leaf
  target so large mixed-pack inventories do not share a page with dozens of
  unrelated objects. Individually larger inventories get a dedicated leaf.
  Separate immutable metadata pages upload
  with at most eight requests in flight and a 4 MiB queued-byte target, reducing
  serial S3 latency while retaining safe whole-object deletion semantics.
- Consecutive jobs share one read/write byte limiter, including preparation
  metadata, rebuilt indexes and manifest publication. The bounded final metadata
  rewrite is charged after releasing the publication mutex; source reads prefetch at
  most eight blocks. GC runs between jobs once a full batch is pending, and new jobs stop at the configured
  cycle-start window or on no progress, WAL pressure or error.
- Compaction preserves logical ingestion timestamps. Repacking part of a large
  object does not postpone every remaining stream by another cooldown period.
- Dirty mixed packs larger than a job are evacuated over consecutive bounded jobs.
  The source stays live until the last referenced block moves. Without this,
  consolidation can leave old packs permanently stranded alongside their copies.
  Merge selection runs before copy-only evacuation, so a low dead-byte threshold
  cannot repeatedly relocate unmerged fragments instead of improving queries.
- Queries coalesce cache misses by object and physical offset, with a 64 KiB gap
  limit and an 8 MiB range target. At most eight requests run concurrently, in
  batches of at most 128 block references. Per-block SHA-256 checks and logical
  result order are preserved. Blocks larger than 8 MiB remain individual reads.
- Aggregate queries skip neighboring blocks, while FLEX still includes the bucket
  containing an unaligned start. Raw queries keep boundary neighbors.
- A committed flush keeps the decoded rightmost index path it wrote for each
  stream, bounded to 2^19 entries. The next flush appends without re-reading
  those pages under the ingestion mutex. With 1500 metrics and five levels, an
  append-only flush previously issued 4500 sequential index GETs; it now issues
  none, and in-memory flush time no longer grows with rightmost-leaf size
  (0.7-1.1 s instead of 0.9-5.3 s over six flushes). Compaction-replaced roots miss
  and are read once. Selection also treats pinned one-entry roots as known
  singletons, independent of shared-cache warmth. `TestFlushReusesPinnedIndexTails`
  asserts zero index reads across leaf splits, and correctness after compaction
  and restart.
- Reclamation publishes one manifest per batch of up to 256 deletions across up
  to 64 journal pages, instead of one per 16 deletions plus one per finished page.
  Finished pages are deleted by the next batch without a publication of their own,
  and the background worker waits for a full batch or ten seconds. In the
  1500-metric convergence fixture (20 append-only flushes, 512-block jobs, run to
  one block per stream), maintenance manifest PUTs fell from 1570 (559 MB) to 570
  (203 MB); the remaining ones are mostly job registration and publication.

The executable defaults to `append_only_aggregates=true`, with 512 source blocks
per job and a 30-second window for starting consecutive jobs every 60 seconds.
Individual jobs retain their own 60-second timeout. The active job can extend the
cycle beyond the start window. The library defaults are 128 blocks and a ten-second
start window; append-only mode must be enabled explicitly there.

Flush now writes newly completed aggregate records rather than reading and
recompressing an old growing tail. Each fragment is immediately reachable through
the normal time index. WAL fsync-before-ACK, conditional publication and checkpoint
before WAL reclamation are unchanged. Disabling compaction or merging in the
executable restores tail extension; incompatible library options are rejected.

## Evidence

The 1500-metric regression uses four flushes and the smaller 128-block job limit.
Every metric converges from four raw blocks to one and retains all four samples;
the regression allows at most 240 bounded calls. This asserts convergence rather
than only reduced stored bytes. Separate tests cover aggregate fragments, S3-only
restart, index contraction, cache retention, blocked coordination PUTs, byte-budget
cancellation and multiple jobs within one scheduling interval. The mixed-stream
regression uses 150 metrics, eight flushes and four stored levels; the S3 fixture
covers the same shape with 1500 metrics. A separate copy-only test evacuates a dirty 30-metric object
with eight-block jobs and verifies physical deletion after its last block moves.

The query regression compares an in-memory answer with a cold stored FLEX answer
covering 23 contiguous aggregate blocks: one data GET. A narrow query inside the
last bucket of a block reads exactly that block's compressed length. Additional
tests cover reordered references, gaps and checksum failures. Publication gates
exercise both flush-first and compaction-first ordering; prepared jobs publish and
WAL ingestion remains available during the compactor's metadata upload.

The repeated one-metric experiment uses 100 flushes of ten samples, local durable
WAL and an in-memory object store supporting byte-range reads. Three runs per
mode, with no maintenance worker during flush timing. The append-only mode then
runs manual bounded compaction and queries a cold cache again.

| Observation | Tail extension | Append-only |
|---|---:|---:|
| Data written during flush | 342320 B | 79704 B |
| Index written during flush | about 326000 B | about 787000 B |
| Total flush PUT count | 822-825 | 767-771 |
| Median summed flush time | 638 ms | 441 ms |
| Observed summed flush range | 506-658 ms | 326-665 ms |
| Aggregate blocks before maintenance | 1 | 100 |
| Cold query reads before maintenance | 2 | 103 |
| Aggregate blocks after consolidation | 1 | 1 |
| Cold query reads after consolidation | 2 | 2 |

All six query response hashes match, and the append-only probes assert unchanged
responses after consolidation. Data writes fall by about 4.3 times, while index
writing increases. Timing ranges overlap substantially: this small CPU/store
experiment does not establish a production ingestion speedup. It does demonstrate
reduced data rewriting and recovery of the original query block-read count.

[Raw experiment data](compaction-optimized-results.csv) excludes later compaction
writes from flush counters; maintenance has additional cost. The experiment does
not run or scrape the Prometheus HTTP endpoint.

The real-S3 fixture uses 128 samples per metric delivered in eight flushes.
It now runs until every stream has one block (up to 512 bounded jobs), then drains
GC and asserts one raw block per metric, all raw records and lower physical storage.
Mixed source packs can temporarily increase physical space during partial
consolidation, so quota headroom is necessary; source packs remain until their
last live block moves. A failed upload preserves existing roots and durable WAL.
[Storage results](compaction-optimized-s3.csv) cover complete fixture convergence and
query equivalence after an S3-only restart. These are functional storage checks,
not stable latency measurements. The final S3 run was separate from the full race
suite. It converged in 1, 11 and 108 passes for 6, 150 and 1500 metrics respectively.
At 1500 metrics, data/index bytes fell from 21,852,165 to 6,740,213, raw block count
from 12,000 to 1500, and the index still accounts for all 192,000 raw records.
Compaction plus GC took 162.96 seconds in that fixture; ingestion, baseline GC,
layout auditing and restart checks are outside that interval. Each fixture checks
24 response pairs after an S3-only restart. Legacy
parity checks all four request types before/after compaction and after restart.
The executable Prometheus endpoint and orderly shutdown are checked separately.

## Operational limits

The byte limiter controls throughput, not a hard per-goroutine CPU or heap cap.
Normal ingestion/flush, queries and the worker still share CPU, memory and S3.
Fragment backlog can grow under sustained overload: the fixed limits are not an
unconditional guarantee of immediate optimal layout. Watch small-block bytes,
candidate count, completed jobs, input/replacement block totals, WAL pressure and
query latency together. Candidate and small-block gauges include single unmergeable
stream tails and cooldown candidates; they are not an exact actionable-job count.
Their persisted accounting starts with namespaces created by this implementation;
older catalogs report these three new gauges as unavailable (`NaN`), and
`MaintenanceStatus` includes an availability flag. Existing live/dead byte accounting
remains available.

Metadata pages remain separate objects. Time-window sealing and an actionable
stream/window backlog queue are not implemented: tails can still be remerged as
new fragments arrive. Ordinary flush still holds the ingestion mutex during S3
I/O; moving it outside requires a separate durable WAL checkpoint boundary.
Arbitrary uploads orphaned by an unsuccessful ordinary
flush before publication still require separate inventory/reachability cleanup.
None of these objects are deleted speculatively by compaction.

Reproduce the small comparison with:

```sh
go test -tags=review ./engine -run '^TestReviewTailCost$' -count=3 -v
METRICQ_REVIEW_APPEND_ONLY=1 go test -tags=review ./engine -run '^TestReviewTailCost$' -count=3 -v
```

Run the final storage fixture and race suite separately:

```sh
docker compose -f compose.test.yml up -d
METRICQ_COMPACTION_METRICS=6,150,1500 go test -tags=integration ./integration -run '^TestCompactionS3$' -count=1 -v -timeout=15m
go test -race ./... -count=1 -timeout=10m
```

With the MetricQ development stack running, legacy and executable checks use:

```sh
go test -tags=integration ./integration -run '^(TestLegacyRequestParity|TestExecutablePrometheusAndShutdown)$' -count=1 -timeout=8m
```
