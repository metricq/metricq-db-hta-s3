# Compaction review: query layout and deferred ingestion work

Review of `2af234d`, 2026-09-26. Production code was not changed. Reproductions
are in `engine/compaction_review_test.go`, behind the `review` build tag. These
are diagnostic probes, not assertions that the observed shortcomings are desired.

## Findings

### P1: object-first selection can make no progress at moderate cardinality

`engine/compaction.go:110-179` fills the input limit from catalog objects before
checking whether any selected metric has merge partners. With a mixed object
containing at least 128 different metrics, the default budget is consumed by one
block per metric. The subsequent filter removes all those blocks. The cursor then
moves to another object, where the same situation repeats. Increasing the number
of passes cannot fix this layout; there is no retained partial group or per-object
block cursor to gather partners.

Reproduction: 150 metrics, four flushes, one raw point per metric per flush,
aggregate intervals larger than the fixture's time span, `MaxBlocks=128`. Twenty
compaction calls publish no jobs: generation remains 4 and each metric retains
four blocks. All data packs are fully live, so dead-space selection cannot help.

Fix direction: choose a canonical metric/level/time range first, then collect
bounded adjacent index entries. Keep an independent selection path for freeing
partly dead objects. A job budget should be spent on an actionable group, not
consumed before grouping.

### P1: aggregate fragmentation is not independently eligible

`engine/catalog.go:279-294` marks fully live packs as fragmented only for raw
blocks (`Level == 0`). `reserveCompaction` repeats that restriction for clean
packs. The copier can merge aggregate blocks when they happen to be selected
from dirty packs, but there is no reliable selection path for clean aggregate
fragments.

A fully live object containing a two-record aggregate block has an empty candidate
key. Therefore disabling ingestion's aggregate-tail extension would introduce
query fragmentation which the current compactor is not guaranteed to repair.
This directly affects the normal Grafana workload, which reads preaggregated levels.

Fix direction: track fragmentation per metric and HTA level. Select consecutive
aggregate blocks as well as raw blocks. Preserve records and compressed empty
runs exactly; this is block consolidation, not a new aggregation calculation.

### P1: job coordination still performs storage I/O under the ingestion lock

`reserveCompaction` holds `e.mu` at `writeJob` (`compaction.go:191`). Abort and
recovery also read/write job metadata and create trash pages while holding it.
Only the manifest PUT path explicitly releases that mutex. New queries must
acquire the same mutex to capture their snapshot.

A gated `jobs/` PUT reproducibly blocks an ingestion ACK for the entire 100 ms
hold, although the WAL is writable and no flush is active. Thus the existing
blocked-manifest-PUT test proves a narrower property than complete independence
from storage latency.

Fix direction: move all coordination GET/PUT/encoding outside `e.mu`, retaining
source pins, then validate the job/version before publication. Test both ingestion
and new query admission while each maintenance storage operation is blocked.

### P2: a gap discards an otherwise useful consecutive merge prefix

`copyJob`, `compaction.go:301-325`, first groups entries by record budget, then
checks adjacency of the entire group. Any gap resets the group to one entry.
For selected blocks `[0,1,3]`, the consecutive pair `[0,1]` is not merged. The
next attempt `[1,3]` also fails, and all three blocks are merely copied.

Reproduction confirms zero merged-away blocks. Because selection is object based,
this is a practical consequence of selecting blocks from scattered time ranges.
The job may still publish and increment the completion counter without improving
query block count. Select maximal consecutive prefixes and reject jobs that have
neither measurable reclamation nor consolidation benefit.

### P2: the index does not shrink with the number of data blocks

`replaceHistorical`, `compaction.go:432-437`, rewrites the same node type without
combining sparse sibling pages. `prepareCompaction:515` adopts that root without
collapsing a one-child internal root.

Reproduction merges 130 raw blocks to one, but leaves an internal root with one
leaf child: an unnecessary cold index read remains. Large old trees can retain
unnecessary depth and sparse pages after compaction. Root contraction and bounded
sibling-page consolidation are needed for efficient old-history queries.

### P2: pure repacking can lose warm-cache benefits without reducing GET count

Queries issue a range read for each missing block (`query.go:112`); putting several
unchanged blocks in one object does not combine those reads. Both shared caches
are keyed by the full physical `blob` address, including object name and offset.
Copy-only relocation consequently loses cache reuse even when SHA-256 and contents
are unchanged.

Reproduction: a warm raw timeline reads zero blocks before copy-only compaction,
then six blocks immediately afterward, with an identical response. Consider
reusing existing cached decoded records for checksum-identical relocations, and
coalescing nearby byte ranges under a strict over-read budget. For merged blocks,
measure the tradeoff between fewer requests and extra decoding for narrow windows.

### P2: metadata traffic and job scheduling limit deferred ingestion work

Each changed catalog/candidate page is a separate PUT (`catalog.go:84-99`), and
normal flush performs these updates under `e.mu`. In the small tail experiment,
100 flushes generated 1794-1944 total PUTs with current tail extension. This
metadata work can dominate a real S3 deployment even after reducing compression.
Packing metadata pages needs block-level reachability accounting so GC does not
delete an object containing another live page.

The default worker starts at most one bounded compaction per 60-second tick and
selects at most 128 source blocks per job: roughly 2.1 blocks/s of selection
capacity for short jobs, before any failures or ineffective selection. For scale,
a workload that emits 1500 small blocks each minute creates 25 blocks/s for just
one level. Actual flushes are size/pressure driven, not once per minute; this is
a capacity example, not a measurement of the production flush interval.

Before deferring more work, allow bounded consecutive jobs while backlog exists,
sharing a sustained I/O/CPU budget and pausing for WAL pressure. Expose pending
fragment counts/bytes, oldest candidate age, input-to-output block reduction and
no-progress jobs. Existing completed-job and dead-byte counters do not measure
query improvement or whether maintenance keeps up.

## A/B experiment: append aggregate fragments instead of extending tails

100 flushes, ten samples each, one metric, three aggregate levels, local durable
WAL and in-memory object store with byte-range reads. Production background
catalog maintenance is enabled, but no maintenance worker runs during timing.
Three runs per variant; current variant first, experimental variant afterward.
The experimental Go overlay only disables the `level > 0` tail-read/extend branch
in `engine.go:508`. It does not change production code or the WAL/ACK path.

| Observation | Current tail extension | Append-only experiment |
|---|---:|---:|
| Median total flush time | 862 ms | 481 ms |
| Observed flush-time range | 858-927 ms | 342-609 ms |
| Data bytes written | 342320 | 79704 |
| Index bytes written | about 326000 | about 788000 |
| Total PUT count | 1794-1944 | 1074-1182 |
| Blocks at queried aggregate level | 1 | 100 |
| Cold FLEX query block reads, including index | 2 | 103 |
| Compressed query bytes | about 5362 | about 41781 |
| Median cold query time | 1.35 ms | 7.15 ms |

All six response encodings have the same SHA-256. The experiment shows potential
for shorter flush stalls and less data rewriting, together with more index
writing and much worse query fragmentation before compaction. It does not measure
S3 latency, steady-state compaction cost, overall ingestion throughput, or a
Prometheus HTTP endpoint. Timing variability and the small fixture preclude a
production speedup claim. The read-count change is the clearer result.

Raw measurements: [compaction-review-results.csv](compaction-review-results.csv).

## Recommended implementation order

1. Fix stream/time-based selection for every HTA level, gap handling, no-op jobs,
   and index contraction. Add convergence checks at 1500 metrics with standard job
   limits, asserting block counts and cold query reads, not only stored bytes.
2. Remove maintenance coordination I/O from the ingestion mutex, preserve warm
   cache entries where bytes are unchanged, and control metadata PUT amplification.
3. Add a configurable append-only flush mode which writes only newly completed
   records; preserve WAL fsync-before-ACK and checkpoint-before-WAL-reclamation.
   Keep aggregation itself incremental during ingestion. Publish fragments in the
   normal query index so current data remains queryable before compaction.
4. Bound fragment backlog by stream and age. Run multiple bounded jobs under one
   global maintenance budget when necessary; degrade predictably under S3 outage
   or sustained overload. Do not repeatedly merge the newest growing tail after
   every flush, which would recreate the same write amplification in the worker.
5. Measure the joint workload: ingest rate and ACK latency, flush time, S3 GET/PUT
   count and bytes, compaction debt, and cold/warm query p50/p95/p99 across time
   spans and resolutions. Include narrow raw queries, ordinary aggregate timelines,
   concurrent ingestion, restart, and temporarily unavailable compaction.

Run the diagnostic probes with:

```sh
go test -race -tags=review ./engine -run '^TestReview(CardinalitySelection|AggregateCandidate|NonadjacentBatch|IndexRootCollapse|JobPutBlocksIngest|CopyOnlyCache)$' -count=1 -v
go test -tags=review ./engine -run '^TestReviewTailCost$' -count=3 -v
```

For the append-only comparison, copy `engine/engine.go` to a temporary file, change
only the tail-extension `if level > 0` to `if false && level > 0`, and map that file
with Go's `-overlay` option for the second command. Keep it outside the production
checkout. The probe logs response checksums, block reads, bytes and timings.
