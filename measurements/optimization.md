# Batched checkpoints and aggregate tail consolidation

The optimization changes four parts of ingestion and checkpointing:

- Ingest prepares the HTA state and emitted records once, then publishes that
  prepared state only after the WAL frame has been written and synced.
  Recovery still reconstructs state independently from WAL samples.
- Independent gzip streams reuse compressor scratch memory. The first
  measurements retained default compression. A subsequent change selects
  `gzip.BestSpeed`; gob and gzip decoding remain unchanged.
- Index updates copy the rightmost tree path once per metric/level batch.
  Only final reachable pages enter the index pack.
- A partially filled aggregate data block is combined with the next
  checkpoint's records, up to 1,024 physical records. The manifest switches
  to the replacement only after the new data and index packs are stored.

The last change prevents checkpoint frequency from fragmenting higher HTA
levels into many tiny blocks. It adds at most one data-block read per changed
aggregate stream and rewrites its partial tail. Old blocks stay readable by
existing snapshots. Superseded tails remain in their original packs until a
future garbage collector can reclaim them; this is a write/space tradeoff.
Raw data is retained without tail rewriting. Existing fragmented history is
not repacked by this change.

## Local engine measurements

The [before](optimization-engine-before.txt) and
[after](optimization-engine-after.txt) measurements each contain three runs
with `-benchtime=2s` on an AMD Ryzen 7 PRO 4750U. The baseline test binary was
saved before changing the engine, and both binaries ran sequentially. Values
below are medians of the three runs.

| Measurement | Before | After | Change |
| --- | ---: | ---: | ---: |
| Ingest, 500-point deliveries with WAL sync | 479,733 points/s | 615,969 points/s | +28.4% |
| Checkpoint, 100,000 points | 793.7 ms | 535.3 ms | −32.6% |
| Checkpoint allocated bytes | 529.4 MB | 93.6 MB | −82.3% |
| Checkpoint allocations | 105,844 | 9,692 | −90.8% |
| Index pack bytes per checkpoint | 402,679 | 15,680 | −96.1% |
| Data pack bytes per checkpoint | 4,348,976 | 4,348,976 | unchanged |

These benchmarks use a real local WAL and an in-memory object store.
Both measurements in this table used default gzip compression; they do not
measure the subsequent BestSpeed change.
`BenchmarkIngest` excludes checkpoints from its timer and does not include
RabbitMQ or S3 network traffic. `BenchmarkCheckpoint` measures a first
checkpoint and therefore does not include aggregate tail reads from an
earlier checkpoint. These values are not end-to-end ingestion rates.

The source is [engine/performance_test.go](../engine/performance_test.go).
To measure the current implementation:

```sh
go test ./engine -run '^$' -bench 'Benchmark(Ingest|Checkpoint)$' \
  -benchtime=2s -count=3 -benchmem
```

## BestSpeed follow-up

The [BestSpeed results](optimization-engine-bestspeed.txt) use the same
benchmarks after the large S3 run finished. Compared with the preceding
optimized/default-compression engine:

| Measurement | Default gzip | BestSpeed gzip |
| --- | ---: | ---: |
| Median checkpoint, 100,000 points | 535.3 ms | 285.2 ms |
| Data pack bytes | 4,348,976 | 4,498,873 |
| Median index pack bytes | 15,680 | 16,048 |
| Median ingest, excluding checkpoints | 615,969 points/s | 773,620 points/s |

Checkpoint time falls by 46.7%, while data-pack size grows by 3.45%.
BestSpeed ingestion rates varied substantially (436,435, 773,620 and 902,939
points/s), so the median ingestion increase should not be treated as a
precise throughput prediction. Checkpoint times were 305.0, 283.0 and
285.2 ms. The 132-million-point S3 result below was obtained with default
gzip; it does not measure the additional end-to-end effect of BestSpeed.

BestSpeed applies to WAL frames, data blocks, index pages and the manifest.
Gzip streams carry the information needed by the reader; no manifest version
change or migration is needed. The change reduces compression CPU cost but
does not remove tail rewrite amplification or reclaim superseded objects.

## Correctness and recovery checks

The complete Go/S3 module and `metricq-go` pass their race-enabled suites.
The local S3/RabbitMQ integration test passes 1,725 paired history responses
against the legacy file DB across ingestion, WAL replay and S3-only recovery,
plus the storage-outage/backpressure recovery comparison.
After enabling BestSpeed, the full module race suite and the legacy parity /
outage-recovery integration test passed again. Test queues were removed
(17 normal RabbitMQ queues remained; no `hta-latency-*` or `hta-go-test-*`
queues), and the separate S3 test container was stopped and removed.

New regressions check index splits across several tree levels, reachability
of all newly encoded index pages, immutable old roots, and full aggregate
blocks across 35 checkpoints for both dense and sparse metrics. Query
responses are compared before checkpointing, after checkpointing and after
S3-only recovery. Injected data/index read failures and data/index/manifest
write failures must leave the previous root, committed sequence and WAL
unchanged. Additional checks ensure that rejected ingestion preparation and
failed WAL writes never publish the prepared HTA state.

The compression compatibility test constructs a checkpoint whose data,
index and manifest use the previous default compression, appends a WAL frame
with that same compressor, and recovers through the current reader. It then
extends the old aggregate tails using BestSpeed and verifies raw, aggregate
timeline and aggregate responses after S3-only recovery.

The backpressure integration workload now exceeds the AMQP prefetch of 400.
AMQP queue inspection counts ready messages only, so a smaller workload
cannot establish whether unacknowledged prefetched deliveries were retained.

## Large end-to-end comparison

The completed follow-up repeats the 132-million-point workload from
[latency-scale.md](latency-scale.md): six metrics with 22 million points at
2 Hz, 50 query durations from 0.1 to 10 million seconds, 20 paired repetitions,
and the same object/WAL targets and AMQP prefetch. The output files are
`latency-scale-optimized.csv` and `ingest-scale-optimized.csv`.

This run was compiled before the BestSpeed change and therefore still uses
default gzip compression. It also includes the shared 128 MiB decoded-data
cache and bounded parallel block reads already present in the workspace at
the time of compilation. Its results measure the combined implementation,
not an isolated effect of tail consolidation or index batching. The cache
persists between requests, so the workload measures a mixture of cache misses
and hits; it is not a cold-cache benchmark. The new CSV separately counts
data and index Range GETs. A zero GET count can result from cache hits.

All **21,000 response pairs matched**, across 300 configurations per backend
(600 CSV rows). The run completed in 1,747 seconds. The six-metric median
Go/S3 visibility rate rose from 60,049 to **92,147 points/s** (+53.5%). The
six values range from 90,060 to 93,935 points/s. Median publisher/file
visibility rates in this run were approximately 605,159/604,431 points/s;
the file DB remained publisher-limited. The runs happened at different times
on the same development setup; they are not controlled storage-only tests.

Selected means at a requested duration of **10,000,000 seconds**:

| Request | Metrics | S3 before | S3 after | File in new run | S3 GETs before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Timeline, 100 positions | 1 | 122.010 ms | 2.263 ms | 2.097 ms | 43.4 → 0.0 |
| Timeline, 1,000 positions | 1 | 130.362 ms | 4.202 ms | 3.530 ms | 43.6 → 0.0 |
| Timeline, 1,000 positions | 6 | 136.189 ms | 6.652 ms | 15.978 ms | 261.8 → 0.0 |
| Aggregate | 1 | 36.773 ms | 18.746 ms | 38.782 ms | 42.1 → 14.4 |
| Aggregate | 6 | 131.250 ms | 51.397 ms | 22.817 ms | 249.2 → 72.3 |

The long timeline requests in this workload are served entirely from the
decoded-data cache after earlier requests populated it. The zero GETs are
measured cache hits, not evidence that arbitrary cold S3 history can be read
without GETs. Aggregate boundary work still reads data at finer levels.

Artifacts: [latency CSV](latency-scale-optimized.csv),
[ingestion CSV](ingest-scale-optimized.csv),
[file/S3 plot](latency-scale-optimized.svg),
[before/after plot](latency-scale-optimization.svg).

The benchmark starts the Engine and MetricQ adapter directly. It records
internal metrics but does not serve an HTTP Prometheus endpoint. The regular
database executable exposes that endpoint separately.
