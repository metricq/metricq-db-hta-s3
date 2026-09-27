# Binary codecs for data blocks and index pages

Measured on 2026-09-27. Reference: isolated archive of `878630a`; implementation:
`d9844d6`. [CSV results, microbenchmarks and sanitized logs](binary-block-codec/)
accompany this report.

## Implementation and durability

New data and index ranges have a versioned binary envelope and a gzip BestSpeed
payload. Records use fixed little-endian fields and preserve all integer and
floating-point bits. Index pages share object keys through a per-page dictionary.
There are no Gob type descriptions in these blocks. The outer index references
and their SHA-256 checksums retain the same semantics.

The decoder bounds record counts, index fan-out, key lengths and decompression;
checks gzip CRCs and exact payload size; rejects unknown versions/kinds, wrong
types, truncation and trailing compressed members/bytes; and does not update its
destination on failure. Records decode in bounded chunks instead of allocating
a duplicate full uncompressed payload. The independent integration layout reader
also understands the new index format.

Existing gzip/Gob blocks remain readable. Ordinary append and compaction write
new binary blocks, including when inputs are mixed. No eager rewrite is needed.
Other metadata, jobs, held deltas and WAL batches keep their existing formats.
WAL fsync-before-ACK, manifest CAS and generation-pinned GC are unchanged.
No external Go libraries were added.

The exact format is documented in
[`docs/architecture/storage-layout.md`](../docs/architecture/storage-layout.md).

## Real S3 compaction comparison

The reference and binary runs use the local RustFS development backend and run
sequentially. Each generates 1,500 metrics with 128 samples each, across eight
flushes (192,000 samples). Holding is disabled. Maintenance has 512-block jobs,
a 64 MiB/s budget, zero cooldown, and continuous scheduling disabled. The timed
region covers compaction and GC; generation/ingestion and layout audits are
outside it. No other performance benchmark runs during these timed regions.

| Measurement | Gob | Binary |
| --- | ---: | ---: |
| Compaction + GC | 30.716 s | 26.819 s |
| Profiled CPU time | 33.30 s | 25.66 s |
| Data/index bytes after compaction and GC | 6,735,500 | 5,150,154 |
| Raw blocks before / after | 12,000 / 1,500 | 12,000 / 1,500 |
| Raw sample count after | 192,000 | 192,000 |
| Publishing jobs | 141 | 146 |
| Index PUT bytes during maintenance | 18,572,684 | 11,924,348 |
| Index range reads during maintenance | 2,470 | 1,853 |
| Index range-read bytes during maintenance | 52,469,699 | 37,176,001 |
| Catalog phase wall sum | 4.188 s | 4.148 s |
| Index phase wall sum | 3.942 s | 3.007 s |
| Copy/merge phase wall sum | 3.982 s | 3.147 s |
| Selection/reservation phase wall sum | 8.923 s | 7.150 s |
| Recovery response comparisons | 24 | 24 |
| Successful Prometheus HTTP scrapes | 31 | 27 |

Wall time falls by 12.7%, equivalent to about 14.5% higher throughput for this
fixture. The retained data/index footprint falls by 23.5%. CPU profiles show
Gob `decodeTypeSequence` falling from 5.18 s to 0.17 s; Gob remains necessary
for metadata. These are single trials, with varying object IDs and selection
order, not a repeated steady-state production capacity experiment. Changed
block sizes affect byte budgets and job composition, as well as serialization.
Nested phase sums overlap and must not be added. Both Prometheus endpoints had
zero scrape failures.

## Codec microbenchmarks

Three repetitions, 200 ms per benchmark, report medians. The Gob reference uses
the same pooled gzip BestSpeed writers as the binary implementation. Raw data
contains sinusoidal values; aggregate data varies counts and values; the index
has 64 entries with cryptographic hashes and four distinct object keys.

| Case | Gob encode | Binary encode | Gob decode | Binary decode |
| --- | ---: | ---: | ---: | ---: |
| 1,024 raw records | 1.214 ms | 0.996 ms | 0.720 ms | 0.563 ms |
| 1,024 aggregate records | 2.645 ms | 1.499 ms | 1.749 ms | 0.993 ms |
| 64-entry index page | 0.407 ms | 0.238 ms | 0.319 ms | 0.116 ms |

Decode allocation counts fall from 236 to 27 for raw records, 244 to 19 for
aggregates, and 382 to 30 for index pages. Allocated bytes per decode fall from
161,672 to 125,016; 211,074 to 124,977; and 73,856 to 65,448, respectively.

Compressed size depends on data: the full raw block grows from 14,011 to
14,346 bytes (+2.4%); the aggregate block shrinks from 32,020 to 30,728 bytes;
the index page shrinks from 3,651 to 3,052 bytes. The short, highly fragmented
S3 dataset benefits more from eliminating repeated schemas. Its size savings
must not be applied uniformly to every full historical block.

## Query behavior and verification

The cold FLEX locality fixture returns 1,000 rows from 16,384 samples. With
16 flushes, compaction still changes nine data GETs into one, without time-window
partitioning. It reads 232,689 bytes before and after; existing Gob measurements
of the same fixture read 254,915 bytes. Small in-memory latency samples are noisy
and are not used to claim an end-to-end query speedup. Codec decode measurements
show the CPU benefit; this run does not replace the dissertation-style query
span/resolution sweep.

All package tests, `go vet`, focused race tests and fuzzing pass. The fuzzer runs
276,995 inputs in approximately 21 seconds. Tests cover bit-preserving values,
integer limits, malformed/truncated payloads, incorrect kinds/versions, excessive
lengths and CRC/trailer errors. A mixed-format integration-style engine test
publishes legacy data/index references, restarts, appends binary blocks, verifies
all four history request types, merges legacy and binary records, reclaims old
objects and verifies responses after a further restart.

The S3 benchmark compares 24 responses after compaction and recovery in each
format. The full legacy request parity run passes 2,875 response pairs across hot
queries, WAL/S3 restart, compaction, compacted S3 restart and outage backpressure.
The parity summary is retained with the measurement artifacts.

## Remaining bottlenecks

Compression still accounts for roughly 6.4 cumulative CPU seconds in the new
profile; metadata Gob encoding for 3.85 CPU seconds, including compression.
Manifest metadata encoding accounts for 3.06 CPU seconds. These values overlap.
Selection still takes 7.15 wall seconds and catalog PUTs consume 9.93 cumulative
request seconds. The next experiment should reduce root/catalog page rewriting
and repeated candidate inspection, rather than only adding more workers.

## Reproduction

```sh
GOCACHE=/tmp/metricq-go-build-cache \
METRICQ_COMPACTION_METRICS=1500 \
METRICQ_COMPACTION_OUTPUT=/tmp/block-codec.csv \
METRICQ_COMPACTION_CPU_PROFILE=/tmp/block-codec.pprof \
go test -tags integration ./integration -run '^TestCompactionS3$' \
  -v -count=1 -timeout 15m

GOCACHE=/tmp/metricq-go-build-cache \
go test ./engine -run '^$' -bench '^BenchmarkBlockCodec$' \
  -benchtime=200ms -count=3

GOCACHE=/tmp/metricq-go-build-cache \
go test ./engine -run '^$' -fuzz '^FuzzBinaryBlock$' \
  -fuzztime=20s -parallel=4

GOCACHE=/tmp/metricq-go-build-cache \
go test -tags integration ./integration -run '^TestLegacyRequestParity$' \
  -v -count=1 -timeout 5m
```

For the Gob S3 reference, archive `878630a` into a separate directory and run the
same S3 command there. `METRICQ_TEST_S3` defaults to `http://localhost:19000`;
unique test buckets are cleaned up after the tests.
