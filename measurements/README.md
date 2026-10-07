# Measurements and engineering reports

This directory keeps benchmark results, reviews and design notes in the order
they were produced. They record *why* the implementation looks the way it does
and the numbers behind decisions. They are not maintained as current
documentation: later changes can supersede individual statements. The
maintained documentation for developers and operators lives in
[`../docs`](../docs) and is published with GitLab Pages.

Raw data (`*.csv`, `*.txt`) and plots (`*.svg`) belong to the report that
references them. The commands to reproduce a measurement are in its report;
output paths point into this directory.

| Report | Topic |
| --- | --- |
| [latency-benchmark.md](latency-benchmark.md) | First end-to-end latency baseline against the legacy C++ database |
| [latency-long.md](latency-long.md) | Latency with a longer history |
| [latency-scale.md](latency-scale.md) | Latency over eight orders of query duration (132 million points) |
| [latency-scale-f23c0ed.md](latency-scale-f23c0ed.md) | Dissertation-style query sweep after range coalescing |
| [optimization.md](optimization.md) | Batched checkpoints and aggregate tail consolidation |
| [cardinality.md](cardinality.md) | Many active metrics with fixed memory settings |
| [storage-v2.md](storage-v2.md) | Design note of the object format (superseded by `docs/architecture`) |
| [compaction-plan.md](compaction-plan.md) | Background compaction design and operation notes |
| [compaction-review.md](compaction-review.md) | Review: query layout and deferred ingestion work |
| [compaction-optimizations.md](compaction-optimizations.md) | Compaction and append-only flush improvements |
| [ingest-group-commit.md](ingest-group-commit.md) | Group commit, AMQP prefetch and checkpoints outside the ingestion lock |
| [hold-back.md](hold-back.md) | Holding streams in memory until they fill a block |
| [capacity-review-c0d3a3d.md](capacity-review-c0d3a3d.md) | Capacity and history locality review at c0d3a3d |
| [paged-manifest-and-level-locality.md](paged-manifest-and-level-locality.md) | Paged checkpoint metadata and level locality |
| [metadata-split.md](metadata-split.md) | Independent held metadata, hourly write costs and recovery tradeoffs |
| [compaction-throughput.md](compaction-throughput.md) | Shared metadata caches, paged inventories and measured compaction throughput |
| [inventory-upload-pipeline.md](inventory-upload-pipeline.md) | Bounded parallel inventory PUTs, publication barrier and fresh S3 comparison |
| [binary-block-codec.md](binary-block-codec.md) | Versioned binary data/index codecs, CPU/allocation benchmarks and S3 compaction comparison |
| [perf-2026-10-07.md](perf-2026-10-07.md) | Catch-up and chunked ingest rates; dissertation query matrix on the converged development database |
