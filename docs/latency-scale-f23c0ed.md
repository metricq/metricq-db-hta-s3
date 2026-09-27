# Dissertation-style query sweep, 2026-09-26

The run started from `f23c0ed` and completed successfully in 1216.746 seconds.
It repeats the matrix in [latency-scale.md](latency-scale.md), inspired by
Ilsche's dissertation, section 4.3.5 and Figure 4.9:

- Six metrics, each with 22 million generated samples at 2 samples/s:
  132 million samples spanning approximately 127 days.
- 50 logarithmically spaced query durations from 0.1 to 10,000,000 seconds.
- Single aggregate, aggregate timeline with 100 positions, and aggregate
  timeline with 1000 positions; one metric or six metrics concurrently.
- 20 paired random windows per configuration, alternating backend order.
  All 21,000 individual response pairs matched between legacy file DB and Go/S3.

There are 300 configurations per backend and 600 CSV rows. The dataset and
sampling density remain smaller than the dissertation's 189 billion samples.
The timelines use AGGREGATE_TIMELINE, as in the preceding comparison runs.

Artifacts:

- [Full latency and read counters](latency-scale-f23c0ed.csv)
- [Four-panel latency chart](latency-scale-f23c0ed.svg)
- [Four-panel S3 Range-GET chart](latency-scale-f23c0ed-gets.svg)

Selected means for queries spanning 10,000,000 seconds:

| Query | Metrics | File ms | S3 ms | S3 p95 ms | S3 Range GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Timeline, 100 positions | 1 | 1.472 | 1.505 | 2.061 | 0.0 |
| Timeline, 1000 positions | 1 | 3.054 | 3.345 | 4.240 | 0.0 |
| Timeline, 1000 positions | 6 | 15.696 | 5.996 | 8.700 | 0.0 |
| Aggregate | 1 | 44.653 | 14.839 | 16.786 | 7.1 |
| Aggregate | 6 | 30.961 | 25.952 | 31.119 | 39.0 |

Compared with [the preceding optimized run](latency-scale-optimized.csv),
the six-metric aggregate at the largest span decreased from 51.397 to
25.952 ms, and from 72.3 to 39.0 Range GETs. These runs occurred at different
times; this is not an isolated causal measurement of one commit.
The largest mean S3 latency across all configurations was 32.416 ms.

## Interpretation and limits

The benchmark uses the real local RabbitMQ/MetricQ setup, a temporary legacy
C++ file DB container, and local RustFS S3. Shared caches persist across
requests: zero GETs on the large timeline cells indicate cache hits. This is
not a cold-cache or remote-S3 latency measurement. An unrelated dummy data
queue with over 400,000 messages and no consumer was present in the shared
development broker; it was not modified by the benchmark.

The test starts the engine and adapter directly, uses the single-delivery
ingestion callback, and flushes before the query sweep. It does not start
the background compactor or the Prometheus HTTP endpoint. Therefore these
results do not establish steady-state compaction capacity or performance
with concurrent ingestion. In particular, this run does not measure a
layout converged by compaction.

The later commits `e98d8bd` (parallel decoding within coalesced ranges),
`b17b27b` (group commit), and `376246d` (checkpoint uploads outside the
ingestion lock) were committed after this run and are not represented here.

Reproduction uses the environment in [latency-scale.md](latency-scale.md),
with `METRICQ_BENCH_OUTPUT=docs/latency-scale-f23c0ed.csv` and the source
revision stated above. The full command uses 22,000,000 samples per metric,
2 Hz, `METRICQ_BENCH_LOG_SPANS=0.1,10000000,50`, an HTA maximum of
10,000,000 seconds, a 128 MiB object target, a 256 MiB WAL target and
20 repetitions. No new Go dependencies or production code were added.
