# HTA request latency: first development baseline

This experiment adapts the query matrix of Thomas Ilsche, *Energy
Measurements of High Performance Computing Systems: From Instrumentation to
Analysis* (2020), §4.3.5 and Figure 4.9. It is **not** a reproduction of the
published performance numbers. The original experiment used six metrics at
1 kSa/s for one year (189 billion values), dedicated SSD partitions, and a
different two-node setup. This baseline uses six synthetic metrics with
20,000 samples each at 10 Sa/s (a 2,000-second history) on one development
machine.

The same points were delivered through RabbitMQ to the C++ file database and
the Go/S3 database under separate history names. The benchmark waits until
both have ingested the final point, flushes the Go database to S3, and then
requests the same random time window from both. Every measured response pair
is checked for equal timestamps, counts, values, and aggregates. Request
order alternates between backends. Each configuration has 20 random windows.
The measured time runs from issuing the first history request to receiving
all responses in the Go client. Six-metric configurations send six requests
concurrently. Aggregate requests return one aggregate; timeline requests
use `AGGREGATE_TIMELINE` and intervals derived from 100 or 1,000 display
positions. Time spans are 1, 10, 100, and 1,000 seconds.

The old database ran in Docker with its file path at container `/tmp`; the Go
database used the local RustFS S3 test service. RabbitMQ and the MetricQ
manager were from the local development Compose setup. The client and Go
database ran on an AMD Ryzen 7 PRO 4750U host. This compares the available
development paths, including RabbitMQ, container storage, the S3 API, and
client processing. It does not isolate backend code, equalize physical
storage, flush OS caches, or report the server/HTA-worker latencies measured
in the dissertation.

The full results are [CSV](latency-baseline.csv) and a [four-panel SVG
plot](latency-baseline.svg). Times below are mean end-to-end milliseconds;
the CSV also records approximate 95% mean confidence intervals, median,
95th percentile, and S3 Range-GET count and bytes. The file backend shows a
strongly bimodal distribution in many cells (roughly 2–6 ms versus 40–50 ms),
so compare its median and 95th percentile as well as its mean.

| Request | Span | Metrics | File | S3 | S3 Range GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Timeline, 100 positions | 1 s | 1 | 29.8 ms | 24.5 ms | 4 |
| Timeline, 100 positions | 100 s | 1 | 29.2 ms | 11.6 ms | 4 |
| Timeline, 100 positions | 1,000 s | 1 | 1.3 ms | 7.8 ms | 4 |
| Timeline, 100 positions | 1,000 s | 6 | 17.3 ms | 52.9 ms | 24 |
| Aggregate | 1 s | 1 | 21.7 ms | 69.6 ms | 11 |
| Aggregate | 1,000 s | 1 | 23.1 ms | 107.7 ms | 29 |
| Aggregate | 1,000 s | 6 | 28.2 ms | 631.6 ms | 183 |

The Go/S3 timeline path reads four ranges per metric in this dataset and
benefits from choosing preaggregated levels for wider requests. The
single-aggregate path is much more expensive: its boundary and hierarchy
queries repeatedly read index and data blocks, rising from 11 to about 29
Range GETs for one metric and 66 to 183 GETs for six metrics. Even a
one-second aggregate reads about 760 kB of packed blocks per metric. These
counts make reducing repeated reads and block amplification the first
performance target. The six-metric aggregate latency already exceeds the
sub-30-ms range shown in the dissertation, and a remote S3 service would
add network latency to these serialized reads.

To rerun on the local development setup:

```sh
docker compose -f compose.test.yml up -d
METRICQ_BENCH_OUTPUT=docs/latency-current.csv \
  go test -tags=integration ./integration -run '^TestRequestLatency$' -count=1
MPLCONFIGDIR=/tmp/metricq-matplotlib python3 scripts/plot-latency.py \
  docs/latency-current.csv docs/latency-current.svg
docker compose -f compose.test.yml down
```

`METRICQ_BENCH_POINTS` changes samples per metric,
`METRICQ_BENCH_RATE_HZ` changes the sampling rate,
`METRICQ_BENCH_SPANS` selects comma-separated request spans in seconds,
`METRICQ_BENCH_OBJECT_TARGET_BYTES` sets the Go object's target size, and
`METRICQ_BENCH_REPETITIONS` changes the number of random windows. The
[larger follow-up run](latency-long.md) extends the history to 100,000 seconds
and the maximum query span to 50,000 seconds. Neither run is large enough to
establish old-versus-recent query cost over years of production data.

## After query optimization

The same benchmark was repeated after limiting each stored data block to
1,024 records, adding a bounded shared cache of immutable index pages,
reading independent HTA levels concurrently for `AGGREGATE`, and handling
history requests on multiple AMQP worker channels. The original
[CSV](latency-baseline.csv) remains unchanged; intermediate measurements after
block splitting and after index caching are in
[latency-chunked.csv](latency-chunked.csv) and
[latency-cached.csv](latency-cached.csv). The result before the AMQP-worker
change is in [latency-level-parallel.csv](latency-level-parallel.csv). The final results are
[latency-optimized.csv](latency-optimized.csv) and the
[optimized plot](latency-optimized.svg). Each run used 20 paired random
windows per configuration, checked against the file database.

| Request | Span | Metrics | S3 before | S3 after | File in final run | S3 GETs before → after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Aggregate | 1 s | 1 | 69.6 ms | 14.3 ms | 43.3 ms | 11 → 4.6 |
| Aggregate | 1,000 s | 1 | 107.7 ms | 25.0 ms | 23.1 ms | 29.3 → 11.1 |
| Aggregate | 1,000 s | 6 | 631.6 ms | 47.6 ms | 27.5 ms | 183 → 68.1 |
| Timeline, 100 positions | 1,000 s | 1 | 7.8 ms | 5.4 ms | 1.8 ms | 4 → 1 |
| Timeline, 100 positions | 1,000 s | 6 | 52.9 ms | 9.6 ms | 20.5 ms | 24 → 6 |

For the six-metric, 1,000-second aggregate, S3 bytes read dropped from about
5.0 MB to 1.3 MB. End-to-end latency improved by 13.3×; the remaining S3
time is about 1.7× the file database's time in the final run. The 68 Range
GETs and object-store request overhead remain the main measured cost.
Cross-run timing variation, especially the file database's bimodal
latency, means the GET and byte counts are stronger evidence of the storage
improvement than a single timing ratio.
