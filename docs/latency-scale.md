# HTA latency over eight orders of query duration

This experiment extends the [earlier 27.8-hour run](latency-long.md) toward
the request-duration axis in Ilsche, *Energy Measurements of High Performance
Computing Systems* (2020), §4.3.5 and Figure 4.9. The dissertation measures
random requests from 1 second to four months, with 20 repetitions per
configuration. Here the 50 logarithmically spaced durations range from
0.1 to 10,000,000 seconds. The six synthetic metrics contain 22 million
points each at 2 samples/s, for **132 million generated points across
11 million seconds (127.3 days)**. The dataset is longer than the largest
request span, so no duration is silently omitted.

The test sends identical data chunks to the legacy C++ file database and the
Go/S3 database through RabbitMQ. It measures publisher-confirmed delivery
time and separately polls `LAST_VALUE` until the final point is visible in
each database. The Go/S3 engine is then flushed before the history workload.
Data AMQP prefetch is 400 for both databases. The Go test uses a 128 MiB
object target, 256 MiB WAL target, and an HTA maximum interval of
10,000,000 seconds. The client requests `AGGREGATE` or
`AGGREGATE_TIMELINE` with 100 or 1,000 display positions, for one or six
metrics. Every condition uses 20 paired random windows, alternating backend
order. The benchmark compares each returned timestamp, count, value, and
aggregate between implementations.

The completed run produced [600 latency rows](latency-scale.csv): 300
configurations per backend, each with 20 random windows. All **21,000
individual response pairs matched**. The [four-panel latency plot](latency-scale.svg)
shows the mean and approximate 95% confidence interval; a [Range-GET plot](latency-scale-gets.svg)
shows the S3 request count. Selected values
below are client end-to-end means; the full CSV includes median, p95,
confidence interval, and S3 Range-GET count and bytes.

| Request | Duration | Metrics | File | S3 | S3 Range GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Timeline, 100 positions | 0.1 s | 1 | 33.9 ms | 18.8 ms | 3.4 |
| Timeline, 100 positions | 109,854 s | 1 | 1.9 ms | 10.8 ms | 2.4 |
| Timeline, 100 positions | 1,048,113 s | 1 | 1.8 ms | 19.5 ms | 6.4 |
| Timeline, 100 positions | 10,000,000 s | 1 | 1.9 ms | 122.0 ms | 43.4 |
| Timeline, 1,000 positions | 10,000,000 s | 1 | 3.4 ms | 130.4 ms | 43.6 |
| Timeline, 1,000 positions | 10,000,000 s | 6 | 15.3 ms | 136.2 ms | 261.8 |
| Aggregate | 10,000,000 s | 1 | 24.0 ms | 36.8 ms | 42.1 |
| Aggregate | 10,000,000 s | 6 | 19.7 ms | 131.3 ms | 249.2 |

For a 100-position, 10,000,000-second timeline, S3 reads only about 17 kB
per metric but performs 43.4 Range GETs on average. Its mean latency is
122.0 ms (p95 135.9 ms), versus 1.9 ms (p95 2.2 ms) for the file DB. At
109,854 seconds, the same S3 request needs only 2.4 Range GETs and 10.8 ms.
The observed request amplification at long durations is the main scalability
finding. The current counters do not separate index-page and data-block GETs,
so they do not yet establish which part of the layout causes it. Six-metric
single aggregates at 10,000,000 seconds average 249.2 Range GETs and
131.3 ms. The file DB has a bimodal latency distribution in several cells;
its means should be read alongside the CSV medians and p95 values.

The [ingestion CSV](ingest-scale.csv) reports separately when RabbitMQ
confirmed publishing and when the final point became visible through each
database's `LAST_VALUE` response. Across six sequential 22-million-point
cells, median publish rate was 673,000 points/s, median file-DB visibility
rate 672,000 points/s, and median Go/S3 visibility rate **60,000 points/s**
(range 58,200–61,500). The file DB kept up with this publisher, so its
reported visibility rate is a lower bound on its possible ingestion capacity,
not an isolated maximum. The Go/S3 rate includes WAL durability,
aggregation, S3 checkpointing, and AMQP/client overhead. It is the observed
end-to-end rate of this setup, not a standalone HTA worker benchmark.

This is an end-to-end development-machine comparison: the file DB runs in
Docker with container `/tmp`, the S3 DB uses local RustFS, and the requests
pass through the local MetricQ/RabbitMQ setup. It does not isolate the
storage engines or equalize their physical storage. It also has much lower
sample density and total volume than the dissertation's six 1-kSa/s metrics
over one year (189 billion values). In particular, 0.1-second windows can
contain no sample at 2 samples/s; their latency cannot be compared directly
with dense 1-kSa/s windows. The experiment only measures client end-to-end
latency, not the server and worker breakdown from the dissertation.

To reproduce:

```sh
docker compose -f compose.test.yml up -d
METRICQ_BENCH_POINTS=22000000 \
METRICQ_BENCH_RATE_HZ=2 \
METRICQ_BENCH_LOG_SPANS=0.1,10000000,50 \
METRICQ_BENCH_HTA_MAX_SECONDS=10000000 \
METRICQ_BENCH_OBJECT_TARGET_BYTES=134217728 \
METRICQ_BENCH_WAL_TARGET_BYTES=268435456 \
METRICQ_BENCH_REPETITIONS=20 \
METRICQ_BENCH_OUTPUT=docs/latency-scale.csv \
METRICQ_BENCH_INGEST_OUTPUT=docs/ingest-scale.csv \
  go test -tags=integration ./integration -run '^TestRequestLatency$' -count=1 -timeout=90m
MPLCONFIGDIR=/tmp/metricq-matplotlib python3 scripts/plot-latency.py \
  docs/latency-scale.csv docs/latency-scale.svg
MPLCONFIGDIR=/tmp/metricq-matplotlib python3 scripts/plot-range-gets.py \
  docs/latency-scale.csv docs/latency-scale-gets.svg
docker compose -f compose.test.yml down
```
