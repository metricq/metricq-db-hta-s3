# HTA request latency with a longer history

For the subsequent 50-duration run over 0.1–10,000,000 seconds, see the
[132-million-point comparison](latency-scale.md).

This follow-up to the [development baseline](latency-benchmark.md) uses six
synthetic metrics with **1,000,000 samples each at 10 samples/s**: six million
points over 100,000 seconds (27.8 hours). That is 50 times as many points and
50 times the history length of the first run. The six request spans are 1, 10,
100, 1,000, 10,000, and 50,000 seconds; the largest is 13.9 hours. Each span
is tested with one or six metrics and with `AGGREGATE`, 100-position timeline,
or 1,000-position timeline requests. This gives 36 configurations for each
backend, with 20 paired random windows per configuration. All 2,520
individual response pairs matched between the C++ file DB and Go/S3 DB.

The [CSV](latency-long.csv) contains all 72 result rows. The [plot](latency-long.svg)
shows mean client end-to-end latency with an approximate 95% confidence
interval. Selected results below include the 95th percentile and mean S3
Range-GET count. `File` is the C++ database in Docker using container `/tmp`;
`S3` is the Go database using local RustFS. Requests run through RabbitMQ and
the MetricQ history client, with six requests issued concurrently in the
six-metric configurations. The Go database was flushed to S3 before timing.
The request order alternated between backends.

| Request | Span | Metrics | File mean / p95 | S3 mean / p95 | S3 Range GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Timeline, 100 positions | 1,000 s | 1 | 1.8 / 2.2 ms | 9.3 / 12.5 ms | 2.4 |
| Timeline, 100 positions | 10,000 s | 1 | 1.6 / 2.0 ms | 14.4 / 19.9 ms | 5.5 |
| Timeline, 100 positions | 50,000 s | 1 | 2.1 / 2.5 ms | 44.5 / 53.0 ms | 18.5 |
| Timeline, 100 positions | 50,000 s | 6 | 17.0 / 48.3 ms | 39.1 / 46.2 ms | 112.0 |
| Timeline, 1,000 positions | 50,000 s | 6 | 20.1 / 55.1 ms | 55.5 / 62.3 ms | 111.8 |
| Aggregate | 1,000 s | 6 | 18.7 / 45.4 ms | 44.3 / 48.8 ms | 90.3 |
| Aggregate | 50,000 s | 1 | 23.4 / 47.4 ms | 43.3 / 50.3 ms | 37.1 |
| Aggregate | 50,000 s | 6 | 12.7 / 46.3 ms | 83.2 / 88.0 ms | 227.9 |

The longer history exposes a scalability issue in the current S3 layout. A
single-metric, 100-position timeline request rises from 2.4 Range GETs at
1,000 seconds to 18.5 at 50,000 seconds; its mean latency rises from 9.3 to
44.5 ms. The six-metric, 50,000-second aggregate needs about 228 Range GETs,
although it reads only about 2.0 MB total. The request count, rather than
payload size, is the clear concern for a remote S3 deployment. These data do
not identify which share of the GETs fetches index pages versus data blocks.
The file database has a strongly bimodal latency distribution in many cells,
so its means alone are a poor ranking metric; consult the CSV's median and
p95 columns as well.

The run samples random windows across a 27.8-hour history. It does not
separately compare old and recent windows, test multi-year retention, or
equalize the backends' physical storage and caching. It cannot establish the
design goal that old data is as fast to query as new data. It is an end-to-end
development-machine comparison, not a reproduction of Ilsche's one-year,
1 kSa/s experiment.

To reproduce with the local development Compose setup already running:

```sh
docker compose -f compose.test.yml up -d
METRICQ_BENCH_POINTS=1000000 \
METRICQ_BENCH_RATE_HZ=10 \
METRICQ_BENCH_SPANS=1,10,100,1000,10000,50000 \
METRICQ_BENCH_OBJECT_TARGET_BYTES=8388608 \
METRICQ_BENCH_REPETITIONS=20 \
METRICQ_BENCH_OUTPUT=measurements/latency-long.csv \
  go test -tags=integration ./integration -run '^TestRequestLatency$' -count=1 -timeout=45m
MPLCONFIGDIR=/tmp/metricq-matplotlib python3 scripts/plot-latency.py \
  measurements/latency-long.csv measurements/latency-long.svg
docker compose -f compose.test.yml down
```

The 8 MiB object target in this run is larger than the baseline's 1 MiB
target. The Go ingest handler flushes at its pressure threshold so the larger
dataset can pass through the bounded builder. The historical responses were
compared for every measured request, but this benchmark does not exercise
crash recovery or S3 write failures.
