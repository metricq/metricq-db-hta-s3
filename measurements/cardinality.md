# Many active metrics with fixed memory settings

`integration/cardinality_test.go` measures the Go storage engine directly
against the local S3 test service. All metrics receive one chunk in turn
before the next chunk is sent to any metric. Thus global checkpoints see
many active streams instead of one metric being filled completely at a time.
It does not measure AMQP or the legacy file DB; legacy request equivalence is
covered by the separate integration test. Here cold, sweep and hot queries
must return identical responses.

The metric-count axis is configurable, defaulting to 6, 150 and 1,500.
The default dataset has 128 points per metric, spaced one second apart,
delivered in 16-point chunks as fast as the engine accepts them. This short
workload examines flush-induced block fragmentation and stale object bytes.
It is not a long-history or cache-capacity stress result. For those workloads,
increase the points per metric while preserving the same limits.

Every case uses the same settings: 4 MiB estimated object target, 32 MiB
builder limit, 16 MiB WAL target, 32/48 MiB WAL high/hard thresholds, the
existing 128 MiB decoded-data cache and 8,192-page index cache. A 256 MiB Go
soft memory limit is also applied. These are buffer/cache thresholds and a
runtime soft limit, not a hard RSS cap. The sampled peak Go heap is recorded;
Prometheus also exposes Go and process metrics. Tail rewrites and encoding
buffers can increase actual memory beyond the pending-record estimate.

The benchmark serves `/metrics` while running. By default it chooses a free
localhost port and prints its URL; `METRICQ_CARDINALITY_LISTEN` selects a fixed
address. Each case uses its own registry. The endpoint is automatically closed
when the benchmark completes. It exposes the engine's WAL, pressure, flush,
query and store metrics together with Go/process metrics. A request to the
endpoint is verified in each case before ingestion starts.

For each request shape and a target of one or up to six metrics, the benchmark
records three cache states with identical queries:

- **cold:** a fresh Engine with empty application caches for each repetition.
  Manifest loading and WAL opening occur before the timer and are excluded.
  This does not evict the S3 service's or operating system's caches.
- **sweep:** the long-lived Engine after querying all configured metrics for
  the request shape, without an immediate repeat to warm the selected metrics.
- **hot:** a separate fresh Engine queried once with the exact measured request
  before timing the repeat. Hot warmups never populate the sweep Engine.

Multiple requested metrics are queried sequentially; group latency is their
sum, not concurrent dashboard latency. Four request shapes cover a small raw
window, whole-range FLEX timelines for 100/1,000 positions, and a whole-range
aggregate. There are three repetitions per condition by default.

The layout audit follows the published manifest and index trees, checks index
checksums, counts indexed raw records, and rejects underfilled aggregate blocks
except the last block in a stream. It does not fetch/decode sample blocks.
Audit traffic, cache warmups and Engine startup are outside query counters.

`cardinality.csv` contains ingestion rate, buffer settings, sampled peak heap,
raw/aggregate block sizes, and S3 write accounting. **Write amplification** is
successful data/index/manifest PUT body bytes divided by `16 × sample count`
(one 64-bit timestamp and one float64 value). It excludes WAL writes, HTTP
headers and provider-internal replication. Protobuf input bytes are recorded
separately. PUT volume includes all superseded versions; reachable block bytes
are counted by the audit. Their difference is the unreferenced part of packed
objects, not necessarily objects that could be deleted wholesale: a pack can
contain both live raw blocks and superseded aggregate tails.

`cardinality-queries.csv` records each query repetition, latency, response rows,
and separate data/index Range GET counts and bytes. Both CSVs are flushed as
results arrive. Buckets and the metrics server are cleaned up after each run.
No RabbitMQ queues are created by this storage-engine benchmark.

## Completed short-history comparison

The [summary CSV](cardinality.csv), [query CSV](cardinality-queries.csv) and
[plot](cardinality.svg) use the defaults above: 128 points per metric,
16-point deliveries and three repetitions per query condition.
All 216 query measurements completed; cold/sweep/hot responses matched.
The experiment contains 6, 150 and 1,500 simultaneously advancing metrics.

| Active metrics | Mean raw records/block | Obsolete data bytes | S3 PUT/input ratio | Ingest points/s |
| --- | ---: | ---: | ---: | ---: |
| 6 | 128.0 | 0.0% | 3.98 | 21,689 |
| 150 | 95.0 | 16.9% | 4.84 | 16,148 |
| 1,500 | 16.0 | 67.2% | 18.01 | 1,246 |

All cases have zero underfilled non-tail aggregate blocks. The 1,500-metric
case wrote 34.64 MB of data-pack bytes, of which only 11.38 MB remain
referenced. It also wrote 16.73 MB of index packs and 3.96 MB of manifests
against 3.07 MB of nominal timestamp/value input. The write ratio therefore
includes index and manifest overhead as well as repeated aggregate tails.
These are Go/S3 engine rates for small deliveries, not the large-delivery
AMQP ingestion rates reported in the earlier six-metric experiment.

For one-metric whole-range timelines with 100 positions, cold mean latency
was 4.50/5.24/6.13 ms at 6/150/1,500 metrics, with two GETs in each case.
Hot and sweep queries used zero GETs. This short dataset fits the decoded-data
cache, so it does not demonstrate eviction under cache pressure.

## Cache-sized comparison

The second workload uses 1,024 points per metric, 64-point deliveries and
ten repetitions. The same buffers/caches remain fixed for all three metric
counts. Its 1,000-position whole-range timeline selects the one-second HTA
level. About 1,023 physical records per metric across 1,500 metrics need
approximately 187 MiB at the cache's accounting rate of 128 bytes/record,
exceeding the fixed 128 MiB cache. This tests eviction after sweeping the
whole metric set. The shorter workload and this one have different delivery
sizes; their differences cannot be attributed to history length alone.

Artifacts: [summary](cardinality-cache.csv),
[queries](cardinality-cache-queries.csv), [plot](cardinality-cache.svg).

The completed run contains 720 query measurements and 1,680 individual
cold-versus-sweep/hot response comparisons, all matching. It ran for
1,400 seconds against local S3. The small follow-up workload also passed
with the Go race detector. The interrupted run before the laptop lost
power is excluded from these artifacts; all three cases were rerun.

| Active metrics | Mean raw records/block | Unreferenced data bytes | S3 PUT/input ratio | Ingest points/s | Peak Go heap MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 6 | 1,024.0 | 0.0% | 2.88 | 25,267 | 7.8 |
| 150 | 117.0 | 73.5% | 11.67 | 1,824 | 52.8 |
| 1,500 | 64.0 | 83.0% | 19.69 | 1,311 | 186.1 |

The 1,500-metric case wrote 400.07 MB of data packs, 46.57 MB of index
packs and 37.16 MB of manifests for 24.58 MB of nominal sample input.
Only 67.84 MB of the written data-pack ranges remain referenced. There
were again no underfilled non-tail aggregate blocks. Raw blocks fragment
under frequent global checkpoints, while extending aggregate tails retains
dense query blocks at the cost of rewriting older contents. This benchmark
does not implement garbage collection or change that storage policy.

For the fine, 1,000-position timeline at 1,500 metrics:

| Requested metrics | Cold mean ms | Sweep mean ms | Hot mean ms | Sweep data GETs / metric query |
| --- | ---: | ---: | ---: | ---: |
| 1 | 8.93 | 1.13 | 0.27 | 1 / 10 |
| 6 | 59.13 | 7.00 | 1.49 | 12 / 60 |

Cold requests needed one data and one index GET per metric. Hot repeats
needed neither. Sweep requests needed no index GETs, but 13 of the 70
measured metric queries fetched a data block: the working set no longer
fits completely in the data cache. At 6 and 150 configured metrics these
fine sweep queries needed no GETs. The sampled peak heap is within the
configured soft limit; this is not proof of a hard process-memory bound.

Absolute timings come from one local laptop run and fluctuate with machine
conditions. In particular, cold latency at 1,500 metrics was lower than at
150, so the timings should not be treated as a clean scaling curve. The
block sizes, written byte counts and GET counts provide the more direct
evidence of fragmentation, write amplification and cache eviction.

Run the default comparison:

```sh
docker compose -f compose.test.yml up -d
METRICQ_CARDINALITY_LISTEN=127.0.0.1:19091 \
METRICQ_CARDINALITY_OUTPUT=measurements/cardinality.csv \
METRICQ_CARDINALITY_QUERY_OUTPUT=measurements/cardinality-queries.csv \
  go test -v -tags=integration ./integration \
  -run '^TestMetricCardinality$' -count=1 -timeout=60m
MPLCONFIGDIR=/tmp/metricq-matplotlib python3 scripts/plot-cardinality.py \
  measurements/cardinality.csv measurements/cardinality-queries.csv measurements/cardinality.svg
docker compose -f compose.test.yml down
```

For the cache-sized workload, set `METRICQ_CARDINALITY_POINTS=1024`,
`METRICQ_CARDINALITY_CHUNK=64` and `METRICQ_CARDINALITY_REPETITIONS=10`, with
output names `measurements/cardinality-cache.csv` and
`measurements/cardinality-cache-queries.csv`. Pass `timeline-1000` as the fourth
argument to the plot script. Keep the remaining limits fixed. Runtime and
S3 read/write volume can grow substantially. `METRICQ_CARDINALITY_METRICS`, `METRICQ_CARDINALITY_CHUNK`,
`METRICQ_CARDINALITY_REPETITIONS`, `METRICQ_CARDINALITY_OBJECT_TARGET_BYTES`,
`METRICQ_CARDINALITY_BUILDER_HARD_BYTES`, `METRICQ_CARDINALITY_WAL_TARGET_BYTES`,
and `METRICQ_CARDINALITY_MEMORY_LIMIT_BYTES` customize the experiment.
