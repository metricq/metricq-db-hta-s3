# Group commit and AMQP prefetch

Before this change every DataChunk had its own WAL write and fsync, and the
`metricq-go` adapter handled deliveries strictly one after another. With the
WAL on a real disk (about 3.5 ms per fsync) that capped ingestion at roughly
45,000 points/s for 500-point chunks, independent of the prefetch value.

The adapter now offers an optional `DataBatch` handler. It takes one delivery,
collects further deliveries that RabbitMQ has already prefetched (waiting at
most 500 µs, up to the prefetch count and 16 MiB), and acknowledges the batch
with one multiple ACK after the handler returns. `Engine.IngestBatch` validates
and plans every delivery, writes all WAL frames with one write and one fsync,
and only then publishes the new HTA state. Each accepted delivery keeps its
own frame and sequence number, so replay is unchanged. On WAL/builder pressure
the processed prefix stays durable and the caller flushes and retries only the
remainder. ACK after durability is preserved: nothing in a batch is ACKed
before the fsync covering it.

## Measurement

`TestIngestThroughput` (integration tag) publishes six metrics round-robin in
500-point chunks, one million points each, and measures until `LAST_VALUE`
shows the final point of every metric. Only the Go/S3 database runs; the WAL
is on the home file system (not tmpfs), S3 is local RustFS, checkpoints run in
the background as in the executable, with 4 MiB object and 32 MiB WAL targets.
One run per configuration on a 16-core development machine.

| Mode | Prefetch | Points/s | Chunks per fsync | Flush time (share) |
|---|---:|---:|---:|---:|
| per delivery | 400 | 44,975 | 1.0 | 65 s of 133 s (49 %) |
| batch | 10 | 80,495 | 6.6 | 46 s of 75 s |
| batch | 50 | 104,225 | 24 | 38 s of 58 s |
| batch | 100 | 104,607 | 48 | 41 s of 57 s |
| batch | 200 | 110,091 | 88 | 39 s of 55 s |
| batch | 400 | 117,666 | 171 | 36 s of 51 s (71 %) |
| batch | 1,000 | 131,481 | 226 | 33 s of 46 s |
| batch | 2,000 | 132,561 | 235 | 33 s of 45 s |
| batch | 5,000 | 136,935 | 250 | 34 s of 44 s |

Group commit raises throughput 2.6x at the previous prefetch of 400 and about
3x at 1,000 or more. Above 1,000 the gain flattens: fsync now costs about one
second in total, while checkpoints hold the ingestion mutex for 70-75 % of the
run. Moving flush I/O out of that mutex is the next bottleneck, and the prefetch
sweep should be repeated afterwards. `configs/local.example.json` uses prefetch
16, which with batching lies near the 10-50 rows. Prefetch memory is bounded by
prefetch times chunk size (about 10 MB at 2,000 for these 5 kB chunks).

Raw data: [ingest-group-commit.csv](ingest-group-commit.csv).

```sh
docker compose -f compose.test.yml up -d   # plus the MetricQ development stack
METRICQ_INGEST_WAL_DIR=$HOME/.cache/metricq-ingest-wal \
METRICQ_INGEST_POINTS=1000000 \
METRICQ_INGEST_PREFETCH=10,50,100,200,400,1000,2000,5000 \
METRICQ_INGEST_MODES=batch,single \
METRICQ_INGEST_OUTPUT=docs/ingest-group-commit.csv \
  go test -tags=integration ./integration -run '^TestIngestThroughput$' -count=1 -v -timeout=50m
```
