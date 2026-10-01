# Sizing

The numbers below come from measurements on a development machine with local
RustFS (`measurements/` in the repository). Use them as orders of magnitude and
verify with the [dashboard](monitoring.md) under real load.

## Records

Everything scales with **records**, not samples. A sample produces one raw
record plus completed aggregate records:

- about **2.1 records per sample** when `interval_min` is close to the sample
  interval,
- up to about **12 records per sample** for sparse metrics with a much smaller
  `interval_min` (see [Data model](../architecture/data-model.md#hta-levels-and-records)).

## Object storage capacity

Stored blocks need about **20 bytes per record** (gob + gzip), about
**44 bytes per sample** at 2.1 records per sample, plus a few percent for index
pages and metadata.

| Load | Samples per day | Stored per day (≈44 B/sample) | per year |
| --- | ---: | ---: | ---: |
| 1500 metrics × 1/s | 130 M | 5.7 GB | 2.1 TB |
| 100 metrics × 10/s | 86 M | 3.8 GB | 1.4 TB |
| 1500 metrics mixed (10/s … 1/day) | 167 M | 7.3 GB | 2.7 TB |

Data is kept forever; there is no retention. Temporarily, compaction needs
extra space: rewritten blocks exist twice until the source object is fully
evacuated and deleted.

## Object storage traffic

With holding enabled (default), writes are roughly: data blocks once, held
records a second time in `held/` deltas, plus metadata. For 1500 mixed-rate
metrics this was about **460 MB and 800 PUT requests per hour**; without
holding it was 4.3 GB and 5200 PUTs. Compaction adds reads and rewrites of
fragmented data; the catalog dominates its metadata traffic.

## WAL disk

Each delivery is one WAL frame: a 20-byte header, the metric name and about
30 bytes of fixed fields, plus 9 to 12 bytes per sample (varint time delta and
the value). A single-sample delivery of a metric with a 17-character name
takes about **85 bytes**; large chunks approach **10 to 12 bytes per sample**.

The WAL never exceeds `wal_hard_bytes`; ingestion is refused (backpressure)
above `wal_high_bytes`. Provision at least `wal_hard_bytes` + 50 % on a
durable disk. The WAL is emptied after every successful checkpoint, so its
size reflects the time since the last checkpoint and S3 outages.

## Memory

| Consumer | Bound |
| --- | --- |
| Held and pending records | `ingest_memory_limit_bytes` (estimated at 96 bytes per record); held records alone `hold_memory_bytes` |
| Decoded data block cache | 128 MiB |
| Index page cache | 8192 pages (tens of MB) |
| Pinned index paths for checkpoints | 2¹⁹ entries (≈ 45 MB) |
| Per query | up to 256 MiB decoded records; up to 8 history workers in parallel |
| Checkpoint encoding | about the size of the checkpoint's data pack |
| Compaction | `compaction_job_max_bytes` of input plus catalog pages (32 MiB read budget) |

Held records: a stream holds at most 1023 records plus what it receives within
`hold_max_age_seconds`. For 1500 mixed-rate metrics (about 1930 samples/s) the held set
was about 1.3 million records, 130 MB estimated. Set `hold_memory_bytes` above the
expected held set, otherwise streams are written early as small blocks.

A reasonable starting point for 1500 metrics at 1 Hz: `hold_memory_bytes` 512 MiB,
`ingest_memory_limit_bytes` 768 MiB, and 2 GiB of memory for the process.

## Throughput reference

| Operation | Measured |
| --- | --- |
| Ingest, chunks of 500 samples, batched, prefetch 50–200 | ≈ 460 000 samples/s |
| Ingest, chunks of 500 samples, one fsync per delivery | ≈ 90 000 samples/s |
| Ingest, 1 sample per delivery, prefetch 400, backlog after a restart (development stack, 1003 metrics) | ≈ 25 000–30 000 deliveries/s |
| Ingest, 1 sample per delivery, prefetch 100 | ≈ 4 000 deliveries/s (about 50 per fsync) |
| Checkpoint (write to S3) | 450 000 – 770 000 samples/s of checkpoint time |
| Compaction | ≈ 100–250 source blocks/s, bounded by catalog metadata |
| Cold `FLEX_TIMELINE`, compacted layout | 1 data range GET, ≈ 5 ms on local S3 |

With holding, a stream needs merging at most about once per `hold_max_age_seconds`, so
the compaction load is roughly *streams / hold_max_age_seconds* blocks per second
(≈ 3/s for 12 000 streams and one hour).
