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

The table below was measured with an earlier codec (fixed binary fields
compressed with gzip per block); varint records with zstd after compaction
are smaller, see the size note below the table. Measured
in the development stack (1000 metrics at 1 Hz with values as a random walk
with two decimals, plus a 100 Hz metric, about 17 hours;
`measurements/storage-per-record.md`):

| Records | Stored bytes per record |
| --- | ---: |
| raw, 1 Hz | 10.0 |
| raw, 100 Hz | 12.8 |
| aggregates (six fields), 40 s and 400 s levels | 27 – 30 |
| aggregates, 0.4 s and 4 s levels of the 100 Hz metric | 17 – 24 |

With the current codec, compacted blocks (zstd best) of the same development
stack store 2.8 bytes per raw 1 Hz record, 6.1 per raw 100 Hz record and 2.6
per raw 1 kHz record, and 13 – 18 bytes per aggregate record; after the
migration to zstd its data blocks shrank from 2273 to 1782 MB.

Real measurements compress better or worse depending on their values.
Bytes per sample are roughly *raw bytes + (records per sample − 1) ×
aggregate bytes*; records per sample depend on `interval_min` (see above).
Index pages and metadata add about 1 %; `held/` deltas a few percent until
their records are written.

| Load | Samples per day | Bytes per sample | Stored per day | per year |
| --- | ---: | ---: | ---: | ---: |
| 1500 metrics × 1/s, `interval_min` 40 s (measured: 1.03 records per sample) | 130 M | 10.7 | 1.4 GB | 0.5 TB |
| 1500 metrics × 1/s, `interval_min` 1 s (estimate: 2.1 records per sample) | 130 M | ≈ 40 | ≈ 5.2 GB | ≈ 1.9 TB |
| 100 metrics × 10/s, `interval_min` 1 s (estimate: 1.11 records per sample) | 86 M | ≈ 13 | ≈ 1.1 GB | ≈ 0.4 TB |

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
| History queries | `query_memory_bytes` for decoded records of all running queries together (default 512 MiB); up to 8 history workers in parallel |
| Checkpoint encoding | about the size of the checkpoint's data pack |
| Compaction | `compaction_job_max_bytes` of input plus catalog pages (32 MiB read budget) |

Held records: a stream holds at most 1023 records plus what it receives within
`hold_max_age_seconds`. For 1500 mixed-rate metrics (about 1930 samples/s) the held set
was about 1.3 million records, 130 MB estimated. Set `hold_memory_bytes` above the
expected held set, otherwise streams are written early as small blocks.

A reasonable starting point for 1500 metrics at 1 Hz: `hold_memory_bytes` 512 MiB,
`ingest_memory_limit_bytes` 768 MiB, and 2 GiB of memory for the process.

## Example: 10 000 metrics at 1 Hz

An estimate for a load test, scaled from the development database on
2026-10-11. It ran 1009 metrics, of which 1000 run at 1 Hz with `interval_min`
40 s and factor 10, plus a 100 Hz and a few slower metrics, about 1100
samples/s and 6060 streams. Its current codec and layout were measured over
24 hours:

| Measured, 1009 metrics | 24 h |
| --- | ---: |
| Samples | 95 M (1100 /s) |
| Growth of live data | 350 MB (3.7 bytes per sample including aggregates) |
| Stored, including dead bytes and partial objects | live + 9 % |
| Held and pending records | 0.59 M on average, 1.22 M at most (≈ 1200 per metric) |
| Ingest memory (estimated at 96 bytes per record) | 117 MB at most |
| Process memory (RSS) | 640 MB on average, 880 MB at most |
| Checkpoints | 94 per hour, each rewriting about 240 KB of state pages |

Storage and held records scale with the number of streams and samples. Caches
and encoders do not: block cache 128 MiB, inventory cache 64 MiB, catalog
cache 32 MiB, and one best-level zstd encoder of about 70 MB. Ten times the
metrics give:

| Projected, 10 000 metrics × 1 Hz | |
| --- | ---: |
| Samples | 864 M per day (10 000 /s) |
| Live data | ≈ 3.2 GB per day, ≈ 1.2 TB per year; stored ≈ 3.5 GB per day, ≈ 1.3 TB per year |
| New objects | ≈ 1000 per day at about 3 MB each, ≈ 370 000 per year; catalog inventories add about 2 % |
| Streams | ≈ 60 000 (six levels per metric) |
| Held and pending records | ≈ 6 M on average, 12 M at peak: 0.6–1.2 GB |
| Process memory | ≈ 2–3 GB |
| WAL, one sample per delivery | ≈ 85 bytes × 10 000 /s ≈ 0.85 MB/s |
| Compaction merges with holding | ≈ 60 000 streams / 3600 s ≈ 17 blocks/s |

A checkpoint is due after `checkpoint_unsaved_bytes` of records held only in
the WAL. With the default 4 MiB that is every 38 s at 1100 samples/s, but
every 4 s at 10 000 samples/s. Each checkpoint also rewrites the state pages
of all metrics, about 2.4 MB for 10 000 metrics, which would be about 2 GB/h
of writes. Scale the checkpoint threshold with the load so checkpoints stay
at roughly two per minute:

```json
{
  "engine": {
    "checkpoint_unsaved_bytes": 33554432,
    "wal_target_bytes": 67108864,
    "wal_high_bytes": 268435456,
    "wal_hard_bytes": 335544320,
    "hold_memory_bytes": 1610612736,
    "ingest_memory_limit_bytes": 2147483648
  }
}
```

- `hold_memory_bytes` above the peak held set (1.2 GB) keeps streams from
  being written early as small blocks; `ingest_memory_limit_bytes` leaves
  room for pending records on top.
- With `wal_hard_bytes` of 320 MiB, the WAL absorbs about six minutes of S3
  outage at 0.85 MB/s before backpressure.
- Give the process 4 GiB and set `GOMEMLIMIT` to about 3.5 GiB, so garbage
  collection tightens before the container limit.
- Ingest of single-sample deliveries was measured at 25 000–30 000 per
  second. That leaves 2.5–3 times headroom: a backlog from a 10-minute
  outage (6 M deliveries) drains in about 7 minutes.

These are extrapolations; held records, checkpoint rate and process memory
should be verified with the dashboard during the test.

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
