# Stored bytes per record in the development stack

Measured on 2026-10-01 at 14:35 UTC with `c8c7516` and later (binary data
blocks from `d9844d6`). Source: the MetricQ development stack with local
RustFS, database `db-hta-s3-dummy`, `hold_memory_bytes` 128 MiB.

## Load

- 1000 metrics `load.hta-s3.m0000` … `m0999` at 1 sample per second, one
  sample per message, values a bounded random walk rounded to two decimals
  (`cmd/metricq-db-hta-s3-load`). HTA: `interval_min` 40 s, factor 10,
  `interval_max` 400 000 s. Raw data spanned 17.3 h.
- `dummy.source.hta-s3`: about 100 samples per second, `interval_min` 0.4 s,
  24.8 h.
- Two RabbitMQ rate metrics, `interval_min` 400 s.

Sizes are the sum of index entry lengths per stream (compressed block bytes),
read from the index of the published manifest.

## Results

| Group | Level | Records | Bytes | Bytes per record |
| --- | --- | ---: | ---: | ---: |
| load (1000 metrics) | raw | 58 368 000 | 583 134 178 | 9.99 |
| load | 40 s | 1 389 000 | 36 942 478 | 26.60 |
| load | 400 s | 143 000 | 4 331 562 | 30.29 |
| load | 4000 s | 14 000 | 607 821 | 43.42 |
| load | 40 000 s | 2 000 | 178 046 | 89.02 |
| load | total | 59 916 000 | 625 194 085 | 10.43 |
| dummy | raw | 8 538 874 | 109 536 958 | 12.83 |
| dummy | 0.4 s | 212 992 | 5 006 587 | 23.51 |
| dummy | 4 s | 20 480 | 461 834 | 22.55 |
| dummy | total | 8 774 718 | 115 046 124 | 13.11 |

Coarse levels show high bytes per record because their blocks are still
small (fixed per-block overhead); they converge as blocks fill.

- Records per sample: 1.03 for all groups (`interval_min` 40 times the sample
  interval).
- Bytes per sample: load 10.71, dummy 13.47, rate metrics 15.59.
- Load growth: 625 MB in 17.3 h, about 36 MB per hour or 0.87 GB per day.

## Object store totals at the same time

| Kind | Objects | MB |
| --- | ---: | ---: |
| data | 1304 | 751.0 (1.4 % dead) |
| held | 96 | 24.4 |
| index | 74 | 4.7 |
| catalog + candidates | 539 | 4.7 |
| roots, state, held-state, manifest | 8 | 0.8 |
