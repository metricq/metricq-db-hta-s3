# Data model

## Metrics, inputs and configuration

MetricQ uses *metric* for a named time series and *data point* for a
timestamp/value pair. This document uses *sample* when describing the database
engine's ingest and aggregation behavior; it refers to the same data point.

The storage engine uses these terms precisely:

- A **record** is a stored raw value or an HTA aggregate (including a gap run).
- A **stream** is one metric at one HTA level.
- A **block** contains up to 1024 records from one stream.
- A **WAL frame** is one checksummed log entry for one data delivery. A **WAL
  segment** is a local file containing one or more frames.

The MetricQ manager sends the database a configuration with one entry per
stored metric:

```json
{
  "metrics": {
    "dummy.source.go": {
      "input": "dummy.source",
      "interval_min": 400000000,
      "interval_max": 400000000000000,
      "interval_factor": 10
    }
  }
}
```

- The **key** is the canonical metric name. It is the storage identity and the
  name under which history requests are answered.
- **`input`** (optional, defaults to the key) is the MetricQ metric whose data
  is consumed. Changing it later does not rename stored data.
- **`interval_min`**, **`interval_factor`**, **`interval_max`** (nanoseconds)
  define the HTA levels. They are part of the stored layout: changing them for
  an existing metric is rejected. New metrics can be added at runtime; removing
  or remapping inputs requires a restart.

Fields of the legacy database configuration (`mode`, `threads`, `type`,
`path`) are ignored, so an existing `metricq-db-hta` configuration can be used
unchanged under a new token (with distinct metric names, see
[Deployment](../operations/deployment.md)).

## HTA levels and records

Every metric has:

- **Level 0**: raw samples `(time, value)`.
- **Aggregate levels** `interval_min`, `interval_min × factor`, … up to
  `interval_max`. A record on level *L* summarizes one interval of length *L*:
  minimum, maximum, sum, count, integral and active time.

Samples must have strictly increasing timestamps per metric. Duplicates,
out-of-order, non-positive timestamps and non-finite values are dropped before
they reach the WAL, as in the legacy database. An aggregate interval is
completed — and its record emitted — when the first sample *after* the interval
arrives. The newest, still open interval is part of the in-memory HTA state,
not a record. Runs of identical empty intervals (gaps) are stored as one
record with a repeat count.

A stream is the unit of storage: each stream has its own time index, and each
data block holds records from exactly one stream.

The number of records depends on the rate and the configuration:

| Samples per metric | Records per sample, `interval_min` = 1 s | Records per sample, `interval_min` = sample interval |
| --- | ---: | ---: |
| 10/s | 1.1 | 2.1 |
| 1/s | 2.1 | 2.1 |
| 1/10 s | 4.1 | 2.1 |
| 1/min | 5.7 | 2.1 |
| 1/h | 9.4 | 2.1 |
| 1/day | 11.7 | 2.0 |

Sparse metrics with a small `interval_min` produce several records per sample
because every gap closes an interval and adds a run on each fine level.
Choosing `interval_min` close to the sample interval keeps it at about two.

## Query levels

`FLEX_TIMELINE` requests carry `interval_max`, typically the requested time
span divided by the display width. If it is below the metric's `interval_min`
the raw level answers; otherwise the largest aggregate level not exceeding it.
A response therefore has roughly between *N* and *N × factor* values for *N*
display points, independent of the time span, and reads one stream.

## Compatibility with metricq-db-hta

The database answers requests like the C++ `metricq-db-hta`; a parity test
compares thousands of response pairs of both implementations.

- Samples are filtered like the legacy database: non-positive, duplicate and
  out-of-order timestamps and NaN/infinity are skipped, before the WAL append.
  The first arrival at a timestamp wins, including AMQP redeliveries.
- Integrals use the *next* sample's value over the interval since the previous
  sample, in nanoseconds.
- Timelines keep extended left boundaries, partial initial buckets, absent
  final incomplete buckets, empty aggregates, negative-resolution requests,
  level fallback and the legacy `FLEX_TIMELINE` smoothing.
- The configuration key is the history name; `input` selects the incoming
  metric. The input binding is not part of the stored layout.
- The storage format is new; there is no reader or importer for legacy `.hta`
  files.
