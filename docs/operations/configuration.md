# Configuration

Three sources, in increasing precedence:

1. **Defaults** of the executable.
2. **Configuration file** (`--config`, JSON), mainly for tuning options.
3. **Environment variables** `METRICQ_*`, also read from a `.metricq` file in
   the working directory or home directory (`KEY=value` lines, existing
   variables win).
4. **Command-line flags.** Both `-flag` and `--flag` work.

The metric definitions are not configured here: they come from the MetricQ
manager ([Deployment](deployment.md#2-register-the-database-with-the-manager)).

## Command line and environment

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--server` | `METRICQ_SERVER` | — (required) | AMQP URL of the MetricQ RabbitMQ. `$USER` and `$HOST` are replaced. |
| `--token` | `METRICQ_TOKEN` | `db-hta-go` | Client token; the manager's configuration document id. |
| `-v`, `--verbosity` | `METRICQ_VERBOSITY` | `warning` | `debug`, `info`, `warning`, `error` |
| `--metrics-listen` | `METRICQ_METRICS_LISTEN` | `127.0.0.1:9090` | Address of `/metrics` and `/readyz` |
| `--prefetch` | `METRICQ_PREFETCH` | `100` | AMQP data prefetch = largest group-commit batch |
| `--wal-dir` | `METRICQ_WAL_DIR` | `/var/lib/metricq-db-hta-go/wal` | WAL directory on durable local storage |
| `--s3-bucket` | `METRICQ_S3_BUCKET` | — (required) | Bucket |
| `--s3-prefix` | `METRICQ_S3_PREFIX` | empty | Key prefix of this database (exclusive) |
| `--s3-endpoint` | `METRICQ_S3_ENDPOINT` | AWS | Endpoint URL, `http(s)://host[:port]` |
| `--s3-region` | `METRICQ_S3_REGION` | `us-east-1` | Region |
| `--s3-path-style` | `METRICQ_S3_PATH_STYLE` | `false` | Path-style addressing (most non-AWS endpoints) |
| `--config` | `METRICQ_CONFIG` | none | JSON configuration file |
| `--version` | | | Print version and exit |

S3 credentials come from the AWS SDK chain (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, …). Logs go to standard error.

!!! warning "The WAL is bound to the storage location"
    The WAL directory records endpoint, region, bucket and prefix. Changing any
    of them — including the spelling of the endpoint URL — makes startup refuse
    the WAL. Stop the database cleanly (final checkpoint) before moving it.

## Configuration file

All keys are optional. Values in bytes are plain integers.

```json
{
  "server": "amqp://user:pass@rabbitmq/",
  "token": "db-hta-go",
  "listen": "127.0.0.1:9090",
  "prefetch": 100,
  "s3": {"bucket": "metricq", "prefix": "db-hta-go", "endpoint": "https://s3.example.org", "region": "us-east-1", "path_style": true},
  "engine": {
    "wal_directory": "/var/lib/metricq-db-hta-go/wal",
    "wal_target_bytes": 33554432,
    "wal_high_bytes": 67108864,
    "wal_hard_bytes": 83886080,
    "object_target_bytes": 4194304,
    "builder_hard_bytes": 805306368,
    "hold_seconds": 3600,
    "hold_bytes": 536870912,
    "hold_expiry_batch_seconds": 30,
    "max_query_rows": 1000000,
    "append_only_aggregates": true,
    "compaction": {
      "enabled": true,
      "merge_small_blocks": true,
      "interval_seconds": 60,
      "max_cycle_seconds": 30,
      "max_duration_seconds": 60,
      "cooldown_seconds": 60,
      "max_blocks": 512,
      "max_job_bytes": 33554432,
      "object_bytes": 4194304,
      "bytes_per_second": 8388608,
      "dead_fraction": 0.4,
      "locality_min_ranges": 4,
      "disable_locality": false
    }
  }
}
```

### Engine options

| Key | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `wal_target_bytes` | 32 MiB | < `wal_high_bytes` | Active WAL segment size that triggers a checkpoint |
| `wal_high_bytes` | 64 MiB | < `wal_hard_bytes` | WAL size (all segments) at which ingestion is refused |
| `wal_hard_bytes` | 80 MiB | | Absolute WAL limit; a single delivery larger than this is rejected |
| `object_target_bytes` | 4 MiB | ≤ `builder_hard_bytes` | Records only in the WAL (estimated) that trigger a checkpoint |
| `builder_hard_bytes` | 32 MiB | | Memory limit for pending, held and uploading records; ingestion is refused above |
| `hold_seconds` | 3600 (executable), 0 (library) | ≥ 0 | Hold streams in memory until a full block or this age; 0 disables holding |
| `hold_bytes` | ½ `builder_hard_bytes` | < `builder_hard_bytes` | Held records above this are written early, largest streams first |
| `hold_expiry_batch_seconds` | 30 | | Age-triggered checkpoints are grouped on this cadence |
| `max_query_rows` | 1 000 000 | ≥ 1 | Largest history response |
| `append_only_aggregates` | true (executable) | requires compaction with merging | Append new aggregate blocks instead of extending the last one |
| `background_maintenance` | always on in the executable | | Catalog, compaction and GC |

### Compaction options

| Key | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `enabled` | true (executable) | | Run compaction |
| `merge_small_blocks` | true (executable) | | Merge adjacent small blocks of a stream |
| `interval_seconds` | 60 | ≥ 1 | Period of compaction cycles |
| `max_cycle_seconds` | 30 (executable), 10 | 1–3600 | Consecutive jobs are started within this window per cycle |
| `max_duration_seconds` | 60 | 1–3600 | Timeout of one job |
| `cooldown_seconds` | 60 (executable), 0 | ≥ 0 | Objects younger than this are not merged |
| `max_blocks` | 512 (executable), 128 | 1–512 | Source blocks per job |
| `max_job_bytes` | 32 MiB | ≤ 64 MiB | Source bytes per job |
| `object_bytes` | 4 MiB | ≤ `max_job_bytes` | Size of output packs |
| `bytes_per_second` | 8 MiB | ≥ 1 | Rate limit for all maintenance reads and writes |
| `dead_fraction` | 0.4 | 0 < x < 1 | Objects with more dead bytes are evacuated |
| `locality_min_ranges` | 4 | | Lay out a metric level contiguously when it spans at least this many ranges |
| `disable_locality` | false | | Turn off level locality |

What to change when is described in [Tuning](tuning.md).
