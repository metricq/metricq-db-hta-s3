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
| `--token` | `METRICQ_TOKEN` | `db-hta-s3` | Client token; the manager's configuration document id. |
| `-v`, `--verbosity` | `METRICQ_VERBOSITY` | `warning` | `debug`, `info`, `warning`, `error` |
| `--metrics-listen` | `METRICQ_METRICS_LISTEN` | `127.0.0.1:9090` | Address of `/metrics` and `/readyz` |
| `--ingest-prefetch` | `METRICQ_INGEST_PREFETCH` | `400` | AMQP data prefetch = largest group-commit batch |
| `--wal-dir` | `METRICQ_WAL_DIR` | `/var/lib/metricq-db-hta-s3/wal` | WAL directory on durable local storage |
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

All keys are optional; unknown keys are rejected. Values in bytes are plain
integers. Option names start with the area they affect: `wal_`,
`checkpoint_`, `ingest_`, `hold_`, `query_`, `maintenance_` and
`compaction_` (then `cycle_`, `job_`, `output_`, `io_`, `merge_`, `reclaim_`,
`locality_`). The same names label `metricq_db_config{parameter=…}`.

```json
{
  "server": "amqp://admin:admin@localhost/",
  "token": "db-hta-s3",
  "metrics_listen": "127.0.0.1:9090",
  "pprof": false,
  "ingest_prefetch": 400,
  "s3": {
    "bucket": "metricq",
    "prefix": "db-hta-s3",
    "endpoint": "http://localhost:9000",
    "region": "us-east-1",
    "path_style": true
  },
  "engine": {
    "wal_directory": "./wal",
    "wal_target_bytes": 33554432,
    "wal_high_bytes": 67108864,
    "wal_hard_bytes": 83886080,
    "checkpoint_unsaved_bytes": 4194304,
    "checkpoint_append_only_aggregates": true,
    "ingest_memory_limit_bytes": 805306368,
    "hold_max_age_seconds": 3600,
    "hold_memory_bytes": 536870912,
    "hold_expiry_interval_seconds": 30,
    "query_max_response_bytes": 15728640,
    "query_memory_bytes": 536870912,
    "compaction_enabled": true,
    "compaction_continuous": false,
    "compaction_cycle_interval_seconds": 60,
    "compaction_cycle_max_seconds": 30,
    "compaction_job_timeout_seconds": 60,
    "compaction_job_max_blocks": 512,
    "compaction_job_max_bytes": 33554432,
    "compaction_output_object_bytes": 4194304,
    "compaction_io_bytes_per_second": 8388608,
    "compaction_merge_enabled": true,
    "compaction_merge_cooldown_seconds": 60,
    "compaction_reclaim_dead_fraction": 0.4,
    "compaction_locality_fan_in": 4,
    "compaction_locality_disabled": false
  }
}
```

### Connection

| Key | Flag | Meaning |
| --- | --- | --- |
| `server`, `token` | `--server`, `--token` | MetricQ connection |
| `metrics_listen` | `--metrics-listen` | Prometheus endpoint address |
| `pprof` | `--pprof` | Serve Go profiles under `/debug/pprof/` on the metrics address (default false; trusted networks only, see [monitoring](monitoring.md#profiling)) |
| `ingest_prefetch` | `--ingest-prefetch` | AMQP data prefetch |
| `s3.bucket`, `s3.prefix`, `s3.endpoint`, `s3.region`, `s3.path_style` | `--s3-*` | Object store location |

### Engine options

| Key | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `wal_directory` | `/var/lib/metricq-db-hta-s3/wal` (executable) | required | WAL directory (`--wal-dir`) |
| `wal_target_bytes` | 32 MiB | < `wal_high_bytes` | Active WAL segment size that triggers a checkpoint |
| `wal_high_bytes` | 64 MiB | < `wal_hard_bytes` | WAL size (all segments) at which ingestion is refused |
| `wal_hard_bytes` | 80 MiB | | Absolute WAL limit; a single delivery larger than this is rejected |
| `checkpoint_unsaved_bytes` | 4 MiB | ≤ `ingest_memory_limit_bytes` | Records only in the WAL (estimated) that trigger a checkpoint |
| `checkpoint_append_only_aggregates` | true (executable) | requires `compaction_enabled` and `compaction_merge_enabled` | Append new aggregate blocks instead of extending the last one |
| `ingest_memory_limit_bytes` | 32 MiB | | Memory limit for pending, held and uploading records; ingestion is refused above |
| `hold_max_age_seconds` | 3600 (executable), 0 (library) | ≥ 0 | Hold streams in memory until a full block or this age; 0 disables holding |
| `hold_memory_bytes` | ½ `ingest_memory_limit_bytes` | < `ingest_memory_limit_bytes` | Held records above this are written early, largest streams first |
| `hold_expiry_interval_seconds` | 30 | | Age-triggered checkpoints are grouped on this cadence |
| `query_max_response_bytes` | 15 MiB | ≥ 1 | Largest encoded history response. Keep it below RabbitMQ's `max_message_size` (16 MiB by default since RabbitMQ 4.0); larger messages are refused by the broker. About 1.2 million raw values or 300 000 aggregates fit into 15 MiB. |
| `query_memory_bytes` | 512 MiB | ≥ 1 | Decoded records of all running history queries together. A query reserves its need, known from the index, before reading; queries that do not fit wait, a query needing more than the whole budget fails. |
| `maintenance_enabled` | always on in the executable | | Catalog, compaction and GC |

### Compaction options

| Key | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `compaction_enabled` | true (executable) | | Run compaction |
| `compaction_continuous` | false | | Resume a remaining backlog immediately and wake on checkpoints; I/O budget and WAL pressure still apply |
| `compaction_cycle_interval_seconds` | 60 | ≥ 1 | Period of compaction cycles |
| `compaction_cycle_max_seconds` | 30 (executable), 10 | 1–3600 | Consecutive jobs are started within this window per cycle |
| `compaction_job_timeout_seconds` | 60 | 1–3600 | Timeout of one job |
| `compaction_job_max_blocks` | 512 (executable), 128 | 1–512 | Source blocks per job |
| `compaction_job_max_bytes` | 32 MiB | ≤ 64 MiB | Source bytes per job |
| `compaction_output_object_bytes` | 4 MiB | ≤ `compaction_job_max_bytes` | Size of output packs |
| `compaction_io_bytes_per_second` | 8 MiB | ≥ 1 | Rate limit for all maintenance reads and writes |
| `compaction_merge_enabled` | true (executable) | | Merge adjacent small blocks of a stream |
| `compaction_merge_cooldown_seconds` | 60 (executable), 0 | ≥ 0 | Objects younger than this are not merged |
| `compaction_reclaim_dead_fraction` | 0.4 | 0 < x < 1 | Objects with more dead bytes are evacuated |
| `compaction_locality_fan_in` | 4 | 2 – 64 | Number of contiguous sections of one size tier that locality merges into one section of the next tier, up to `compaction_output_object_bytes` |
| `compaction_locality_disabled` | false | | Turn off level locality |

What to change when is described in [Tuning](tuning.md).
