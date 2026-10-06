# Monitoring

## Endpoint

The process serves Prometheus metrics at `http://<metrics-listen>/metrics`
and readiness at `/readyz` (200 once the engine is open). Every series carries
the label `token` with the database's MetricQ token, so one Prometheus can
scrape several databases:

```yaml
scrape_configs:
  - job_name: metricq-db-hta-s3
    static_configs:
      - targets: ["db-host:9090"]
```

Besides the `metricq_db_*` series listed below, the endpoint exports the Go
runtime and process collectors (`go_*`, `process_*`) and
`metricq_db_build_info{version, goversion}`.

## Dashboard

`grafana/metricq-db-hta-s3.json` is a Grafana dashboard with a *Prometheus*
data source variable and a *Token* variable
(`label_values(metricq_db_build_info, token)`). Import it via
*Dashboards → New → Import*, or provision it from a file. The JSON is
generated: change `grafana/generate_dashboard.py` and run it.

| Row | Answers |
| --- | --- |
| Overview | Is data flowing, is ingestion blocked, when was the last checkpoint? |
| Ingest and WAL | Batch sizes, fsync latency, WAL size against its three limits |
| Checkpoints | Why checkpoints run, how long they take, how many partial blocks they create |
| Holding | Held memory against its budget, age of the oldest held stream |
| Object store | Requests, bytes, errors and latency by operation and object kind |
| Storage and fragmentation | Live and dead bytes, small blocks, compaction candidates |
| Compaction | Jobs, errors, merge rate, throughput against its rate limit, locality |
| Reclamation | Objects awaiting deletion and deletions |
| Queries | Rate, errors, latency |
| Process | Memory, CPU, goroutines |

Every panel that relates to a limit draws the configured value
(`metricq_db_config{parameter=…}`) as a separate series, and panel
descriptions (ⓘ) point to the parameter to change. See [Tuning](tuning.md).

The development compose file starts Prometheus and Grafana with the dashboard
provisioned (`--profile monitoring`, see [Deployment](deployment.md)).

## Alerts

The rules live in `docker/monitoring/alerts.yml`, which the monitoring profile
of the development stack loads; `docker/monitoring/alerts_test.yml` checks
them with `promtool test rules`. Adjust durations to your checkpoint cadence.

- **MetricQDBUncleanRestart**: a clean shutdown checkpoints the WAL, so
  replayed frames at startup mean a crash, panic or kill. Check the log.
- **MetricQDBCompactionFailing**: jobs fail and none succeeds within an hour.
  A deterministic failure repeats on every cycle and stops all compaction,
  long before **MetricQDBCompactionStuck** fires.
- **MetricQDBCompactionJobPending**: an aborted job was not recovered; no new
  job can start.

```yaml
--8<-- "docker/monitoring/alerts.yml"
```

## Metric reference

This table is generated from `engine/metrics.go` by
`scripts/metrics-reference.py`. Histograms export `_bucket`, `_sum` and
`_count`. Counters include work replayed after a restart.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| **Ingest** | | | |
| `metricq_db_ingest_backpressure` | gauge |  | One while WAL high watermark blocks ingestion. |
| `metricq_db_ingest_backpressure_events_total` | counter |  | Ingest attempts refused because the WAL high watermark or ingest memory limit was reached. |
| `metricq_db_ingest_batch_deliveries` | histogram |  | AMQP deliveries made durable by one WAL fsync. |
| `metricq_db_ingest_batches_total` | counter |  | Group-committed delivery batches (one WAL fsync each). |
| `metricq_db_ingest_memory_bytes` | gauge |  | Estimated memory of records not yet in blocks (pending, held, uploading); limited by ingest_memory_limit_bytes. |
| `metricq_db_ingest_metrics` | gauge |  | Configured metric count. |
| `metricq_db_ingest_pending_records` | gauge |  | Aggregated records not yet in data blocks, including held and uploading records. |
| `metricq_db_ingest_samples_dropped_total` | counter |  | Duplicate, out-of-order, nonpositive timestamp or nonfinite samples. |
| `metricq_db_ingest_samples_total` | counter |  | Accepted samples. |
| **WAL** | | | |
| `metricq_db_wal_bytes` | gauge |  | Current local WAL bytes. |
| `metricq_db_wal_errors_total` | counter |  | WAL write or fsync errors; restart is required. |
| `metricq_db_wal_head_sequence` | gauge |  | Last fsynced WAL sequence. |
| `metricq_db_wal_oldest_timestamp_seconds` | gauge |  | Receipt time of oldest uncheckpointed WAL frame, zero when empty. |
| `metricq_db_wal_pending_frames` | gauge |  | Durable WAL frames not yet checkpointed in object storage. |
| `metricq_db_wal_pressure_ratio` | gauge |  | WAL bytes divided by high watermark. |
| `metricq_db_wal_replayed_frames_total` | counter |  | Replayed WAL frames after the object-store checkpoint. |
| `metricq_db_wal_segments` | gauge |  | Local WAL segment files, including the active one. |
| `metricq_db_wal_sync_seconds` | histogram |  | WAL write and fsync latency. |
| **Checkpoints** | | | |
| `metricq_db_checkpoint_blocks_total` | counter | size | Data blocks written by checkpoints; partial blocks (below 1024 records) are future compaction work. |
| `metricq_db_checkpoint_commits_total` | counter |  | Successful durable manifest commits. |
| `metricq_db_checkpoint_errors_total` | counter |  | Failed object-store commits. |
| `metricq_db_checkpoint_last_timestamp_seconds` | gauge |  | Unix time of last successful object-store commit. |
| `metricq_db_checkpoint_metadata_pages_total` | counter | kind | Immutable metadata pages encoded, including failed publication attempts. |
| `metricq_db_checkpoint_object_bytes_total` | counter |  | Uploaded compressed data bytes. |
| `metricq_db_checkpoint_objects_total` | counter |  | Successfully uploaded data objects including retried uploads. |
| `metricq_db_checkpoint_pinned_index_entries` | gauge |  | Index entries of rightmost paths kept in memory for checkpoints. |
| `metricq_db_checkpoint_records_total` | counter |  | Aggregated records written to data blocks by checkpoints. |
| `metricq_db_checkpoint_seconds` | histogram |  | Object and manifest commit latency. |
| `metricq_db_checkpoint_starts_total` | counter | reason | Checkpoints by trigger: wal, object_target, hold_age, hold_budget or explicit. |
| `metricq_db_checkpoint_unsaved_bytes` | gauge |  | Estimated bytes of records only in the WAL (neither in blocks nor in held deltas). |
| `metricq_db_checkpoint_wal_sequence` | gauge |  | Last durable object-store sequence. |
| **Holding** | | | |
| `metricq_db_hold_delta_bytes_total` | counter |  | Bytes of held/ delta objects written by checkpoints. |
| `metricq_db_hold_delta_records` | gauge |  | Held records persisted in held/ deltas. |
| `metricq_db_hold_deltas` | gauge |  | held/ delta objects referenced by the manifest. |
| `metricq_db_hold_oldest_age_seconds` | gauge |  | Age of the oldest held stream; streams are written at hold_max_age_seconds. |
| `metricq_db_hold_streams` | gauge |  | Streams (metric and HTA level) with records held in memory. |
| **Object store** | | | |
| `metricq_db_store_bytes_total` | counter | op, kind | Object store payload bytes by operation and object kind. |
| `metricq_db_store_errors_total` | counter | op, kind | Failed object store requests by operation and object kind. |
| `metricq_db_store_get_seconds` | histogram |  | Backend GET latency including failures. |
| `metricq_db_store_manifest_bytes` | gauge |  | Size of the last published manifest. |
| `metricq_db_store_put_seconds` | histogram |  | Backend PUT latency including failures. |
| `metricq_db_store_requests_total` | counter | op, kind | Object store requests by operation and object kind. |
| **Metadata cache** | | | |
| `metricq_db_metadata_cache_requests_total` | counter | kind, result | Decoded metadata cache lookups. |
| **Storage state** | | | |
| `metricq_db_storage_data_sections` | gauge |  | Contiguous runs of consecutive blocks of one stream within one object; range requests to read every stream in full. |
| `metricq_db_storage_dead_bytes` | gauge |  | Unreferenced bytes inside partially live data/index objects. |
| `metricq_db_storage_fragment_blocks` | gauge |  | Small data blocks followed by a newer block of their stream; compaction work. |
| `metricq_db_storage_fragmentation_ratio` | gauge |  | Data sections relative to one per stream plus one per compaction_output_object_bytes of data; about 1 is ideal, open tails add up to one per stream. |
| `metricq_db_storage_live_bytes` | gauge |  | Referenced compressed data/index bytes in the maintenance catalog. |
| `metricq_db_storage_objects` | gauge |  | Data and index objects tracked by the maintenance catalog. |
| `metricq_db_storage_small_block_bytes` | gauge |  | Compressed bytes in live data blocks below 1024 records. |
| `metricq_db_storage_small_blocks` | gauge |  | Live data blocks below 1024 records, including single stream tails. |
| `metricq_db_storage_streams` | gauge |  | Streams (metric and HTA level) with published data. |
| `metricq_db_storage_tail_blocks` | gauge |  | Partial blocks at the end of streams that together fit into one block: open tails, filled by later records or a deferred merge, not compaction work. |
| **Compaction** | | | |
| `metricq_db_compaction_active` | gauge |  | One while a background compaction job runs. |
| `metricq_db_compaction_candidate_objects` | gauge |  | Tracked candidate objects, including cooldown and single tails. |
| `metricq_db_compaction_conflicts_total` | counter |  | Metadata proposals rebuilt after concurrent publication. |
| `metricq_db_compaction_deferred_merges_total` | counter |  | Large tail merge candidates deferred until sufficient growth or age. |
| `metricq_db_compaction_input_blocks_total` | counter |  | Source blocks in successfully published jobs. |
| `metricq_db_compaction_io_read_bytes_total` | counter |  | Compressed data/index bytes read by compaction. |
| `metricq_db_compaction_io_write_bytes_total` | counter |  | Data/index pack bytes uploaded by compaction. |
| `metricq_db_compaction_job_budget_exceeded_total` | counter |  | Compaction jobs aborted because publication exceeded the catalog budget. |
| `metricq_db_compaction_job_errors_total` | counter |  | Failed background compaction jobs. |
| `metricq_db_compaction_job_last_success_timestamp_seconds` | gauge |  | Unix time of the last published compaction job. |
| `metricq_db_compaction_job_object_limit` | gauge |  | Current maximum source objects per compaction job (adapts to the catalog budget). |
| `metricq_db_compaction_job_pending` | gauge |  | One while a reserved or aborted compaction job is recorded in the manifest. |
| `metricq_db_compaction_job_seconds` | histogram |  | Background compaction job duration. |
| `metricq_db_compaction_jobs_total` | counter |  | Successfully published background compactions. |
| `metricq_db_compaction_locality_jobs_total` | counter |  | Published jobs packing consecutive blocks of a metric level. |
| `metricq_db_compaction_locality_pending_streams` | gauge |  | Changed stream roots awaiting layout inspection; not necessarily actionable fragmentation. |
| `metricq_db_compaction_noop_total` | counter |  | Candidate scans with no actionable job. |
| `metricq_db_compaction_output_blocks_total` | counter |  | Copied or consolidated source replacements in successfully published jobs. |
| `metricq_db_compaction_rechunked_blocks_total` | counter |  | Source blocks rewritten to move a fragment towards the end of its stream. |
| **Maintenance (deletion of retired objects)** | | | |
| `metricq_db_maintenance_delete_errors_total` | counter |  | Failed retired-object deletion attempts. |
| `metricq_db_maintenance_delete_pending_objects` | gauge |  | Fully retired objects awaiting deletion, including reader-pinned objects. |
| `metricq_db_maintenance_deleted_objects_total` | counter |  | Successfully deleted retired objects. |
| **Queries** | | | |
| `metricq_db_query_errors_total` | counter |  | Failed history requests. |
| `metricq_db_query_requests_total` | counter |  | History requests. |
| `metricq_db_query_seconds` | histogram |  | History request latency. |
| **Configuration** | | | |
| `metricq_db_config` | gauge | parameter | Configured engine option values; the parameter label names the option. |

## Checkpoint metadata pages

`metricq_db_checkpoint_metadata_pages_total{kind}` counts encoded immutable
pages for `state`, `roots`, `held-inventory` and `held-watermarks`. Failed
publication attempts are included; root descriptors/directories are excluded.
Inventory-only checkpoints should not increase `held-watermarks`. Use
`metricq_db_store_bytes_total{op="put",kind="held-state"}` and
`metricq_db_store_requests_total` for physical traffic, rather than estimating
it from page counts. Maintenance reuses unchanged held metadata.

## Compaction phases and caches

`metricq_db_compaction_job_seconds` now measures the whole attempt, including
selection and no-op attempts. `metricq_db_compaction_active` also covers
selection. Use `metricq_db_compaction_phase_seconds{phase}` to distinguish:
`select` (including reservation), `copy_merge`, `prepare`, `index`, `catalog`,
`publish_wait`, `publish_locked` and `pace`. The index/catalog phases are nested
inside preparation, which is nested inside the publication lock. Their sums
must not be added as independent costs. Phase observations for index/catalog
currently cover successful completion; a failed preparation remains visible in
`prepare` and the error counter.

`metricq_db_metadata_cache_requests_total{kind,result}` reports direct decoded
catalog/index cache hits and misses. Prefetched index misses and pinned/local
index hits are not included; physical store counters remain authoritative.
`metricq_db_compaction_deferred_merges_total` counts inspected large-tail merge
candidates deferred for insufficient growth, not distinct pending jobs.
