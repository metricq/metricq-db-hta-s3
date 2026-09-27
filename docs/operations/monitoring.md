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

Suggested rules; adjust durations to your checkpoint cadence.

```yaml
groups:
  - name: metricq-db-hta-s3
    rules:
      - alert: MetricQDBBackpressure
        expr: max by (token) (metricq_db_backpressure) == 1
        for: 5m
        annotations:
          summary: "{{ $labels.token }} refuses deliveries (WAL or ingest memory limit)"
      - alert: MetricQDBCheckpointsFailing
        expr: increase(metricq_db_commit_errors_total[15m]) > 0 and increase(metricq_db_commits_total[15m]) == 0
        annotations:
          summary: "{{ $labels.token }} cannot publish checkpoints; data accumulates in the WAL"
      - alert: MetricQDBNoCheckpoint
        expr: time() - metricq_db_last_commit_timestamp_seconds > 2 * 3600 and rate(metricq_db_samples_total[15m]) > 0
        annotations:
          summary: "{{ $labels.token }} has not checkpointed for two hours"
      - alert: MetricQDBWALError
        expr: increase(metricq_db_wal_errors_total[5m]) > 0
        annotations:
          summary: "{{ $labels.token }} WAL write failed; the process must be restarted"
      - alert: MetricQDBCompactionStuck
        expr: time() - metricq_db_compaction_last_success_timestamp_seconds > 6 * 3600 and metricq_db_compaction_candidate_objects > 0
        annotations:
          summary: "{{ $labels.token }} compaction has not published a job for six hours"
      - alert: MetricQDBFragmentationGrowing
        expr: deriv(metricq_db_small_data_blocks[6h]) > 0 and delta(metricq_db_small_data_blocks[24h]) > 0.2 * metricq_db_small_data_blocks
        annotations:
          summary: "{{ $labels.token }} small blocks grow faster than compaction merges them"
      - alert: MetricQDBObjectStoreErrors
        expr: sum by (token) (rate(metricq_db_store_errors_total[10m])) > 0.1
        for: 10m
        annotations:
          summary: "{{ $labels.token }} object store requests fail"
```

## Metric reference

This table is generated from `engine/metrics.go` by
`scripts/metrics-reference.py`. Histograms export `_bucket`, `_sum` and
`_count`. Counters include work replayed after a restart.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| **Ingest** | | | |
| `metricq_db_backpressure` | gauge |  | One while WAL high watermark blocks ingestion. |
| `metricq_db_backpressure_events_total` | counter |  | Ingest attempts refused because the WAL high watermark or ingest memory limit was reached. |
| `metricq_db_ingest_batch_deliveries` | histogram |  | AMQP deliveries made durable by one WAL fsync. |
| `metricq_db_ingest_batches_total` | counter |  | Group-committed delivery batches (one WAL fsync each). |
| `metricq_db_samples_dropped_total` | counter |  | Duplicate, out-of-order, nonpositive timestamp or nonfinite samples. |
| `metricq_db_samples_total` | counter |  | Accepted samples. |
| `metricq_db_series` | gauge |  | Configured metric count. |
| **WAL** | | | |
| `metricq_db_wal_bytes` | gauge |  | Current local WAL bytes. |
| `metricq_db_wal_errors_total` | counter |  | WAL write or fsync errors; restart is required. |
| `metricq_db_wal_hard_limit_bytes` | gauge |  | Maximum permitted WAL size. |
| `metricq_db_wal_head_sequence` | gauge |  | Last fsynced WAL sequence. |
| `metricq_db_wal_high_bytes` | gauge |  | WAL usage stopping further ingestion. |
| `metricq_db_wal_oldest_timestamp_seconds` | gauge |  | Receipt time of oldest uncheckpointed WAL frame, zero when empty. |
| `metricq_db_wal_pending_frames` | gauge |  | Durable WAL frames not yet checkpointed in object storage. |
| `metricq_db_wal_pressure_ratio` | gauge |  | WAL bytes divided by high watermark. |
| `metricq_db_wal_replayed_frames_total` | counter |  | Replayed WAL frames after the object-store checkpoint. |
| `metricq_db_wal_segments` | gauge |  | Local WAL segment files, including the active one. |
| `metricq_db_wal_sync_seconds` | histogram |  | WAL write and fsync latency. |
| `metricq_db_wal_target_bytes` | gauge |  | WAL usage triggering an object-store checkpoint. |
| **Checkpoints and holding** | | | |
| `metricq_db_builder_bytes` | gauge |  | Estimated memory of records not yet in blocks (pending, held, uploading); limited by ingest_memory_limit_bytes. |
| `metricq_db_checkpoint_blocks_total` | counter | size | Data blocks written by checkpoints; partial blocks (below 1024 records) are future compaction work. |
| `metricq_db_checkpoint_records_total` | counter |  | Aggregated records written to data blocks by checkpoints. |
| `metricq_db_checkpoint_sequence` | gauge |  | Last durable object-store sequence. |
| `metricq_db_checkpoints_total` | counter | reason | Checkpoints by trigger: wal, object_target, hold_age, hold_budget or explicit. |
| `metricq_db_commit_errors_total` | counter |  | Failed object-store commits. |
| `metricq_db_commits_total` | counter |  | Successful durable manifest commits. |
| `metricq_db_flush_seconds` | histogram |  | Object and manifest commit latency. |
| `metricq_db_held_covered_records` | gauge |  | Held records persisted in held/ deltas. |
| `metricq_db_held_delta_bytes_total` | counter |  | Bytes of held/ delta objects written by checkpoints. |
| `metricq_db_held_deltas` | gauge |  | held/ delta objects referenced by the manifest. |
| `metricq_db_held_oldest_age_seconds` | gauge |  | Age of the oldest held stream; streams are written at hold_max_age_seconds. |
| `metricq_db_held_streams` | gauge |  | Streams (metric and HTA level) with records held in memory. |
| `metricq_db_index_pinned_entries` | gauge |  | Index entries of rightmost paths kept in memory for checkpoints. |
| `metricq_db_last_commit_timestamp_seconds` | gauge |  | Unix time of last successful object-store commit. |
| `metricq_db_manifest_bytes` | gauge |  | Size of the last published manifest. |
| `metricq_db_object_bytes_written_total` | counter |  | Uploaded compressed data bytes. |
| `metricq_db_objects_written_total` | counter |  | Successfully uploaded data objects including retried uploads. |
| `metricq_db_pending_records` | gauge |  | Aggregated records not yet in data blocks, including held and uploading records. |
| `metricq_db_unsaved_bytes` | gauge |  | Estimated bytes of records only in the WAL (neither in blocks nor in held deltas). |
| **Object store** | | | |
| `metricq_db_store_bytes_total` | counter | op, kind | Object store payload bytes by operation and object kind. |
| `metricq_db_store_errors_total` | counter | op, kind | Failed object store requests by operation and object kind. |
| `metricq_db_store_get_seconds` | histogram |  | Backend GET latency including failures. |
| `metricq_db_store_put_seconds` | histogram |  | Backend PUT latency including failures. |
| `metricq_db_store_requests_total` | counter | op, kind | Object store requests by operation and object kind. |
| **Storage state** | | | |
| `metricq_db_dead_object_bytes` | gauge |  | Unreferenced bytes inside partially live data/index objects. |
| `metricq_db_gc_delete_errors_total` | counter |  | Failed retired-object deletion attempts. |
| `metricq_db_gc_deleted_objects_total` | counter |  | Successfully deleted retired objects. |
| `metricq_db_gc_pending_objects` | gauge |  | Fully retired objects awaiting deletion, including reader-pinned objects. |
| `metricq_db_live_object_bytes` | gauge |  | Referenced compressed data/index bytes in the maintenance catalog. |
| `metricq_db_live_objects` | gauge |  | Data and index objects tracked by the maintenance catalog. |
| `metricq_db_small_data_block_bytes` | gauge |  | Compressed bytes in live data blocks below 1024 records. |
| `metricq_db_small_data_blocks` | gauge |  | Live data blocks below 1024 records, including single stream tails. |
| **Compaction** | | | |
| `metricq_db_compaction_active` | gauge |  | One while a background compaction job runs. |
| `metricq_db_compaction_budget_exceeded_total` | counter |  | Compaction jobs aborted because publication exceeded the catalog budget. |
| `metricq_db_compaction_candidate_objects` | gauge |  | Tracked candidate objects, including cooldown and single tails. |
| `metricq_db_compaction_conflicts_total` | counter |  | Metadata proposals rebuilt after concurrent publication. |
| `metricq_db_compaction_errors_total` | counter |  | Failed background compaction jobs. |
| `metricq_db_compaction_input_blocks_total` | counter |  | Source blocks in successfully published jobs. |
| `metricq_db_compaction_job_pending` | gauge |  | One while a reserved or aborted compaction job is recorded in the manifest. |
| `metricq_db_compaction_last_success_timestamp_seconds` | gauge |  | Unix time of the last published compaction job. |
| `metricq_db_compaction_locality_jobs_total` | counter |  | Published jobs packing consecutive blocks of a metric level. |
| `metricq_db_compaction_locality_pending_streams` | gauge |  | Changed stream roots awaiting layout inspection; not necessarily actionable fragmentation. |
| `metricq_db_compaction_noop_total` | counter |  | Candidate scans with no actionable job. |
| `metricq_db_compaction_object_limit` | gauge |  | Current maximum source objects per compaction job (adapts to the catalog budget). |
| `metricq_db_compaction_output_blocks_total` | counter |  | Copied or consolidated source replacements in successfully published jobs. |
| `metricq_db_compaction_read_bytes_total` | counter |  | Compressed data/index bytes read by compaction. |
| `metricq_db_compaction_seconds` | histogram |  | Background compaction job duration. |
| `metricq_db_compaction_write_bytes_total` | counter |  | Data/index pack bytes uploaded by compaction. |
| `metricq_db_compactions_total` | counter |  | Successfully published background compactions. |
| **Queries** | | | |
| `metricq_db_queries_total` | counter |  | History requests. |
| `metricq_db_query_errors_total` | counter |  | Failed history requests. |
| `metricq_db_query_seconds` | histogram |  | History request latency. |
| **Configuration** | | | |
| `metricq_db_config` | gauge | parameter | Configured engine option values; the parameter label names the option. |
