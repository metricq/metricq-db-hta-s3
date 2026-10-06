#!/usr/bin/env python3
"""Generate grafana/metricq-db-hta-s3.json.

Edit this script, not the JSON: python3 grafana/generate_dashboard.py
Every query selects one database through the token label that the executable
adds to all of its Prometheus series.
"""
import json
import pathlib

T = '{token="$token"}'
panels = []
y = 0


def sel(extra=""):
    return '{token="$token"' + ("," + extra if extra else "") + "}"


def row(title):
    global y
    panels.append({"type": "row", "title": title, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def panel(title, targets, unit="short", kind="timeseries", w=8, h=8, x=None,
          description="", stack=False, thresholds=None, min_zero=True, text_mode="value"):
    global y
    if x is None:
        x = panel.x
    if x + w > 24:
        x = 0
        y += panel.h
    p = {
        "type": kind, "title": title, "description": description,
        "datasource": {"type": "prometheus", "uid": "${datasource}"},
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [{"expr": e, "legendFormat": l, "refId": chr(65 + i),
                     "datasource": {"type": "prometheus", "uid": "${datasource}"}}
                    for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {},
    }
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"fillOpacity": 10, "showPoints": "never",
                                                  "stacking": {"mode": "normal" if stack else "none"}}
        if min_zero:
            p["fieldConfig"]["defaults"]["min"] = 0
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "multi", "sort": "desc"}}
    if kind == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value",
                        "graphMode": "area", "textMode": text_mode}
        # Without explicit steps Grafana colours values above 80 red.
        thresholds = thresholds or [{"color": "text", "value": None}]
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
    panels.append(p)
    panel.x = x + w
    panel.h = h
    if panel.x >= 24:
        panel.x = 0
        y += h


panel.x = 0
panel.h = 0


def newrow(title):
    global y
    if panel.x:
        y += panel.h
    panel.x = 0
    row(title)


def cfg(name):
    return f'max(metricq_db_config{sel(f"parameter=\"{name}\"")})'


GREEN, AMBER, RED = "green", "orange", "red"

newrow("Overview")
panel("Version", [(f"metricq_db_build_info{T}", "{{version}}")], kind="stat", w=4, h=4, text_mode="name",
      description="Build version of the database.")
panel("Metrics", [(f"metricq_db_ingest_metrics{T}", "")], kind="stat", w=4, h=4,
      description="Metrics configured by the MetricQ manager.")
panel("Samples/s", [(f"sum(rate(metricq_db_ingest_samples_total{T}[$__rate_interval]))", "")], "short", kind="stat", w=4, h=4)
panel("Backpressure", [(f"max(metricq_db_ingest_backpressure{T})", "")], kind="stat", w=4, h=4,
      thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}],
      description="1 while the WAL high watermark or ingest memory limit refuses deliveries. See Operations → Tuning.")
panel("Last checkpoint", [(f"time() - (max(metricq_db_checkpoint_last_timestamp_seconds{T}) > 0)", "")], "s", kind="stat", w=4, h=4,
      thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 600}, {"color": RED, "value": 3600}],
      description="Seconds since the last published checkpoint (manifest).")
panel("Compaction job pending", [(f"max(metricq_db_compaction_job_pending{T})", "")], kind="stat", w=4, h=4,
      thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 1}],
      description="1 while a reserved or aborted job is recorded; an aborted job blocks compaction for about a minute.")

newrow("Ingest and WAL")
panel("Samples", [(f"sum(rate(metricq_db_ingest_samples_total{T}[$__rate_interval]))", "accepted"),
                  (f"sum(rate(metricq_db_ingest_samples_dropped_total{T}[$__rate_interval]))", "dropped")], "short",
      description="Accepted and dropped (duplicate, out-of-order, non-finite) samples per second.")
panel("Deliveries per fsync", [
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_ingest_batch_deliveries_bucket{T}[$__rate_interval])))", "p50"),
    (f"histogram_quantile(0.95, sum by (le) (rate(metricq_db_ingest_batch_deliveries_bucket{T}[$__rate_interval])))", "p95"),
    (cfg("ingest_prefetch"), "ingest_prefetch")], "short",
    description="Group commit: AMQP deliveries made durable by one WAL fsync. Bounded by prefetch.")
panel("WAL fsync latency", [
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_wal_sync_seconds_bucket{T}[$__rate_interval])))", "p50"),
    (f"histogram_quantile(0.99, sum by (le) (rate(metricq_db_wal_sync_seconds_bucket{T}[$__rate_interval])))", "p99")], "s")
panel("WAL size", [(f"max(metricq_db_wal_bytes{T})", "WAL"), (cfg("wal_target_bytes"), "target (checkpoint)"),
                   (cfg("wal_high_bytes"), "high (backpressure)"), (cfg("wal_hard_bytes"), "hard")], "bytes",
      description="Local WAL bytes not yet covered by a checkpoint, with the limits that act on it.")
panel("WAL segments and pending frames", [(f"max(metricq_db_wal_segments{T})", "segments"),
                                          (f"max(metricq_db_wal_pending_frames{T})", "frames")], "short",
      description="More than a few segments means checkpoints fail or cannot keep up.")
panel("Backpressure events", [(f"sum(rate(metricq_db_ingest_backpressure_events_total{T}[$__rate_interval]))", "refused")], "short",
      description="Ingest attempts refused by WAL high watermark or ingest memory limit; each triggers an inline checkpoint.")

newrow("Checkpoints")
panel("Checkpoints by trigger", [(f"sum by (reason) (rate(metricq_db_checkpoint_starts_total{T}[$__rate_interval]))", "{{reason}}")],
      "short", stack=True,
      description="wal/object_target: size thresholds; hold_age/hold_budget: holding; explicit: backpressure or shutdown.")
panel("Checkpoint duration", [
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_checkpoint_seconds_bucket{T}[$__rate_interval])))", "p50"),
    (f"histogram_quantile(0.95, sum by (le) (rate(metricq_db_checkpoint_seconds_bucket{T}[$__rate_interval])))", "p95")], "s")
panel("Blocks written", [(f"sum by (size) (rate(metricq_db_checkpoint_blocks_total{T}[$__rate_interval]))", "{{size}}")],
      "short", stack=True,
      description="Partial blocks (below 1024 records) become compaction work. Holding keeps this low.")
panel("Unsaved bytes", [(f"max(metricq_db_checkpoint_unsaved_bytes{T})", "unsaved"), (cfg("checkpoint_unsaved_bytes"), "object target")], "bytes",
      description="Records only in the WAL. A checkpoint starts at the object target.")
panel("Checkpoint errors", [(f"sum(rate(metricq_db_checkpoint_errors_total{T}[$__rate_interval]))", "errors")], "short")
panel("Records written", [(f"sum(rate(metricq_db_checkpoint_records_total{T}[$__rate_interval]))", "records")], "short")

newrow("Holding")
panel("Held memory", [(f"max(metricq_db_ingest_memory_bytes{T})", "held and pending"), (cfg("hold_memory_bytes"), "hold budget"),
                      (cfg("ingest_memory_limit_bytes"), "ingest memory limit")], "bytes",
      description="Above the hold budget the largest streams are written early (partial blocks).")
panel("Oldest held stream", [(f"max(metricq_db_hold_oldest_age_seconds{T})", "age"), (cfg("hold_max_age_seconds"), "hold limit")], "s")
panel("Held streams and records", [(f"max(metricq_db_hold_streams{T})", "streams"),
                                   (f"max(metricq_db_ingest_pending_records{T})", "records"),
                                   (f"max(metricq_db_hold_delta_records{T})", "in deltas")], "short")
panel("Held deltas", [(f"max(metricq_db_hold_deltas{T})", "objects"),
                      (f"sum(rate(metricq_db_hold_delta_bytes_total{T}[$__rate_interval]))", "bytes/s")], "short")

newrow("Object store")
panel("Requests by kind", [(f"sum by (op, kind) (rate(metricq_db_store_requests_total{T}[$__rate_interval]))", "{{op}} {{kind}}")],
      "reqps", w=12, stack=True)
panel("Bytes by kind", [(f"sum by (op, kind) (rate(metricq_db_store_bytes_total{T}[$__rate_interval]))", "{{op}} {{kind}}")],
      "Bps", w=12, stack=True)
panel("Request latency", [
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_store_get_seconds_bucket{T}[$__rate_interval])))", "GET p50"),
    (f"histogram_quantile(0.99, sum by (le) (rate(metricq_db_store_get_seconds_bucket{T}[$__rate_interval])))", "GET p99"),
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_store_put_seconds_bucket{T}[$__rate_interval])))", "PUT p50"),
    (f"histogram_quantile(0.99, sum by (le) (rate(metricq_db_store_put_seconds_bucket{T}[$__rate_interval])))", "PUT p99")], "s")
panel("Errors", [(f"sum by (op, kind) (rate(metricq_db_store_errors_total{T}[$__rate_interval]))", "{{op}} {{kind}}")], "reqps")
panel("Manifest size", [(f"max(metricq_db_store_manifest_bytes{T})", "manifest")], "bytes",
      description="Written with every checkpoint and maintenance publication.")

newrow("Storage and fragmentation")
panel("Stored bytes", [(f"max(metricq_db_storage_live_bytes{T})", "live"), (f"max(metricq_db_storage_dead_bytes{T})", "dead")],
      "bytes", stack=True,
      description="Dead bytes are unreferenced parts of partly live objects; compaction reclaims them above compaction_reclaim_dead_fraction.")
panel("Objects", [(f"max(metricq_db_storage_objects{T})", "live objects"), (f"max(metricq_db_maintenance_delete_pending_objects{T})", "awaiting deletion")], "short")
panel("Fragmentation", [(f"max(metricq_db_storage_fragment_blocks{T})", "fragments (compaction work)"),
                        (f"max(metricq_db_storage_tail_blocks{T})", "open stream tails"),
                        (f"max(metricq_db_compaction_candidate_objects{T})", "candidate objects")], "short",
      description="Fragments are small blocks inside a stream and should return towards zero; a steady rise means compaction falls behind. "
                  "Open tails are the newest partial block of each stream and only fill with new records.")
panel("Small block share", [(f"max(metricq_db_storage_small_block_bytes{T}) / max(metricq_db_storage_live_bytes{T})", "small / live")],
      "percentunit")
panel("Stream locality", [(f"max(metricq_db_storage_fragmentation_ratio{T})", "fragmentation ratio"),
                          (f"max(metricq_db_storage_data_sections{T}) / max(metricq_db_storage_streams{T})", "sections per stream")], "short",
      description="Contiguous sections are runs of one metric level in one object, each read with one range request. "
                  "The ratio relates them to one per stream plus one per compaction_output_object_bytes of data: about 1 is ideal, "
                  "open tails add up to one per stream. Sections per stream grow with history, the ratio should not.")

newrow("Compaction")
panel("Jobs", [(f"sum(rate(metricq_db_compaction_jobs_total{T}[$__rate_interval]))", "published"),
               (f"sum(rate(metricq_db_compaction_job_errors_total{T}[$__rate_interval]))", "failed"),
               (f"sum(rate(metricq_db_compaction_job_budget_exceeded_total{T}[$__rate_interval]))", "catalog budget"),
               (f"sum(rate(metricq_db_compaction_noop_total{T}[$__rate_interval]))", "no work")], "short")
panel("Blocks in and out", [(f"sum(rate(metricq_db_compaction_input_blocks_total{T}[$__rate_interval]))", "input"),
                            (f"sum(rate(metricq_db_compaction_output_blocks_total{T}[$__rate_interval]))", "output")], "short",
      description="Input minus output is the rate at which fragments are merged away.")
panel("Throughput", [(f"sum(rate(metricq_db_compaction_io_read_bytes_total{T}[$__rate_interval]))", "read"),
                     (f"sum(rate(metricq_db_compaction_io_write_bytes_total{T}[$__rate_interval]))", "write"),
                     (cfg("compaction_io_bytes_per_second"), "rate limit")], "Bps")
panel("Job duration", [
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_compaction_job_seconds_bucket{T}[$__rate_interval])))", "p50"),
    (f"histogram_quantile(0.95, sum by (le) (rate(metricq_db_compaction_job_seconds_bucket{T}[$__rate_interval])))", "p95"),
    (cfg("compaction_job_timeout_seconds"), "timeout")], "s")
panel("Active and source-object limit", [(f"max(metricq_db_compaction_active{T})", "active"),
                                         (f"max(metricq_db_compaction_job_object_limit{T})", "object limit")], "short")
panel("Level locality", [(f"sum(rate(metricq_db_compaction_locality_jobs_total{T}[$__rate_interval]))", "locality jobs/s"),
                         (f"max(metricq_db_compaction_locality_pending_streams{T})", "streams to inspect")], "short",
      description="Jobs laying out consecutive blocks of a metric level contiguously (fewer GETs per FLEX query).")
panel("Last successful job", [(f"time() - (max(metricq_db_compaction_job_last_success_timestamp_seconds{T}) > 0)", "age")], "s")

newrow("Reclamation")
panel("Objects awaiting deletion", [(f"max(metricq_db_maintenance_delete_pending_objects{T})", "pending")], "short")
panel("Deletions", [(f"sum(rate(metricq_db_maintenance_deleted_objects_total{T}[$__rate_interval]))", "deleted"),
                    (f"sum(rate(metricq_db_maintenance_delete_errors_total{T}[$__rate_interval]))", "errors")], "short")

newrow("Queries")
panel("Requests", [(f"sum(rate(metricq_db_query_requests_total{T}[$__rate_interval]))", "queries"),
                   (f"sum(rate(metricq_db_query_errors_total{T}[$__rate_interval]))", "errors")], "reqps")
panel("Latency", [
    (f"histogram_quantile(0.5, sum by (le) (rate(metricq_db_query_seconds_bucket{T}[$__rate_interval])))", "p50"),
    (f"histogram_quantile(0.95, sum by (le) (rate(metricq_db_query_seconds_bucket{T}[$__rate_interval])))", "p95"),
    (f"histogram_quantile(0.99, sum by (le) (rate(metricq_db_query_seconds_bucket{T}[$__rate_interval])))", "p99")], "s")
panel("Pinned index entries", [(f"max(metricq_db_checkpoint_pinned_index_entries{T})", "entries")], "short",
      description="Rightmost index paths kept for checkpoints (bounded).")

newrow("Process")
panel("Memory", [(f"max(process_resident_memory_bytes{T})", "resident"), (f"max(go_memstats_heap_inuse_bytes{T})", "heap in use")], "bytes")
panel("CPU", [(f"rate(process_cpu_seconds_total{T}[$__rate_interval])", "cores")], "short")
panel("Goroutines and files", [(f"max(go_goroutines{T})", "goroutines"), (f"max(process_open_fds{T})", "open files")], "short")

dashboard = {
    "uid": "metricq-db-hta-s3",
    "title": "MetricQ HTA database (S3)",
    "description": "State of one metricq-db-hta-s3 database, selected by its MetricQ token.",
    "tags": ["metricq", "database"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "30s",
    "time": {"from": "now-6h", "to": "now"},
    "templating": {"list": [
        {"name": "datasource", "label": "Prometheus", "type": "datasource", "query": "prometheus",
         "current": {}, "hide": 0},
        {"name": "token", "label": "Token", "type": "query",
         "datasource": {"type": "prometheus", "uid": "${datasource}"},
         "query": {"query": "label_values(metricq_db_build_info, token)", "refId": "token"},
         "definition": "label_values(metricq_db_build_info, token)", "refresh": 2, "sort": 1,
         "current": {}, "hide": 0, "includeAll": False, "multi": False},
    ]},
    "panels": panels,
}
out = pathlib.Path(__file__).with_name("metricq-db-hta-s3.json")
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} with {sum(p['type'] != 'row' for p in panels)} panels")
