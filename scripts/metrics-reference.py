#!/usr/bin/env python3
"""Print the Prometheus metric reference table for docs/operations/monitoring.md.

Reads the metric definitions from engine/metrics.go, so the table follows the
code. Usage: python3 scripts/metrics-reference.py > /tmp/table.md
"""
import pathlib
import re

source = pathlib.Path(__file__).resolve().parent.parent / "engine" / "metrics.go"
text = source.read_text()
kinds = {"gauge": "gauge", "counter": "counter", "hist": "histogram", "gaugeVec": "gauge", "counterVec": "counter"}
rows = []
for kind, name, help_text, labels in re.findall(r'(gaugeVec|counterVec|gauge|counter|hist)\("([a-z_]+)", "([^"]*)"((?:, "[a-z]+")*)\)', text):
    label_list = ", ".join(re.findall(r'"([a-z]+)"', labels))
    rows.append((name, kinds[kind], label_list, help_text))
if "ingest_batch_deliveries" in text:
    rows.append(("ingest_batch_deliveries", "histogram", "", "AMQP deliveries made durable by one WAL fsync."))
groups = [
    ("Ingest", ("ingest_",)),
    ("WAL", ("wal_",)),
    ("Checkpoints", ("checkpoint_",)),
    ("Holding", ("hold_",)),
    ("Object store", ("store_",)),
    ("Metadata cache", ("metadata_",)),
    ("Storage state", ("storage_",)),
    ("Compaction", ("compaction_",)),
    ("Maintenance (deletion of retired objects)", ("maintenance_",)),
    ("Queries", ("query_",)),
    ("Configuration", ("config",)),
]
used = set()
print("| Metric | Type | Labels | Meaning |\n| --- | --- | --- | --- |")
for title, prefixes in groups:
    print(f"| **{title}** | | | |")
    for name, kind, labels, help_text in sorted(rows):
        if name in used or not name.startswith(prefixes):
            continue
        used.add(name)
        print(f"| `metricq_db_{name}` | {kind} | {labels} | {help_text} |")
missing = [r[0] for r in rows if r[0] not in used]
if missing:
    raise SystemExit(f"ungrouped metrics: {missing}")
