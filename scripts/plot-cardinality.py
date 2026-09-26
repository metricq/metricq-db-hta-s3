#!/usr/bin/env python3
"""Plot the summary and cache measurements of TestMetricCardinality."""
import csv
import sys
from collections import defaultdict
from statistics import mean
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

with Path(sys.argv[1]).open(newline='') as f:
    summaries = sorted(csv.DictReader(f), key=lambda r: int(r['metrics']))
with Path(sys.argv[2]).open(newline='') as f:
    queries = list(csv.DictReader(f))
counts = [int(r['metrics']) for r in summaries]
kind = sys.argv[4] if len(sys.argv) > 4 else 'timeline-100'
fig, axes = plt.subplots(2, 2, figsize=(11, 7))
ax = axes[0, 0]
ax.plot(counts, [float(r['mean_raw_records_per_block']) for r in summaries], marker='o')
ax.set_ylabel('Mean raw records per block')
ax = axes[0, 1]
ax.plot(counts, [float(r['s3_write_amplification']) for r in summaries], marker='o', label='All S3 PUT bytes / logical input')
ax.set_ylabel('S3 written bytes / input bytes')
ax.legend(fontsize=8)
ax = axes[1, 0]
ax.plot(counts, [100 * float(r['obsolete_data_bytes']) / float(r['data_put_bytes']) for r in summaries], marker='o')
ax.set_ylabel('Unreferenced data-pack bytes (%)')
ax.set_ylim(0, 100)
ax = axes[1, 1]
for mode in ('cold', 'sweep', 'hot'):
    grouped = defaultdict(list)
    for r in queries:
        if r['cache'] == mode and r['kind'] == kind and r['requested_metrics'] == '1':
            grouped[int(r['configured_metrics'])].append(float(r['latency_ms']))
    ax.plot(counts, [mean(grouped[n]) for n in counts], marker='o', label=mode)
ax.set_ylabel(f'{kind} · mean latency (ms)')
ax.set_yscale('log')
ax.legend(fontsize=8)
for ax in axes.flat:
    ax.set_xscale('log')
    ax.set_xlabel('Configured active metrics')
    ax.grid(alpha=0.25)
fig.suptitle('Metric cardinality with fixed buffer and cache limits')
fig.tight_layout()
fig.savefig(sys.argv[3])
print(sys.argv[3])
