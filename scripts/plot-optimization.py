#!/usr/bin/env python3
"""Compare matching S3 latency benchmark CSVs before and after optimization."""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load(path):
    with Path(path).open(newline="") as stream:
        return {
            (r["kind"], int(r["metrics"]), int(r["positions"]), float(r["span_s"])): r
            for r in csv.DictReader(stream)
            if r["backend"] == "s3"
        }


before = load(sys.argv[1])
after = load(sys.argv[2])
if before.keys() != after.keys():
    raise SystemExit("Benchmark configurations differ; refusing incomplete comparison")
target = Path(sys.argv[3])
fig, axes = plt.subplots(2, 3, figsize=(14, 7), sharex=True)
conditions = (("timeline", 1, 100), ("timeline", 6, 1000), ("aggregate", 6, 0))
for column, condition in enumerate(conditions):
    keys = sorted(k for k in before if k[:3] == condition)
    for rows, label, color in ((before, "Before", "#b45309"), (after, "After", "#0369a1")):
        for row, field in enumerate(("mean_ms", "mean_range_gets")):
            ax = axes[row, column]
            ax.plot([k[3] for k in keys], [float(rows[k][field]) for k in keys],
                    label=label, color=color, marker="o", markersize=3)
    kind, metrics, positions = condition
    title = f"{kind.capitalize()} · {metrics} metric{'s' if metrics > 1 else ''}"
    if positions:
        title += f" · {positions} positions"
    axes[0, column].set_title(title)
    for row in range(2):
        ax = axes[row, column]
        ax.set_xscale("log")
        # Warm data-cache hits can legitimately make a cell's GET count zero.
        if row == 0:
            ax.set_yscale("log")
        else:
            ax.set_yscale("symlog", linthresh=1)
            ax.set_ylim(bottom=0)
        ax.set_ylabel("Mean latency (ms)" if row == 0 else "Mean S3 Range GETs")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
    axes[1, column].set_xlabel("Requested time span (s)")

fig.suptitle("Go/S3 before and after index, aggregation and query optimizations")
fig.tight_layout()
fig.savefig(target)
print(target)
