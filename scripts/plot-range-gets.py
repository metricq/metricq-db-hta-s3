#!/usr/bin/env python3
"""Plot mean S3 Range-GET counts from TestRequestLatency."""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


source = Path(sys.argv[1] if len(sys.argv) > 1 else "measurements/latency-scale.csv")
target = Path(sys.argv[2] if len(sys.argv) > 2 else "measurements/latency-scale-gets.svg")
with source.open(newline="") as stream:
    rows = [row for row in csv.DictReader(stream) if row["backend"] == "s3"]

fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True)
for row_index, kind in enumerate(("timeline", "aggregate")):
    for column_index, metrics in enumerate((1, 6)):
        ax = axes[row_index, column_index]
        positions_list = (100, 1000) if kind == "timeline" else (0,)
        for positions in positions_list:
            values = sorted(
                (
                    row
                    for row in rows
                    if row["kind"] == kind
                    and int(row["metrics"]) == metrics
                    and int(row["positions"]) == positions
                ),
                key=lambda row: float(row["span_s"]),
            )
            ax.plot(
                [float(row["span_s"]) for row in values],
                [float(row["mean_range_gets"]) for row in values],
                marker="o" if positions != 1000 else "s",
                markersize=3,
                label=f"{positions} positions" if positions else "aggregate",
            )
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(f"{kind.capitalize()} · {metrics} metric{'s' if metrics > 1 else ''}")
        ax.set_ylabel("Mean S3 Range GETs")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
        if row_index == 1:
            ax.set_xlabel("Requested time span (s)")

fig.suptitle("S3 request amplification across query duration")
fig.tight_layout()
fig.savefig(target)
print(target)
