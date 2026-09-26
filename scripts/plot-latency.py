#!/usr/bin/env python3
"""Plot the CSV emitted by TestRequestLatency as four PDF-style panels."""

import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


source = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/latency-baseline.csv")
target = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/latency-baseline.svg")
with source.open(newline="") as stream:
    rows = list(csv.DictReader(stream))

fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharex=True)
colors = {"file": "#1967a0", "s3": "#d1741b"}
for row_index, kind in enumerate(("timeline", "aggregate")):
    for column_index, metrics in enumerate((1, 6)):
        ax = axes[row_index, column_index]
        for backend in ("file", "s3"):
            positions_list = (100, 1000) if kind == "timeline" else (0,)
            for positions in positions_list:
                values = sorted(
                    (
                        row for row in rows
                        if row["backend"] == backend
                        and row["kind"] == kind
                        and int(row["metrics"]) == metrics
                        and int(row["positions"]) == positions
                    ),
                    key=lambda row: float(row["span_s"]),
                )
                ax.errorbar(
                    [float(row["span_s"]) for row in values],
                    [float(row["mean_ms"]) for row in values],
                    yerr=[float(row["ci95_ms"]) for row in values],
                    color=colors[backend],
                    linestyle="-" if positions != 1000 else "--",
                    marker="o" if positions != 1000 else "s",
                    capsize=2,
                    label=f"{backend.upper()}" + (f", {positions} positions" if positions else ""),
                )
        ax.set_xscale("log")
        ax.set_title(f"{kind.capitalize()} · {metrics} metric{'s' if metrics > 1 else ''}")
        ax.set_ylabel("Client end-to-end latency (ms)")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
        if row_index == 1:
            ax.set_xlabel("Requested time span (s)")

fig.suptitle("HTA file vs S3 — 20 paired random windows per point")
fig.tight_layout()
fig.savefig(target)
print(target)
