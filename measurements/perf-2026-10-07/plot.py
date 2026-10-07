import csv, statistics as st, collections, math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def load(f):
    g = collections.defaultdict(list)
    for r in csv.DictReader(open(f)):
        g[(r["type"], int(r["metrics"]), float(r["span_s"]))].append(r)
    return g

load_g, dummy_g = load("query-matrix-1sa.csv"), load("query-matrix-100sa.csv")
fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharey=True)
panels = [("timeline", 1), ("timeline", 6), ("aggregate", 1), ("aggregate", 6)]
for ax, (typ, n) in zip(axes.flat, panels):
    for g, label, color in [(load_g, "1 Sa/s metrics: end-to-end", "C0"), (dummy_g, "100 Sa/s metric: end-to-end", "C2")]:
        keys = sorted(k for k in g if k[0] == typ and k[1] == n)
        if not keys:
            continue
        xs = [k[2] for k in keys]
        means = [st.mean(float(r["latency_ms"]) for r in g[k]) for k in keys]
        ci = [1.96 * st.stdev(float(r["latency_ms"]) for r in g[k]) / math.sqrt(len(g[k])) for k in keys]
        db = [st.mean(float(r["db_max_ms"]) for r in g[k]) for k in keys]
        ax.errorbar(xs, means, yerr=ci, marker="o", ms=3, color=color, label=label, capsize=2)
        ax.plot(xs, db, ls="--", color=color, alpha=.7, label=label.split(":")[0] + ": database (max)")
    limit = 30 if typ == "timeline" else 8
    ax.axhline(limit, color="red", lw=1, ls=":", label=f"Ilsche 2020 Fig. 4.9: all < {limit} ms")
    ax.set_xscale("log")
    ax.set_title(f"{typ}, {'random single metric' if n == 1 else 'six metrics'}")
    ax.grid(alpha=.3)
for ax in axes[1]:
    ax.set_xlabel("Duration of queried time interval (s)")
for ax in axes[:, 0]:
    ax.set_ylabel("Query response latency (ms)")
axes[0][0].legend(fontsize=7, loc="upper left")
fig.suptitle("metricq-db-hta-s3 dev database, 2026-10-07: mean ± 95 % CI of 20 random windows")
fig.tight_layout()
fig.savefig("query-matrix.svg")
fig.savefig("query-matrix.png", dpi=110)
