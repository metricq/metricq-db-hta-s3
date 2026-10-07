import csv, statistics as st, collections, math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def load(f):
    g = collections.defaultdict(list)
    for r in csv.DictReader(open(f)):
        g[(r["type"], int(r["metrics"]), float(r["span_s"]))].append(r)
    return g

now = {"1 Sa/s": load("query-matrix-1sa.csv"), "100 Sa/s": load("query-matrix-100sa.csv")}
before = {"1 Sa/s": load("query-matrix-1sa-8c00bd2.csv"), "100 Sa/s": load("query-matrix-100sa-8c00bd2.csv")}
colors = {"1 Sa/s": "C0", "100 Sa/s": "C2"}
# Ilsche 2020, Figure 4.9, read from the figure's vector paths.
ilsche = collections.defaultdict(list)
for r in csv.DictReader(open("ilsche-2020-fig-4.9.csv")):
    ilsche[(r["type"], int(r["metrics"]), r["series"])].append(r)
fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharey=True)
panels = [("timeline", 1), ("timeline", 6), ("aggregate", 1), ("aggregate", 6)]
for ax, (typ, n) in zip(axes.flat, panels):
    for name, g in now.items():
        keys = sorted(k for k in g if k[0] == typ and k[1] == n)
        if not keys:
            continue
        xs = [k[2] for k in keys]
        means = [st.mean(float(r["latency_ms"]) for r in g[k]) for k in keys]
        ci = [1.96 * st.stdev(float(r["latency_ms"]) for r in g[k]) / math.sqrt(len(g[k])) for k in keys]
        db = [st.mean(float(r["db_max_ms"]) for r in g[k]) for k in keys]
        ax.errorbar(xs, means, yerr=ci, marker="o", ms=3, color=colors[name], label=f"{name}: end-to-end", capsize=2)
        ax.plot(xs, db, ls="--", color=colors[name], alpha=.7, label=f"{name}: database (max)")
        if typ == "aggregate":
            old = before[name]
            ax.plot(xs, [st.mean(float(r["latency_ms"]) for r in old[k]) for k in keys], ls=":", color="gray", label=f"{name}: end-to-end before index aggregates" if name == "1 Sa/s" else None)
    ref = ilsche[(typ, n, "end_to_end")]
    xs = [float(r["span_s"]) for r in ref]
    ax.plot(xs, [float(r["mean_ms"]) for r in ref], color="black", lw=1, label="Ilsche 2020 Fig. 4.9: end-to-end")
    ax.fill_between(xs, [float(r["ci_low_ms"]) for r in ref], [float(r["ci_high_ms"]) for r in ref], color="black", alpha=.15, lw=0)
    server = ilsche[(typ, n, "server_request")]
    ax.plot([float(r["span_s"]) for r in server], [float(r["mean_ms"]) for r in server], color="black", lw=1, ls="--", label="Ilsche 2020 Fig. 4.9: server request")
    ax.set_xscale("log")
    ax.set_title(f"{typ}, {'random single metric' if n == 1 else 'six metrics'}")
    ax.grid(alpha=.3)
for ax in axes[1]:
    ax.set_xlabel("Duration of queried time interval (s)")
for ax in axes[:, 0]:
    ax.set_ylabel("Query response latency (ms)")
axes[0][0].legend(fontsize=7, loc="upper left")
axes[1][0].legend(fontsize=7, loc="upper left")
fig.suptitle("metricq-db-hta-s3 dev database, 2026-10-07 (ae0054b), against Ilsche 2020 Fig. 4.9: mean ± 95 % CI of 20 random windows", fontsize=10)
fig.tight_layout()
fig.savefig("query-matrix.svg")
fig.savefig("/tmp/claude-1000/-home-mario-repos-metricq-metricq-db-hta-s3/8957a7b3-5182-42a0-939b-3329ce85623a/scratchpad/query-matrix2.png", dpi=100)
