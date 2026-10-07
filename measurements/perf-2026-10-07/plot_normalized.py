# Same matrix with the span in units of 1000 × interval_min: where the
# requested 1000 intervals reach interval_min a timeline switches from raw
# values to the first aggregate level, so all data sets switch levels at the
# same positions (Ilsche 2020: interval_min = 30/r = 30 ms at 1 kSa/s).
import csv, statistics as st, collections, math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def load(f):
    g = collections.defaultdict(list)
    for r in csv.DictReader(open(f)):
        g[(r["type"], int(r["metrics"]), float(r["span_s"]))].append(r)
    return g

sets = [("1 Sa/s, interval_min 40 s", "query-matrix-1sa.csv", 40.0, "C0"),
        ("100 Sa/s, interval_min 0.4 s", "query-matrix-100sa.csv", 0.4, "C2"),
        ("1 kSa/s, interval_min 30 ms (dissertation parameters)", "query-matrix-1ksa.csv", 0.03, "C1")]
ilsche = collections.defaultdict(list)
for r in csv.DictReader(open("ilsche-2020-fig-4.9.csv")):
    ilsche[(r["type"], int(r["metrics"]), r["series"])].append(r)
fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharey=True)
for ax, (typ, n) in zip(axes.flat, [("timeline", 1), ("timeline", 6), ("aggregate", 1), ("aggregate", 6)]):
    for name, f, imin, color in sets:
        try:
            g = load(f)
        except FileNotFoundError:
            continue
        keys = sorted(k for k in g if k[0] == typ and k[1] == n)
        if not keys:
            continue
        xs = [k[2] / (1000 * imin) for k in keys]
        means = [st.mean(float(r["latency_ms"]) for r in g[k]) for k in keys]
        ci = [1.96 * st.stdev(float(r["latency_ms"]) for r in g[k]) / math.sqrt(len(g[k])) for k in keys]
        ax.errorbar(xs, means, yerr=ci, marker="o", ms=3, color=color, label=name, capsize=2)
    ref = ilsche[(typ, n, "end_to_end")]
    xs = [float(r["span_s"]) / 30 for r in ref]
    ax.plot(xs, [float(r["mean_ms"]) for r in ref], color="red", lw=1, label="Ilsche 2020 Fig. 4.9: end-to-end")
    ax.fill_between(xs, [float(r["ci_low_ms"]) for r in ref], [float(r["ci_high_ms"]) for r in ref], color="red", alpha=.15, lw=0)
    ax.set_xscale("log")
    ax.set_xlim(1e-4, 1e4)
    ax.set_title(f"{typ}, {'random single metric' if n == 1 else 'six metrics'}")
    ax.grid(alpha=.3)
for ax in axes[1]:
    ax.set_xlabel("Queried span / (1000 × interval_min)")
for ax in axes[:, 0]:
    ax.set_ylabel("End-to-end latency (ms)")
axes[0][0].legend(fontsize=7, loc="upper left")
fig.suptitle("Query latency over the span normalised by interval_min: mean ± 95 % CI of 20 random windows", fontsize=10)
fig.tight_layout()
fig.savefig("query-matrix-normalized.svg")
fig.savefig("/tmp/claude-1000/-home-mario-repos-metricq-metricq-db-hta-s3/8957a7b3-5182-42a0-939b-3329ce85623a/scratchpad/norm.png", dpi=100)
