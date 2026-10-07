# Six metrics at 1 kSa/s with interval_min 30 ms and factor 10, the
# parameters of Ilsche 2020 §4.3.5, against the measured values of Fig. 4.9.
import csv, statistics as st, collections, math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

g = collections.defaultdict(list)
for r in csv.DictReader(open("query-matrix-1ksa.csv")):
    g[(r["type"], int(r["metrics"]), float(r["span_s"]))].append(r)
ilsche = collections.defaultdict(list)
for r in csv.DictReader(open("ilsche-2020-fig-4.9.csv")):
    ilsche[(r["type"], int(r["metrics"]), r["series"])].append(r)
fig, axes = plt.subplots(2, 2, figsize=(11, 7), sharey=True)
for ax, (typ, n) in zip(axes.flat, [("timeline", 1), ("timeline", 6), ("aggregate", 1), ("aggregate", 6)]):
    keys = sorted(k for k in g if k[0] == typ and k[1] == n)
    xs = [k[2] for k in keys]
    means = [st.mean(float(r["latency_ms"]) for r in g[k]) for k in keys]
    ci = [1.96 * st.stdev(float(r["latency_ms"]) for r in g[k]) / math.sqrt(len(g[k])) for k in keys]
    ax.errorbar(xs, means, yerr=ci, marker="o", ms=3, color="C1", capsize=2, label="metricq-db-hta-s3: end-to-end")
    ax.plot(xs, [st.mean(float(r["db_max_ms"]) for r in g[k]) for k in keys], color="C1", ls="--", label="metricq-db-hta-s3: database (max)")
    ref = ilsche[(typ, n, "end_to_end")]
    rx = [float(r["span_s"]) for r in ref]
    ax.plot(rx, [float(r["mean_ms"]) for r in ref], color="red", lw=1, label="Ilsche 2020 Fig. 4.9: end-to-end")
    ax.fill_between(rx, [float(r["ci_low_ms"]) for r in ref], [float(r["ci_high_ms"]) for r in ref], color="red", alpha=.15, lw=0)
    server = ilsche[(typ, n, "server_request")]
    ax.plot([float(r["span_s"]) for r in server], [float(r["mean_ms"]) for r in server], color="red", lw=1, ls="--", label="Ilsche 2020 Fig. 4.9: server request")
    ax.set_xscale("log")
    ax.set_xlim(0.7, 1e4)
    ax.set_title(f"{typ}, {'random single metric' if n == 1 else 'six metrics'}")
    ax.grid(alpha=.3)
for ax in axes[1]:
    ax.set_xlabel("Duration of queried time interval (s)")
for ax in axes[:, 0]:
    ax.set_ylabel("Query response latency (ms)")
axes[0][0].legend(fontsize=7, loc="upper right")
fig.suptitle("Dissertation parameters (six metrics, 1 kSa/s, interval_min 30 ms, factor 10, 2 h history): mean ± 95 % CI of 20 random windows", fontsize=9)
fig.tight_layout()
fig.savefig("query-matrix-1ksa.svg")
fig.savefig("/tmp/claude-1000/-home-mario-repos-metricq-metricq-db-hta-s3/8957a7b3-5182-42a0-939b-3329ce85623a/scratchpad/k1.png", dpi=100)
