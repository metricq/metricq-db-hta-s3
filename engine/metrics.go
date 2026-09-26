package engine

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	WALTarget, WALHigh, WALHard, WALPending, OldestWAL                                      prometheus.Gauge
	WALErrors                                                                               prometheus.Counter
	WALBytes, Pressure, Backpressure, BuilderBytes, Series, Checkpoint, Head, LastCommit    prometheus.Gauge
	Samples, Skipped, Replayed, Commits, CommitErrors, Objects, Bytes, Queries, QueryErrors prometheus.Counter
	Sync, Flush, Query, StoreGet, StorePut                                                  prometheus.Histogram
}

func NewMetrics(r prometheus.Registerer) *Metrics {
	gauge := func(name, help string) prometheus.Gauge {
		v := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "metricq_db", Name: name, Help: help})
		r.MustRegister(v)
		return v
	}
	counter := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "metricq_db", Name: name, Help: help})
		r.MustRegister(v)
		return v
	}
	hist := func(name, help string) prometheus.Histogram {
		v := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "metricq_db", Name: name, Help: help, Buckets: prometheus.ExponentialBuckets(.0001, 4, 10)})
		r.MustRegister(v)
		return v
	}
	return &Metrics{
		WALTarget:    gauge("wal_target_bytes", "WAL usage triggering an object-store checkpoint."),
		WALHigh:      gauge("wal_high_bytes", "WAL usage stopping further ingestion."),
		WALHard:      gauge("wal_hard_limit_bytes", "Maximum permitted WAL size."),
		WALPending:   gauge("wal_pending_frames", "Durable WAL frames not yet checkpointed in object storage."),
		OldestWAL:    gauge("wal_oldest_timestamp_seconds", "Receipt time of oldest uncheckpointed WAL frame, zero when empty."),
		WALErrors:    counter("wal_errors_total", "WAL write or fsync errors; restart is required."),
		WALBytes:     gauge("wal_bytes", "Current local WAL bytes."),
		Pressure:     gauge("wal_pressure_ratio", "WAL bytes divided by high watermark."),
		Backpressure: gauge("backpressure", "One while WAL high watermark blocks ingestion."),
		BuilderBytes: gauge("builder_bytes", "Estimated uncommitted record bytes."),
		Series:       gauge("series", "Configured metric count."),
		Checkpoint:   gauge("checkpoint_sequence", "Last durable object-store sequence."),
		Head:         gauge("wal_head_sequence", "Last fsynced WAL sequence."),
		LastCommit:   gauge("last_commit_timestamp_seconds", "Unix time of last successful object-store commit."),
		Samples:      counter("samples_total", "Accepted samples."),
		Skipped:      counter("samples_dropped_total", "Duplicate, out-of-order, nonpositive timestamp or nonfinite samples."),
		Replayed:     counter("wal_replayed_frames_total", "Replayed WAL frames after the object-store checkpoint."),
		Commits:      counter("commits_total", "Successful durable manifest commits."),
		CommitErrors: counter("commit_errors_total", "Failed object-store commits."),
		Objects:      counter("objects_written_total", "Successfully uploaded data objects including retried uploads."),
		Bytes:        counter("object_bytes_written_total", "Uploaded compressed data bytes."),
		Queries:      counter("queries_total", "History requests."),
		QueryErrors:  counter("query_errors_total", "Failed history requests."),
		Sync:         hist("wal_sync_seconds", "WAL write and fsync latency."),
		Flush:        hist("flush_seconds", "Object and manifest commit latency."),
		Query:        hist("query_seconds", "History request latency."),
		StoreGet:     hist("store_get_seconds", "Backend GET latency including failures."),
		StorePut:     hist("store_put_seconds", "Backend PUT latency including failures."),
	}
}
