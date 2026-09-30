package engine

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the engine's Prometheus collectors. The executable registers
// them with a token label; docs/operations/monitoring.md lists them.
type Metrics struct {
	CompactionDeferredMerges                                                                prometheus.Counter
	CompactionPhases                                                                        *prometheus.HistogramVec
	MetadataCache                                                                           *prometheus.CounterVec
	CandidateObjects, SmallBlocks, SmallBlockBytes                                          prometheus.Gauge
	TailBlocks, FragmentBlocks                                                              prometheus.Gauge
	CompactionRechunkedBlocks                                                               prometheus.Counter
	CompactionInputBlocks, CompactionOutputBlocks, CompactionNoop                           prometheus.Counter
	CompactionActive                                                                        prometheus.Gauge
	CompactionDuration                                                                      prometheus.Histogram
	LiveObjectBytes, DeadObjectBytes                                                        prometheus.Gauge
	CompactionReadBytes, CompactionWriteBytes                                               prometheus.Counter
	Compactions, CompactionErrors, CompactionConflicts                                      prometheus.Counter
	GCPending                                                                               prometheus.Gauge
	GCDeleted, GCErrors                                                                     prometheus.Counter
	WALPending, OldestWAL                                                                   prometheus.Gauge
	WALErrors                                                                               prometheus.Counter
	WALBytes, Pressure, Backpressure, BuilderBytes, Series, Checkpoint, Head, LastCommit    prometheus.Gauge
	Samples, Skipped, Replayed, Commits, CommitErrors, Objects, Bytes, Queries, QueryErrors prometheus.Counter
	Sync, Flush, Query, StoreGet, StorePut                                                  prometheus.Histogram

	// Configured limits, labelled by option name, so dashboards can relate
	// usage to the parameter that bounds it.
	Config *prometheus.GaugeVec

	MetadataPages      *prometheus.CounterVec // kind; encoded pages, including failed attempts
	IngestBatches      prometheus.Counter
	IngestBatchSize    prometheus.Histogram
	BackpressureEvents prometheus.Counter

	PendingRecords, UnsavedBytes                               prometheus.Gauge
	HeldStreams, HeldCoveredRecords, HeldDeltas, HeldOldestAge prometheus.Gauge

	FlushReasons   *prometheus.CounterVec // reason
	FlushBlocks    *prometheus.CounterVec // size: full, partial
	FlushRecords   prometheus.Counter
	HeldDeltaBytes prometheus.Counter
	ManifestBytes  prometheus.Gauge
	WALSegments    prometheus.Gauge
	PinnedEntries  prometheus.Gauge

	// Object store traffic by operation (get, range, put, delete) and object
	// kind (key prefix: data, index, held, catalog, candidates, trash, jobs,
	// manifest).
	StoreRequests, StoreBytes, StoreErrors *prometheus.CounterVec

	LiveObjects                                 prometheus.Gauge
	CompactionObjectLimit, CompactionJobPending prometheus.Gauge
	CompactionLastSuccess                       prometheus.Gauge
	CompactionBudgetExceeded                    prometheus.Counter
	LocalityJobs                                prometheus.Counter
	LocalityPendingStreams                      prometheus.Gauge
}

// NewMetrics creates and registers all engine metrics with r.
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
	gaugeVec := func(name, help string, labels ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "metricq_db", Name: name, Help: help}, labels)
		r.MustRegister(v)
		return v
	}
	counterVec := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "metricq_db", Name: name, Help: help}, labels)
		r.MustRegister(v)
		return v
	}
	batchSize := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "metricq_db", Name: "ingest_batch_deliveries", Help: "AMQP deliveries made durable by one WAL fsync.", Buckets: prometheus.ExponentialBuckets(1, 2, 13)})
	r.MustRegister(batchSize)
	phases := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "metricq_db", Name: "compaction_phase_seconds", Help: "Compaction phase durations; nested phases overlap. Selection includes no-op attempts.", Buckets: prometheus.ExponentialBuckets(.0001, 4, 11)}, []string{"phase"})
	r.MustRegister(phases)
	return &Metrics{
		CompactionDeferredMerges:  counter("compaction_deferred_merges_total", "Large tail merge candidates deferred until sufficient growth or age."),
		CompactionPhases:          phases,
		MetadataCache:             counterVec("metadata_cache_requests_total", "Decoded metadata cache lookups.", "kind", "result"),
		Config:                    gaugeVec("config", "Configured engine option values; the parameter label names the option.", "parameter"),
		IngestBatches:             counter("ingest_batches_total", "Group-committed delivery batches (one WAL fsync each)."),
		IngestBatchSize:           batchSize,
		BackpressureEvents:        counter("ingest_backpressure_events_total", "Ingest attempts refused because the WAL high watermark or ingest memory limit was reached."),
		PendingRecords:            gauge("ingest_pending_records", "Aggregated records not yet in data blocks, including held and uploading records."),
		UnsavedBytes:              gauge("checkpoint_unsaved_bytes", "Estimated bytes of records only in the WAL (neither in blocks nor in held deltas)."),
		HeldStreams:               gauge("hold_streams", "Streams (metric and HTA level) with records held in memory."),
		HeldCoveredRecords:        gauge("hold_delta_records", "Held records persisted in held/ deltas."),
		HeldDeltas:                gauge("hold_deltas", "held/ delta objects referenced by the manifest."),
		HeldOldestAge:             gauge("hold_oldest_age_seconds", "Age of the oldest held stream; streams are written at hold_max_age_seconds."),
		FlushReasons:              counterVec("checkpoint_starts_total", "Checkpoints by trigger: wal, object_target, hold_age, hold_budget or explicit.", "reason"),
		FlushBlocks:               counterVec("checkpoint_blocks_total", "Data blocks written by checkpoints; partial blocks (below 1024 records) are future compaction work.", "size"),
		FlushRecords:              counter("checkpoint_records_total", "Aggregated records written to data blocks by checkpoints."),
		MetadataPages:             counterVec("checkpoint_metadata_pages_total", "Immutable metadata pages encoded, including failed publication attempts.", "kind"),
		HeldDeltaBytes:            counter("hold_delta_bytes_total", "Bytes of held/ delta objects written by checkpoints."),
		ManifestBytes:             gauge("store_manifest_bytes", "Size of the last published manifest."),
		WALSegments:               gauge("wal_segments", "Local WAL segment files, including the active one."),
		PinnedEntries:             gauge("checkpoint_pinned_index_entries", "Index entries of rightmost paths kept in memory for checkpoints."),
		StoreRequests:             counterVec("store_requests_total", "Object store requests by operation and object kind.", "op", "kind"),
		StoreBytes:                counterVec("store_bytes_total", "Object store payload bytes by operation and object kind.", "op", "kind"),
		StoreErrors:               counterVec("store_errors_total", "Failed object store requests by operation and object kind.", "op", "kind"),
		LiveObjects:               gauge("storage_objects", "Data and index objects tracked by the maintenance catalog."),
		CompactionObjectLimit:     gauge("compaction_job_object_limit", "Current maximum source objects per compaction job (adapts to the catalog budget)."),
		CompactionJobPending:      gauge("compaction_job_pending", "One while a reserved or aborted compaction job is recorded in the manifest."),
		CompactionLastSuccess:     gauge("compaction_job_last_success_timestamp_seconds", "Unix time of the last published compaction job."),
		LocalityJobs:              counter("compaction_locality_jobs_total", "Published jobs packing consecutive blocks of a metric level."),
		LocalityPendingStreams:    gauge("compaction_locality_pending_streams", "Changed stream roots awaiting layout inspection; not necessarily actionable fragmentation."),
		CompactionBudgetExceeded:  counter("compaction_job_budget_exceeded_total", "Compaction jobs aborted because publication exceeded the catalog budget."),
		CandidateObjects:          gauge("compaction_candidate_objects", "Tracked candidate objects, including cooldown and single tails."),
		SmallBlocks:               gauge("storage_small_blocks", "Live data blocks below 1024 records, including single stream tails."),
		SmallBlockBytes:           gauge("storage_small_block_bytes", "Compressed bytes in live data blocks below 1024 records."),
		TailBlocks:                gauge("storage_tail_blocks", "Streams whose newest data block is below 1024 records: open tails, filled by later records, not compaction work."),
		FragmentBlocks:            gauge("storage_fragment_blocks", "Small data blocks followed by a newer block of their stream; compaction work."),
		CompactionRechunkedBlocks: counter("compaction_rechunked_blocks_total", "Source blocks rewritten to move a fragment towards the end of its stream."),
		CompactionInputBlocks:     counter("compaction_input_blocks_total", "Source blocks in successfully published jobs."),
		CompactionOutputBlocks:    counter("compaction_output_blocks_total", "Copied or consolidated source replacements in successfully published jobs."),
		CompactionNoop:            counter("compaction_noop_total", "Candidate scans with no actionable job."),
		CompactionActive:          gauge("compaction_active", "One while a background compaction job runs."),
		CompactionDuration:        hist("compaction_job_seconds", "Background compaction job duration."),
		CompactionConflicts:       counter("compaction_conflicts_total", "Metadata proposals rebuilt after concurrent publication."),
		LiveObjectBytes:           gauge("storage_live_bytes", "Referenced compressed data/index bytes in the maintenance catalog."),
		DeadObjectBytes:           gauge("storage_dead_bytes", "Unreferenced bytes inside partially live data/index objects."),
		CompactionReadBytes:       counter("compaction_io_read_bytes_total", "Compressed data/index bytes read by compaction."),
		CompactionWriteBytes:      counter("compaction_io_write_bytes_total", "Data/index pack bytes uploaded by compaction."),
		Compactions:               counter("compaction_jobs_total", "Successfully published background compactions."),
		CompactionErrors:          counter("compaction_job_errors_total", "Failed background compaction jobs."),
		GCPending:                 gauge("maintenance_delete_pending_objects", "Fully retired objects awaiting deletion, including reader-pinned objects."),
		GCDeleted:                 counter("maintenance_deleted_objects_total", "Successfully deleted retired objects."),
		GCErrors:                  counter("maintenance_delete_errors_total", "Failed retired-object deletion attempts."),
		WALPending:                gauge("wal_pending_frames", "Durable WAL frames not yet checkpointed in object storage."),
		OldestWAL:                 gauge("wal_oldest_timestamp_seconds", "Receipt time of oldest uncheckpointed WAL frame, zero when empty."),
		WALErrors:                 counter("wal_errors_total", "WAL write or fsync errors; restart is required."),
		WALBytes:                  gauge("wal_bytes", "Current local WAL bytes."),
		Pressure:                  gauge("wal_pressure_ratio", "WAL bytes divided by high watermark."),
		Backpressure:              gauge("ingest_backpressure", "One while WAL high watermark blocks ingestion."),
		BuilderBytes:              gauge("ingest_memory_bytes", "Estimated memory of records not yet in blocks (pending, held, uploading); limited by ingest_memory_limit_bytes."),
		Series:                    gauge("ingest_metrics", "Configured metric count."),
		Checkpoint:                gauge("checkpoint_wal_sequence", "Last durable object-store sequence."),
		Head:                      gauge("wal_head_sequence", "Last fsynced WAL sequence."),
		LastCommit:                gauge("checkpoint_last_timestamp_seconds", "Unix time of last successful object-store commit."),
		Samples:                   counter("ingest_samples_total", "Accepted samples."),
		Skipped:                   counter("ingest_samples_dropped_total", "Duplicate, out-of-order, nonpositive timestamp or nonfinite samples."),
		Replayed:                  counter("wal_replayed_frames_total", "Replayed WAL frames after the object-store checkpoint."),
		Commits:                   counter("checkpoint_commits_total", "Successful durable manifest commits."),
		CommitErrors:              counter("checkpoint_errors_total", "Failed object-store commits."),
		Objects:                   counter("checkpoint_objects_total", "Successfully uploaded data objects including retried uploads."),
		Bytes:                     counter("checkpoint_object_bytes_total", "Uploaded compressed data bytes."),
		Queries:                   counter("query_requests_total", "History requests."),
		QueryErrors:               counter("query_errors_total", "Failed history requests."),
		Sync:                      hist("wal_sync_seconds", "WAL write and fsync latency."),
		Flush:                     hist("checkpoint_seconds", "Object and manifest commit latency."),
		Query:                     hist("query_seconds", "History request latency."),
		StoreGet:                  hist("store_get_seconds", "Backend GET latency including failures."),
		StorePut:                  hist("store_put_seconds", "Backend PUT latency including failures."),
	}
}

// observeStore counts one object store request by operation and object kind.
func (m *Metrics) observeStore(op, key string, n int, err error) {
	if m == nil || m.StoreRequests == nil {
		return
	}
	kind, _, _ := strings.Cut(key, "/")
	m.StoreRequests.WithLabelValues(op, kind).Inc()
	m.StoreBytes.WithLabelValues(op, kind).Add(float64(n))
	if err != nil {
		m.StoreErrors.WithLabelValues(op, kind).Inc()
	}
}

func (m *Metrics) phaseTimer(phase string) func() {
	start := time.Now()
	return func() { m.CompactionPhases.WithLabelValues(phase).Observe(time.Since(start).Seconds()) }
}
