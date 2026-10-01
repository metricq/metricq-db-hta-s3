package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
)

// ErrPressure reports that the WAL high watermark or the builder memory limit
// refuses further deliveries. The caller should run Flush and retry; the
// deliveries not processed must not be acknowledged.
var ErrPressure = errors.New("WAL or builder high watermark reached")

// pendingRecordBytes estimates builder memory per unflushed record: an 80-byte
// hta.Record in its stream slice plus append growth slack. Records no longer
// carry their metric name.
const pendingRecordBytes = 96

// Options configures an Engine. Zero values select the defaults documented in
// docs/operations/configuration.md; Open validates the combination.
type Options struct {
	// WALDirectory holds the WAL segments; it must be on durable local storage.
	WALDirectory string `json:"wal_directory"`
	// WALTarget is the active segment size that triggers a checkpoint.
	WALTarget int64 `json:"wal_target_bytes"`
	// WALHigh is the total WAL size above which deliveries are refused.
	WALHigh int64 `json:"wal_high_bytes"`
	// WALHard is the absolute WAL limit; larger deliveries are rejected.
	WALHard int64 `json:"wal_hard_bytes"`

	// CheckpointUnsavedBytes is the estimated size of records only in the WAL
	// (neither in blocks nor in held deltas) that triggers a checkpoint.
	CheckpointUnsavedBytes int64 `json:"checkpoint_unsaved_bytes"`
	// CheckpointAppendOnlyAggregates appends new aggregate blocks at
	// checkpoints instead of extending the last partial block; compaction
	// merges them later.
	CheckpointAppendOnlyAggregates bool `json:"checkpoint_append_only_aggregates"`

	// IngestMemoryLimitBytes bounds the estimated memory of all records not yet
	// in blocks (pending, held, uploading); deliveries are refused above it.
	IngestMemoryLimitBytes int64 `json:"ingest_memory_limit_bytes"`

	// HoldMaxAgeSeconds > 0 keeps streams in memory until they fill a block or
	// their oldest record reaches this age; held records persist in held/ deltas.
	HoldMaxAgeSeconds int64 `json:"hold_max_age_seconds"`
	// HoldMemoryBytes bounds held records (estimated); above it the largest
	// streams are written early. Zero means half of IngestMemoryLimitBytes.
	HoldMemoryBytes int64 `json:"hold_memory_bytes"`
	// HoldExpiryIntervalSeconds groups age-triggered checkpoints; explicit
	// Flush and memory pressure remain immediate.
	HoldExpiryIntervalSeconds int64 `json:"hold_expiry_interval_seconds"`

	// QueryMaxRows is the largest number of rows in one history response.
	QueryMaxRows int `json:"query_max_rows"`

	// MaintenanceEnabled enables the catalog, compaction and the trash journal
	// (RunMaintenance); required for holding.
	MaintenanceEnabled bool `json:"maintenance_enabled"`
	// CompactionOptions is embedded so its compaction_* options appear at the
	// same level in JSON.
	CompactionOptions
}

func (o Options) defaults() Options {
	o.CompactionOptions = o.CompactionOptions.defaults()
	if o.HoldExpiryIntervalSeconds == 0 {
		o.HoldExpiryIntervalSeconds = 30
	}
	if o.WALTarget == 0 {
		o.WALTarget = 32 << 20
	}
	if o.WALHigh == 0 {
		o.WALHigh = 64 << 20
	}
	if o.WALHard == 0 {
		o.WALHard = 80 << 20
	}
	if o.CheckpointUnsavedBytes == 0 {
		o.CheckpointUnsavedBytes = 4 << 20
	}
	if o.IngestMemoryLimitBytes == 0 {
		o.IngestMemoryLimitBytes = 32 << 20
	}
	if o.QueryMaxRows == 0 {
		o.QueryMaxRows = 1_000_000
	}
	if o.HoldMemoryBytes == 0 {
		o.HoldMemoryBytes = o.IngestMemoryLimitBytes / 2
	}
	return o
}

type entry struct {
	Metric string
	Record hta.Record
}
type manifest struct {
	seriesDirty      map[string]bool
	seriesDirtyKnown bool
	// Transient publication hints. Known dirtiness must include additions and
	// deletions; arbitrary snapshots use the full comparison fallback.
	rootDirty      map[string]bool
	rootDirtyKnown bool
	// Immutable metadata roots; the CAS object omits the hydrated maps below.
	CheckpointState, StreamIndex, HeldState blob
	heldPages                               heldTrees
	seriesPages, rootPages                  metadataDirectory
	stagingNamespace                        string

	TrashOffset  int
	TrashCleanup string
	// Held-record deltas still needed after a restart, oldest first, and the
	// last written record time of every stream with held delta records.
	Held           []blob
	HeldWatermarks map[string]int64
	// Finished journal pages, deleted by the next reclamation pass.
	TrashCleanups []string

	Catalog, Candidates                                blob
	CatalogReady                                       bool
	MaintenanceStatsReady                              bool
	TrashHead, TrashComplete, TrashBatch, TrashPending blob
	TrashObjects                                       int64
	CandidateObjects, SmallBlocks, SmallBlockBytes     int64
	CompactionJob                                      blob
	LiveObjectBytes, StoredObjectBytes                 int64
	LiveObjects                                        int64

	Version    int
	Generation uint64
	Sequence   uint64
	Series     map[string]*hta.Series
	Roots      map[string]map[int64]blob
	Garbage    map[string]bool // Fully retired packs; durable deletion queue.
}
type batch struct {
	ReceivedAt int64
	Config     hta.Config
	Metric     string
	Points     []hta.Point
}

// Input is a MetricQ routing binding, not part of the persisted HTA layout.
func aggregationConfig(c hta.Config) hta.Config {
	c = c.Defaults()
	c.Input = ""
	return c
}

// Engine is one open database. All exported methods are safe for concurrent
// use; only one Engine (one process) may own a WAL directory and store prefix.
type Engine struct {
	maintenanceWanted        chan struct{}
	sharedCatalog            *catalogPageCache
	dirtySeries              map[string]bool
	mu                       sync.Mutex
	maintenanceMu            sync.Mutex
	publishMu                sync.Mutex
	activeMaintenance        string
	stagingKeys              []string
	candidateCursor          string
	compactionSeedObject     string
	compactionSeedOffset     int
	compactionEvacuateObject string
	compactionBudget         *rateBudget
	lastCompactionEnd        time.Time
	compactionCompletions    uint64
	compactionScanMore       bool
	localityScans            map[string]localityScan
	deferredSeeds            map[string]int64 // merge seeds deferred until this time (unix ns)
	fragmentScans            map[string]fragmentScan
	fragmentCursor           string
	localityCursor           string
	compactionObjectLimit    int // source objects per job, adapted to the catalog budget
	lastReclaim              time.Time
	deletedCleanups          map[string]bool
	catalogCache             map[blob]catalogNode
	catalogCacheBytes        int64
	catalogReadBudget        int64
	catalogReadBytes         int64
	nodeReadLimit            int
	nodeReads                int
	store                    storage.Store
	wal                      *wal
	options                  Options
	metrics                  *Metrics
	nodeCache                map[blob]indexNode
	sharedNodes              *indexPageCache
	sharedBlocks             *dataBlockCache
	tailPages                map[blob]indexNode
	tailPaths                map[string]tailPath
	tailEntries              int
	tails                    map[string]streamTail
	tailBlocks               int // sum of open suffix blocks over tails
	tailsKnown               bool
	state                    manifest
	committed                manifest
	pins                     map[uint64]int
	version                  string
	sequence                 uint64
	pending                  pendingSet
	pendingBytes             int64
	flushing                 pendingSet // frozen by a running Flush, still queryable
	flushWanted              chan struct{}
	held                     map[string]*heldStream
	coveredRecords           int64
	now                      func() time.Time
	flushingBytes            int64
	oldestWAL                int64
	closed                   bool
	fatal                    error
	objectRefs               map[string]int64
	garbage                  map[string]bool
	readers                  int
}

func encode(v any) ([]byte, error) {
	if b, ok := v.(batch); ok {
		return encodeWALBatch(b), nil
	}
	if b, handled, err := encodeBinaryBlock(v); handled {
		return b, err
	}
	return encodeGob(v)
}

// Metadata keeps its existing gzip/Gob representation.
func encodeGob(v any) ([]byte, error) {
	var b bytes.Buffer
	z := gzipWriters.Get().(*gzip.Writer)
	z.Reset(&b)
	err := gob.NewEncoder(z).Encode(v)
	if e := z.Close(); err == nil {
		err = e
	}
	z.Reset(io.Discard)
	gzipWriters.Put(z)
	return b.Bytes(), err
}

// Each block keeps its independent gzip stream. Reuse only the compressor's
// scratch memory, not the gob type dictionary or encoded data.
var gzipWriters = sync.Pool{New: func() any {
	z, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	if err != nil {
		panic(err) // BestSpeed is a valid constant, independent of configuration.
	}
	return z
}}

func decode(b []byte, v any) error {
	if bytes.HasPrefix(b, []byte(walMagic)) {
		out, ok := v.(*batch)
		if !ok {
			return fmt.Errorf("WAL frame decoded into %T", v)
		}
		return decodeWALBatch(b, out)
	}
	if bytes.HasPrefix(b, []byte(blockMagic)) {
		return decodeBinaryBlock(b, v)
	}
	return decodeGob(b, v)
}

// Legacy independent gzip/Gob data and index blocks remain readable.
func decodeGob(b []byte, v any) error {
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer z.Close()
	return gob.NewDecoder(io.LimitReader(z, 512<<20)).Decode(v)
}

// Open loads the committed state from s, restores held records, replays the
// WAL in o.WALDirectory and configures the given metrics. It fails without
// modifying data if the WAL belongs to another store namespace, is damaged, or
// contradicts the stored configuration.
func Open(ctx context.Context, s storage.Store, o Options, configs map[string]hta.Config, m *Metrics) (*Engine, error) {
	o = o.defaults()
	if o.WALDirectory == "" || o.WALTarget <= 0 || o.WALTarget >= o.WALHigh || o.WALHigh >= o.WALHard || o.CheckpointUnsavedBytes <= 0 || o.IngestMemoryLimitBytes < o.CheckpointUnsavedBytes || o.QueryMaxRows < 1 {
		return nil, fmt.Errorf("invalid engine options")
	}
	if o.HoldExpiryIntervalSeconds < 1 || o.HoldExpiryIntervalSeconds > 3600 {
		return nil, fmt.Errorf("invalid hold expiry batching")
	}
	if o.HoldMaxAgeSeconds < 0 || (o.HoldMaxAgeSeconds > 0 && !o.MaintenanceEnabled) {
		return nil, fmt.Errorf("holding streams requires background maintenance")
	}
	if o.HoldMemoryBytes < 0 || o.HoldMemoryBytes >= o.IngestMemoryLimitBytes {
		return nil, fmt.Errorf("hold_memory_bytes must be below ingest_memory_limit_bytes")
	}
	if o.CheckpointAppendOnlyAggregates && (!o.MaintenanceEnabled || !o.CompactionOptions.Enabled || !o.CompactionOptions.MergeEnabled) {
		return nil, fmt.Errorf("append-only aggregates require enabled background block consolidation")
	}
	if o.MaintenanceEnabled {
		c := o.CompactionOptions
		if c.CycleMaxSeconds < 1 || c.CycleMaxSeconds > 3600 || c.JobTimeoutSeconds < 1 || c.JobTimeoutSeconds > 3600 || c.CycleIntervalSeconds < 1 || c.MergeCooldownSeconds < 0 || c.LocalityMinRanges < 2 || c.LocalityMinRanges > 512 || c.JobMaxBlocks < 1 || c.JobMaxBlocks > 512 || c.JobMaxBytes < 1 || c.JobMaxBytes > 64<<20 || c.OutputObjectBytes < 1 || c.OutputObjectBytes > c.JobMaxBytes || c.IOBytesPerSecond < 1 || c.ReclaimDeadFraction <= 0 || c.ReclaimDeadFraction >= 1 {
			return nil, fmt.Errorf("invalid compaction options")
		}
	}
	if m == nil {
		m = NewMetrics(prometheus.NewRegistry())
	}
	w, err := openWAL(o.WALDirectory)
	if err != nil {
		return nil, err
	}
	e := &Engine{store: s, wal: w, options: o, metrics: m, flushWanted: make(chan struct{}, 1), maintenanceWanted: make(chan struct{}, 1), sharedNodes: newIndexPageCache(), sharedCatalog: newCatalogPageCache(), sharedBlocks: newDataBlockCache(), state: manifest{Version: 2, Series: map[string]*hta.Series{}, Roots: map[string]map[int64]blob{}}}
	success := false
	defer func() {
		if !success {
			w.close()
		}
	}()
	b, v, err := e.get(ctx, "manifest")
	if err == nil {
		if err = decode(b, &e.state); err != nil {
			return nil, fmt.Errorf("manifest: %w", err)
		}
		if err = e.loadManifestState(ctx, &e.state); err != nil {
			return nil, fmt.Errorf("manifest metadata: %w", err)
		}
		if e.state.Version != 2 || e.state.Series == nil || e.state.Roots == nil {
			return nil, fmt.Errorf("invalid manifest version or state")
		}
		for _, series := range e.state.Series {
			if series != nil {
				series.Config = aggregationConfig(series.Config)
			}
		}
		e.version = v
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	e.committed = cloneManifest(e.state)
	e.pins = make(map[uint64]int)
	if err = e.configure(configs); err != nil {
		return nil, err
	}
	if err = w.bind(s.Identity(), e.state.Sequence); err != nil {
		return nil, err
	}
	e.sequence = e.state.Sequence
	e.setConfigMetrics()
	// Held records precede the WAL frames after the checkpoint.
	if err = e.loadHeld(ctx); err != nil {
		return nil, err
	}
	err = w.replay(func(seq uint64, b []byte) error {
		if seq <= e.state.Sequence {
			return nil
		}
		if seq != e.sequence+1 {
			return fmt.Errorf("WAL sequence gap: got %d after %d", seq, e.sequence)
		}
		var data batch
		if err := decode(b, &data); err != nil {
			return err
		}
		if _, ok := e.state.Series[data.Metric]; !ok {
			return fmt.Errorf("WAL metric %q missing from configuration", data.Metric)
		}
		if e.state.Series[data.Metric].Config != aggregationConfig(data.Config) {
			return fmt.Errorf("WAL aggregation config changed for %q", data.Metric)
		}
		if err := e.apply(data); err != nil {
			return fmt.Errorf("replay WAL sequence %d: %w", seq, err)
		}
		e.sequence = seq
		e.metrics.Replayed.Inc()
		return nil
	})
	if err != nil {
		return nil, err
	}
	if w.size > o.WALHard || e.pendingBytes > o.IngestMemoryLimitBytes {
		return nil, fmt.Errorf("recovered WAL exceeds configured limits; restore previous limits")
	}
	if err := e.initializeGC(ctx); err != nil {
		return nil, err
	}
	e.updateMetrics()
	success = true
	return e, nil
}

// Caller holds mu; swap this set at checkpoint freeze, restore on failure.
func (e *Engine) markSeriesDirty(name string) {
	if e.dirtySeries == nil {
		e.dirtySeries = make(map[string]bool)
	}
	e.dirtySeries[name] = true
}

func (e *Engine) configure(configs map[string]hta.Config) error {
	for name, c := range configs {
		c = aggregationConfig(c)
		if name == "" {
			return fmt.Errorf("empty metric name")
		}
		if err := c.Validate(); err != nil {
			return err
		}
		if old, ok := e.state.Series[name]; ok && old.Config != c {
			return fmt.Errorf("changing HTA configuration for %q requires migration", name)
		}
	}
	for name, c := range configs {
		if _, ok := e.state.Series[name]; !ok {
			e.state.Series[name] = hta.New(aggregationConfig(c))
			e.markSeriesDirty(name)
			e.state.Roots[name] = map[int64]blob{}
		}
	}
	return nil
}

// Configure adds metrics. Changing the aggregation parameters of an existing
// metric is rejected.
func (e *Engine) Configure(configs map[string]hta.Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.configure(configs)
}
func (e *Engine) updateMetrics() {
	if e.options.MaintenanceEnabled {
		e.updateMaintenanceMetrics(e.state)
	}
	e.metrics.WALPending.Set(float64(e.sequence - e.state.Sequence))
	e.metrics.OldestWAL.Set(float64(e.oldestWAL) / 1e9)
	e.metrics.WALBytes.Set(float64(e.wal.total()))
	e.metrics.Pressure.Set(float64(e.wal.total()) / float64(e.options.WALHigh))
	e.metrics.BuilderBytes.Set(float64(e.pendingBytes + e.flushingBytes))
	e.metrics.Series.Set(float64(len(e.state.Series)))
	e.metrics.Head.Set(float64(e.sequence))
	e.metrics.Checkpoint.Set(float64(e.state.Sequence))
	blocked := float64(0)
	if e.wal.total() >= e.options.WALHigh || e.pendingBytes+e.flushingBytes >= e.options.IngestMemoryLimitBytes {
		blocked = 1
	}
	e.metrics.Backpressure.Set(blocked)
	e.metrics.PendingRecords.Set(float64(e.pending.len() + e.flushing.len()))
	e.metrics.UnsavedBytes.Set(float64(e.unsavedBytes()))
	e.metrics.HeldCoveredRecords.Set(float64(e.coveredRecords))
	e.metrics.HeldDeltas.Set(float64(len(e.state.Held)))
	e.metrics.WALSegments.Set(float64(len(e.wal.frozen) + 1))
	e.metrics.PinnedEntries.Set(float64(e.tailEntries))
}

// setConfigMetrics publishes the effective limits once at startup.
func (e *Engine) setConfigMetrics() {
	o, c := e.options, e.options.CompactionOptions.defaults()
	for name, v := range map[string]float64{
		"wal_target_bytes": float64(o.WALTarget), "wal_high_bytes": float64(o.WALHigh), "wal_hard_bytes": float64(o.WALHard),
		"checkpoint_unsaved_bytes": float64(o.CheckpointUnsavedBytes), "ingest_memory_limit_bytes": float64(o.IngestMemoryLimitBytes),
		"hold_max_age_seconds": float64(o.HoldMaxAgeSeconds), "hold_expiry_interval_seconds": float64(o.HoldExpiryIntervalSeconds), "hold_memory_bytes": float64(o.HoldMemoryBytes), "query_max_rows": float64(o.QueryMaxRows),
		"compaction_cycle_interval_seconds": float64(c.CycleIntervalSeconds), "compaction_merge_cooldown_seconds": float64(c.MergeCooldownSeconds),
		"compaction_job_timeout_seconds": float64(c.JobTimeoutSeconds), "compaction_cycle_max_seconds": float64(c.CycleMaxSeconds),
		"compaction_job_max_bytes": float64(c.JobMaxBytes), "compaction_job_max_blocks": float64(c.JobMaxBlocks),
		"compaction_output_object_bytes": float64(c.OutputObjectBytes), "compaction_io_bytes_per_second": float64(c.IOBytesPerSecond),
		"compaction_reclaim_dead_fraction": c.ReclaimDeadFraction, "compaction_locality_min_ranges": float64(c.LocalityMinRanges),
	} {
		e.metrics.Config.WithLabelValues(name).Set(v)
	}
	e.metrics.CompactionObjectLimit.Set(maxCompactionObjects)
	continuous := float64(0)
	if c.Continuous {
		continuous = 1
	}
	e.metrics.Config.WithLabelValues("compaction_continuous").Set(continuous)
}
func (e *Engine) apply(b batch) error {
	if e.oldestWAL == 0 {
		e.oldestWAL = b.ReceivedAt
	}
	s := e.state.Series[b.Metric]
	e.markSeriesDirty(b.Metric)
	for _, p := range b.Points {
		if !s.Insert(p, func(r hta.Record) {
			e.addPending(b.Metric, r)
		}) {
			return fmt.Errorf("WAL contains unprocessable point for %q at %d", b.Metric, p.Time)
		}
		e.metrics.Samples.Inc()
	}
	return nil
}

// Ingest makes one delivery durable; see IngestBatch.
func (e *Engine) Ingest(ctx context.Context, name string, chunk *metricq.DataChunk) error {
	_, err := e.IngestBatch(ctx, []Delivery{{Metric: name, Chunk: chunk}})
	return err
}

// Delivery is one DataChunk for a canonical metric.
type Delivery struct {
	Metric string
	Chunk  *metricq.DataChunk
}

func chunkPoints(chunk *metricq.DataChunk) ([]hta.Point, error) {
	if chunk == nil || len(chunk.TimeDelta) != len(chunk.Value) {
		return nil, fmt.Errorf("malformed DataChunk")
	}
	points := make([]hta.Point, len(chunk.Value))
	var ts int64
	for i, d := range chunk.TimeDelta {
		if (d > 0 && ts > math.MaxInt64-d) || (d < 0 && ts < math.MinInt64-d) {
			return nil, fmt.Errorf("timestamp delta overflow")
		}
		ts += d
		points[i] = hta.Point{Time: ts, Value: chunk.Value[i]}
	}
	return points, nil
}

// ingestPlan is one delivery's validated state and record expansion.
type ingestPlan struct {
	metric   string
	series   *hta.Series
	prepared []entry
	extra    int64
	accepted int
	dropped  int
	received int64
}

// IngestBatch makes consecutive deliveries durable with one WAL write and one
// fsync. Each accepted delivery keeps its own WAL frame and sequence, so replay
// is unchanged. It returns how many leading deliveries were processed (durable
// or entirely discarded as duplicates). On ErrPressure the caller can flush and
// retry the remainder; any other error stops the batch after that prefix.
func (e *Engine) IngestBatch(ctx context.Context, deliveries []Delivery) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	e.metrics.IngestBatches.Inc()
	e.metrics.IngestBatchSize.Observe(float64(len(deliveries)))
	points := make([][]hta.Point, len(deliveries))
	var stop error
	valid := len(deliveries)
	for i, d := range deliveries {
		if points[i], stop = chunkPoints(d.Chunk); stop != nil {
			valid = i
			break
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		return 0, e.fatal
	}
	var frames []byte
	var plans []ingestPlan
	working := make(map[string]*hta.Series)
	// Frames and records frozen by a running flush still occupy WAL and memory.
	walSize, pendingBytes := e.wal.total(), e.pendingBytes+e.flushingBytes
	sequence := e.sequence
	processed := 0
	for i := 0; i < valid; i++ {
		name := deliveries[i].Metric
		s := working[name]
		if s == nil {
			if s = e.state.Series[name]; s == nil {
				stop = fmt.Errorf("unconfigured metric %q", name)
				break
			}
		}
		// Plan the exact state/record expansion before claiming durability. This also
		// bounds sparse-series aggregation and prevents large deliveries exceeding RAM.
		clone := *s
		clone.Levels = make(map[int64]hta.Level, len(s.Levels))
		for k, v := range s.Levels {
			clone.Levels[k] = v
		}
		plan := ingestPlan{metric: name, series: &clone, received: time.Now().UnixNano()}
		available := e.options.IngestMemoryLimitBytes - pendingBytes
		accepted := make([]hta.Point, 0, len(points[i]))
		for _, p := range points[i] {
			if clone.Insert(p, func(r hta.Record) {
				plan.extra += pendingRecordBytes
				// Keep the speculative buffer bounded even for rejected deliveries.
				if plan.extra <= available {
					plan.prepared = append(plan.prepared, entry{Metric: name, Record: r})
				}
			}) {
				accepted = append(accepted, p)
			} else {
				plan.dropped++
			}
		}
		plan.accepted = len(accepted)
		if plan.accepted == 0 {
			plans = append(plans, plan)
			processed = i + 1
			continue
		}
		payload, err := encode(batch{ReceivedAt: plan.received, Config: s.Config, Metric: name, Points: accepted})
		if err != nil {
			stop = err
			break
		}
		if len(payload) > maxFrame {
			stop = fmt.Errorf("DataChunk too large")
			break
		}
		if int64(len(payload)+frameHeader) > e.options.WALHard || plan.extra > e.options.IngestMemoryLimitBytes {
			stop = fmt.Errorf("delivery exceeds configured WAL/builder capacity")
			break
		}
		if walSize >= e.options.WALHigh || walSize+int64(len(payload)+frameHeader) > e.options.WALHard || pendingBytes+plan.extra > e.options.IngestMemoryLimitBytes {
			e.metrics.Backpressure.Set(1)
			e.metrics.BackpressureEvents.Inc()
			stop = ErrPressure
			break
		}
		sequence++
		frames = appendFrame(frames, sequence, payload)
		walSize += int64(len(payload) + frameHeader)
		pendingBytes += plan.extra
		working[name] = &clone
		plans = append(plans, plan)
		processed = i + 1
	}
	if len(frames) > 0 {
		start := time.Now()
		err := e.wal.write(frames)
		e.metrics.Sync.Observe(time.Since(start).Seconds())
		if err != nil {
			e.metrics.WALErrors.Inc()
			e.fatal = err
			return 0, err
		}
	}
	// Publish the already validated states only after their WAL frames are
	// durable. WAL replay independently reconstructs exactly this state using apply.
	for _, plan := range plans {
		if plan.dropped > 0 {
			e.metrics.Skipped.Add(float64(plan.dropped))
			slog.Warn("discarded duplicate, non-monotonic or nonfinite samples before WAL append", "metric", plan.metric, "count", plan.dropped)
		}
		if plan.accepted == 0 {
			continue
		}
		e.sequence++
		e.state.Series[plan.metric] = plan.series
		e.markSeriesDirty(plan.metric)
		for _, en := range plan.prepared {
			e.addPending(en.Metric, en.Record)
		}
		if e.oldestWAL == 0 {
			e.oldestWAL = plan.received
		}
		e.metrics.Samples.Add(float64(plan.accepted))
	}
	e.updateMetrics()
	// Wake the flush loop as soon as a threshold is crossed, instead of letting
	// the builder grow towards its hard limit until the next tick.
	if e.wal.size >= e.options.WALTarget || e.unsavedBytes() >= e.options.CheckpointUnsavedBytes {
		select {
		case e.flushWanted <- struct{}{}:
		default:
		}
	}
	return processed, stop
}
func (e *Engine) get(ctx context.Context, key string) ([]byte, string, error) {
	start := time.Now()
	defer func() { e.metrics.StoreGet.Observe(time.Since(start).Seconds()) }()
	b, v, err := e.store.Get(ctx, key)
	e.metrics.observeStore("get", key, len(b), err)
	return b, v, err
}
func (e *Engine) put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	start := time.Now()
	defer func() { e.metrics.StorePut.Observe(time.Since(start).Seconds()) }()
	version, err := e.store.Put(ctx, key, b, v)
	e.metrics.observeStore("put", key, len(b), err)
	if err == nil && key == "manifest" {
		e.metrics.ManifestBytes.Set(float64(len(b)))
	}
	return version, err
}

// NeedsFlush reports whether a checkpoint is due: WAL or object target
// reached, a held stream expired, or held memory above its budget.
func (e *Engine) NeedsFlush() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.size >= e.options.WALTarget || e.unsavedBytes() >= e.options.CheckpointUnsavedBytes || e.holdDue() || e.holdPressure()
}

// Flush writes a checkpoint: it freezes the records to write and the active
// WAL segment, uploads blocks, index pages, held deltas and metadata without
// holding the ingestion mutex, publishes a new manifest and releases the
// covered WAL segments. On failure nothing is released and the next Flush
// retries. Flush calls are serialized with each other and with maintenance
// publications.
func (e *Engine) Flush(ctx context.Context) (err error) {
	e.publishMu.Lock()
	defer e.publishMu.Unlock()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		err = e.fatal
		e.mu.Unlock()
		return err
	}
	if e.sequence == e.state.Sequence && e.version != "" && !e.holdDue() && !e.holdPressure() {
		if !e.options.MaintenanceEnabled {
			e.collectGarbage(ctx)
		}
		e.mu.Unlock()
		return nil
	}
	start := time.Now()
	defer func() {
		e.metrics.Flush.Observe(time.Since(start).Seconds())
		if err != nil {
			e.metrics.CommitErrors.Inc()
		}
	}()
	reason := "explicit"
	switch {
	case e.wal.size >= e.options.WALTarget:
		reason = "wal"
	case e.unsavedBytes() >= e.options.CheckpointUnsavedBytes:
		reason = "object_target"
	case e.holdPressure():
		reason = "hold_budget"
	case e.holdDue():
		reason = "hold_age"
	}
	e.metrics.FlushReasons.WithLabelValues(reason).Inc()
	// Freeze a consistent checkpoint under the ingestion lock: the durable WAL
	// prefix, its records and HTA state. Ingestion continues in a new WAL segment
	// while the frozen part is uploaded; queries still see the frozen records.
	// Only this Flush (serialized by publishMu) changes roots and metadata.
	if err = e.wal.rotate(e.sequence); err != nil {
		e.fatal = err
		e.mu.Unlock()
		return err
	}
	next := cloneManifest(e.state)
	next.Sequence = e.sequence
	next.Generation++
	deltaPack, err := newPack("held")
	if err != nil {
		e.mu.Unlock()
		return err
	}
	// Move each stream's written prefix to the frozen set; held suffixes and
	// newer records stay pending and queryable.
	plan := e.planHold(deltaPack.key)
	var frozen pendingSet
	for metric, levels := range plan.write {
		for level, k := range levels {
			records := e.pending.streams[metric][level]
			if frozen.streams == nil {
				frozen.streams = make(map[string]map[int64][]hta.Record)
			}
			if frozen.streams[metric] == nil {
				frozen.streams[metric] = make(map[int64][]hta.Record)
			}
			frozen.streams[metric][level] = records[:k:k]
			frozen.records += k
			e.pending.streams[metric][level] = records[k:]
			e.pending.records -= k
		}
	}
	frozenOldest := e.oldestWAL
	e.flushing, e.flushingBytes = frozen, int64(frozen.records)*pendingRecordBytes
	e.pendingBytes -= e.flushingBytes
	e.oldestWAL = 0
	frozenDirty := e.dirtySeries
	next.seriesDirty, next.seriesDirtyKnown = frozenDirty, true
	next.rootDirty, next.rootDirtyKnown = make(map[string]bool), true
	for name := range next.Roots {
		if _, ok := e.committed.Roots[name]; !ok {
			next.rootDirty[name] = true
		}
	}
	e.dirtySeries = nil
	e.mu.Unlock()
	refDelta := make(map[string]int64)
	var publishedData, publishedIndex *pack
	var stagedTails map[string]tailPath
	var version string
	err = func() error {
		// Streams in metric/level order pack hot metrics into dedicated objects
		// and combine small streams; pending records are already grouped.
		streams := frozen.sorted()
		if len(streams) > 0 {
			dataPack, packErr := newPack("data")
			if packErr != nil {
				return packErr
			}
			publishedData = dataPack
			indexPack, packErr := newPack("index")
			if packErr != nil {
				return packErr
			}
			publishedIndex = indexPack
			indexPack.nodes = make(map[blob]indexNode)
			stagedTails = make(map[string]tailPath)
			type indexUpdate struct {
				items       []indexEntry
				replaceTail bool
			}
			type block struct {
				stream  int
				records []hta.Record
				encoded []byte
			}
			updates := make([]indexUpdate, len(streams))
			var blocks []block
			for i, stream := range streams {
				records, level := stream.records, stream.level
				var part []hta.Record
				// Fill the last partial aggregate block across checkpoints. Reading
				// only this bounded tail keeps coarse levels independent of flush
				// frequency. Raw blocks are appended; published blobs remain immutable.
				if level > 0 && !e.options.CheckpointAppendOnlyAggregates {
					tail, tailErr := e.lastIndexEntry(ctx, next.Roots[stream.metric][level])
					if tailErr != nil {
						return tailErr
					}
					if tail.Records > 0 && tail.Records < maxDataBlockRecords {
						b, readErr := e.readBlob(ctx, tail.Blob)
						if readErr != nil {
							return readErr
						}
						if err := decode(b, &part); err != nil {
							return err
						}
						if len(part) != tail.Records || part[0].Time != tail.First || part[len(part)-1].LastTime() != tail.Last {
							return fmt.Errorf("invalid aggregate tail block")
						}
						for row, record := range part {
							if record.Level != level || (row > 0 && record.Time <= part[row-1].LastTime()) {
								return fmt.Errorf("invalid aggregate tail records")
							}
						}
						if records[0].Time <= tail.Last {
							return fmt.Errorf("aggregate tail overlaps new records")
						}
						updates[i].replaceTail = true
						n := min(maxDataBlockRecords-len(part), len(records))
						blocks = append(blocks, block{stream: i, records: append(part, records[:n]...)})
						records = records[n:]
					}
				}
				if first := min(plan.first[stream.metric][level], len(records)); first > 0 && !updates[i].replaceTail {
					blocks = append(blocks, block{stream: i, records: records[:first]})
					records = records[first:]
				}
				for k := 0; k < len(records); k += maxDataBlockRecords {
					blocks = append(blocks, block{stream: i, records: records[k:min(k+maxDataBlockRecords, len(records))]})
				}
			}
			// Blocks are independent: compress them in parallel, then pack them in
			// the deterministic stream order.
			encodeErrs := make([]error, len(blocks))
			var encoders sync.WaitGroup
			work := make(chan int)
			for w := 0; w < min(runtime.GOMAXPROCS(0), len(blocks)); w++ {
				encoders.Add(1)
				go func() {
					defer encoders.Done()
					for k := range work {
						blocks[k].encoded, encodeErrs[k] = encode(blocks[k].records)
					}
				}()
			}
			for k := range blocks {
				work <- k
			}
			close(work)
			encoders.Wait()
			for k, b := range blocks {
				if encodeErrs[k] != nil {
					return encodeErrs[k]
				}
				if len(b.records) == maxDataBlockRecords {
					e.metrics.FlushBlocks.WithLabelValues("full").Inc()
				} else {
					e.metrics.FlushBlocks.WithLabelValues("partial").Inc()
				}
				stream := streams[b.stream]
				item := indexEntry{First: b.records[0].Time, Last: b.records[len(b.records)-1].LastTime(), Blob: dataPack.add(b.encoded), Records: len(b.records)}
				updates[b.stream].items = append(updates[b.stream].items, item)
				dataPack.descriptors = append(dataPack.descriptors, BlockInfo{Metric: stream.metric, Level: stream.level, Entry: item})
			}
			empty := ""
			if _, err = e.put(ctx, dataPack.key, dataPack.buf.Bytes(), &empty); err != nil {
				return err
			}
			e.metrics.Objects.Inc()
			e.metrics.Bytes.Add(float64(dataPack.buf.Len()))
			for i, update := range updates {
				metric, level := streams[i].metric, streams[i].level
				indexPack.metric, indexPack.level = metric, level
				root, appendErr := e.updateIndex(ctx, next.Roots[metric][level], update.items, indexPack, update.replaceTail)
				if appendErr != nil {
					return appendErr
				}
				next.Roots[metric][level] = root
				next.rootDirty[metric] = true
				stagedTails[streamKey(metric, level)] = rightmostPath(root, indexPack.nodes)
			}
			if _, err = e.put(ctx, indexPack.key, indexPack.buf.Bytes(), &empty); err != nil {
				return err
			}
			e.metrics.Objects.Inc()
			e.metrics.Bytes.Add(float64(indexPack.buf.Len()))
			refDelta[dataPack.key] += dataPack.blocks
			refDelta[indexPack.key] += indexPack.blocks
			for _, retired := range indexPack.retired {
				refDelta[retired.Key]--
			}
		}
		if e.objectRefs != nil {
			next.Garbage = make(map[string]bool, len(e.garbage))
			for key := range e.garbage {
				next.Garbage[key] = true
			}
			for key, delta := range refDelta {
				count := e.objectRefs[key] + delta
				if count < 0 {
					return fmt.Errorf("negative object references: %s", key)
				}
				if count == 0 {
					next.Garbage[key] = true
				}
			}
		}
		if e.options.MaintenanceEnabled {
			if err = e.catalogCheckpoint(ctx, &next, publishedData, publishedIndex); err != nil {
				return err
			}
		}

		if e.holding() {
			next.Held = plan.live
			if len(plan.delta.Streams) > 0 {
				b, err := encode(plan.delta)
				if err != nil {
					return err
				}
				next.Held = append(append([]blob(nil), plan.live...), deltaPack.add(b))
				empty := ""
				if _, err = e.put(ctx, deltaPack.key, deltaPack.buf.Bytes(), &empty); err != nil {
					return err
				}
				e.metrics.Objects.Inc()
				e.metrics.Bytes.Add(float64(deltaPack.buf.Len()))
				e.metrics.HeldDeltaBytes.Add(float64(deltaPack.buf.Len()))
			}
			next.HeldWatermarks = plan.watermarks
			if err := e.appendTrash(ctx, &next, plan.obsolete, 0); err != nil {
				return err
			}
		}
		b, err := e.encodeManifest(ctx, &next, e.committed)
		if err != nil {
			return err
		}
		version, err = e.put(ctx, "manifest", b, &e.version)
		if err != nil {
			// PUT may have succeeded even if its response was lost. Confirm the exact
			// checkpoint before deleting any WAL; never guess on transport failure.
			actual, v, getErr := e.get(ctx, "manifest")
			if getErr == nil && bytes.Equal(actual, b) {
				version = v
				return nil
			}
			return err
		}
		return nil
	}()
	var committed manifest
	if err == nil {
		committed = cloneManifest(next)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			e.fatal = fmt.Errorf("another writer changed manifest: %w", err)
		}
		for name := range frozenDirty {
			e.markSeriesDirty(name)
		}
		// The frozen records stay queryable and are retried by the next flush,
		// together with its frozen WAL segment.
		e.pending.prepend(e.flushing)
		e.pendingBytes += e.flushingBytes
		e.flushing, e.flushingBytes = pendingSet{}, 0
		if frozenOldest != 0 && (e.oldestWAL == 0 || frozenOldest < e.oldestWAL) {
			e.oldestWAL = frozenOldest
		}
		e.updateMetrics()
		return err
	}
	// Publish the checkpoint, keeping the live HTA state and metrics configured
	// meanwhile. The manifest keeps the frozen state matching its WAL sequence.
	live := e.state.Series
	liveRoots := e.state.Roots
	e.state = next
	e.state.Series = live
	for name, roots := range liveRoots {
		if _, ok := e.state.Roots[name]; !ok {
			e.state.Roots[name] = roots
		}
	}
	e.committed = committed
	if e.objectRefs != nil {
		for key, delta := range refDelta {
			e.objectRefs[key] += delta
			if e.objectRefs[key] == 0 {
				delete(e.objectRefs, key)
			}
		}
		e.garbage = next.Garbage
	}
	e.version = version
	if publishedIndex != nil {
		e.pinTailPaths(stagedTails, publishedIndex.nodes)
		publishedIndex.nodes = nil
	}
	e.commitHold(plan)
	e.metrics.FlushRecords.Add(float64(e.flushing.len()))
	e.flushing, e.flushingBytes = pendingSet{}, 0
	if err = e.wal.checkpoint(next.Sequence); err != nil {
		e.fatal = err
		return err
	}
	if err = e.wal.release(next.Sequence); err != nil {
		e.fatal = err
		return err
	}
	e.metrics.Commits.Inc()
	e.metrics.LastCommit.SetToCurrentTime()
	e.updateMetrics()
	if !e.options.MaintenanceEnabled {
		e.collectGarbage(ctx)
	}
	if e.options.CompactionOptions.Continuous {
		select {
		case e.maintenanceWanted <- struct{}{}:
		default:
		}
	}
	return nil
}

// RunFlush retries only size/pressure-triggered work. Wallclock never closes an
// HTA interval or seals a low-volume object. Shutdown can explicitly Flush.
func (e *Engine) RunFlush(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var failed time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.mu.Lock()
			e.updateHoldMetrics()
			e.mu.Unlock()
		case <-e.flushWanted:
			// After a failed checkpoint, retry only on the tick, not per delivery.
			if time.Since(failed) < time.Second {
				continue
			}
		}
		if e.needsScheduledFlush() {
			if err := e.Flush(ctx); err != nil {
				failed = time.Now()
				slog.Error("object-store checkpoint failed; WAL retained", "error", err)
			}
		} else if !e.options.MaintenanceEnabled && e.publishMu.TryLock() {
			// Legacy GC edits reference state that an unlocked flush reads.
			e.mu.Lock()
			if !e.closed && e.fatal == nil {
				e.collectGarbage(ctx)
			}
			e.mu.Unlock()
			e.publishMu.Unlock()
		}
	}
}

// Close stops accepting work and closes the WAL. It does not flush; call
// Flush first for a final checkpoint.
func (e *Engine) Close() error {
	e.publishMu.Lock()
	defer e.publishMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.wal.close()
}
