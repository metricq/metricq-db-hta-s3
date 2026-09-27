// Package engine implements a single-writer HTA database with a bounded local
// WAL and immutable, size-sealed objects. RabbitMQ owns backlog beyond the WAL.
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

	"github.com/metricq/metricq-db-hta-go/hta"
	"github.com/metricq/metricq-db-hta-go/storage"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
)

var ErrPressure = errors.New("WAL or builder high watermark reached")

// pendingRecordBytes estimates builder memory per unflushed record: an 80-byte
// hta.Record in its stream slice plus append growth slack. Records no longer
// carry their metric name.
const pendingRecordBytes = 96

type Options struct {
	AppendOnlyAggregates bool `json:"append_only_aggregates"`
	// HoldSeconds > 0 keeps streams in memory until they fill a block or their
	// oldest record reaches this age; held records persist in held/ deltas.
	HoldSeconds           int64             `json:"hold_seconds"`
	// HoldBytes bounds held records (estimated); above it the largest streams
	// are written early. Zero means half of BuilderHard.
	HoldBytes             int64             `json:"hold_bytes"`
	BackgroundMaintenance bool              `json:"background_maintenance"`
	Compaction            CompactionOptions `json:"compaction"`
	WALDirectory          string            `json:"wal_directory"`
	WALTarget             int64             `json:"wal_target_bytes"`
	WALHigh               int64             `json:"wal_high_bytes"`
	WALHard               int64             `json:"wal_hard_bytes"`
	ObjectTarget          int64             `json:"object_target_bytes"`
	BuilderHard           int64             `json:"builder_hard_bytes"`
	MaxQueryRows          int               `json:"max_query_rows"`
}

func (o Options) defaults() Options {
	o.Compaction = o.Compaction.defaults()
	if o.WALTarget == 0 {
		o.WALTarget = 32 << 20
	}
	if o.WALHigh == 0 {
		o.WALHigh = 64 << 20
	}
	if o.WALHard == 0 {
		o.WALHard = 80 << 20
	}
	if o.ObjectTarget == 0 {
		o.ObjectTarget = 4 << 20
	}
	if o.BuilderHard == 0 {
		o.BuilderHard = 32 << 20
	}
	if o.MaxQueryRows == 0 {
		o.MaxQueryRows = 1_000_000
	}
	if o.HoldBytes == 0 {
		o.HoldBytes = o.BuilderHard / 2
	}
	return o
}

type entry struct {
	Metric string
	Record hta.Record
}
type manifest struct {
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

type Engine struct {
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
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer z.Close()
	return gob.NewDecoder(io.LimitReader(z, 512<<20)).Decode(v)
}
func Open(ctx context.Context, s storage.Store, o Options, configs map[string]hta.Config, m *Metrics) (*Engine, error) {
	o = o.defaults()
	if o.WALDirectory == "" || o.WALTarget <= 0 || o.WALTarget >= o.WALHigh || o.WALHigh >= o.WALHard || o.ObjectTarget <= 0 || o.BuilderHard < o.ObjectTarget || o.MaxQueryRows < 1 {
		return nil, fmt.Errorf("invalid engine options")
	}
	if o.HoldSeconds < 0 || (o.HoldSeconds > 0 && !o.BackgroundMaintenance) {
		return nil, fmt.Errorf("holding streams requires background maintenance")
	}
	if o.HoldBytes < 0 || o.HoldBytes >= o.BuilderHard {
		return nil, fmt.Errorf("hold_bytes must be below builder_hard_bytes")
	}
	if o.AppendOnlyAggregates && (!o.BackgroundMaintenance || !o.Compaction.Enabled || !o.Compaction.MergeSmallBlocks) {
		return nil, fmt.Errorf("append-only aggregates require enabled background block consolidation")
	}
	if o.BackgroundMaintenance {
		c := o.Compaction
		if c.MaxCycleSeconds < 1 || c.MaxCycleSeconds > 3600 || c.MaxDurationSeconds < 1 || c.MaxDurationSeconds > 3600 || c.IntervalSeconds < 1 || c.CooldownSeconds < 0 || c.MaxBlocks < 1 || c.MaxBlocks > 512 || c.MaxJobBytes < 1 || c.MaxJobBytes > 64<<20 || c.ObjectBytes < 1 || c.ObjectBytes > c.MaxJobBytes || c.BytesPerSecond < 1 || c.DeadFraction <= 0 || c.DeadFraction >= 1 {
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
	e := &Engine{store: s, wal: w, options: o, metrics: m, flushWanted: make(chan struct{}, 1), sharedNodes: newIndexPageCache(), sharedBlocks: newDataBlockCache(), state: manifest{Version: 2, Series: map[string]*hta.Series{}, Roots: map[string]map[int64]blob{}}}
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
	if w.size > o.WALHard || e.pendingBytes > o.BuilderHard {
		return nil, fmt.Errorf("recovered WAL exceeds configured limits; restore previous limits")
	}
	if err := e.initializeGC(ctx); err != nil {
		return nil, err
	}
	e.updateMetrics()
	success = true
	return e, nil
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
			e.state.Roots[name] = map[int64]blob{}
		}
	}
	return nil
}
func (e *Engine) Configure(configs map[string]hta.Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.configure(configs)
}
func (e *Engine) updateMetrics() {
	if e.options.BackgroundMaintenance {
		e.updateMaintenanceMetrics(e.state)
	}
	e.metrics.WALTarget.Set(float64(e.options.WALTarget))
	e.metrics.WALHigh.Set(float64(e.options.WALHigh))
	e.metrics.WALHard.Set(float64(e.options.WALHard))
	e.metrics.WALPending.Set(float64(e.sequence - e.state.Sequence))
	e.metrics.OldestWAL.Set(float64(e.oldestWAL) / 1e9)
	e.metrics.WALBytes.Set(float64(e.wal.total()))
	e.metrics.Pressure.Set(float64(e.wal.total()) / float64(e.options.WALHigh))
	e.metrics.BuilderBytes.Set(float64(e.pendingBytes + e.flushingBytes))
	e.metrics.Series.Set(float64(len(e.state.Series)))
	e.metrics.Head.Set(float64(e.sequence))
	e.metrics.Checkpoint.Set(float64(e.state.Sequence))
	blocked := float64(0)
	if e.wal.total() >= e.options.WALHigh || e.pendingBytes+e.flushingBytes >= e.options.BuilderHard {
		blocked = 1
	}
	e.metrics.Backpressure.Set(blocked)
}
func (e *Engine) apply(b batch) error {
	if e.oldestWAL == 0 {
		e.oldestWAL = b.ReceivedAt
	}
	s := e.state.Series[b.Metric]
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
		available := e.options.BuilderHard - pendingBytes
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
		if int64(len(payload)+frameHeader) > e.options.WALHard || plan.extra > e.options.BuilderHard {
			stop = fmt.Errorf("delivery exceeds configured WAL/builder capacity")
			break
		}
		if walSize >= e.options.WALHigh || walSize+int64(len(payload)+frameHeader) > e.options.WALHard || pendingBytes+plan.extra > e.options.BuilderHard {
			e.metrics.Backpressure.Set(1)
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
	if e.wal.size >= e.options.WALTarget || e.unsavedBytes() >= e.options.ObjectTarget {
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
	return e.store.Get(ctx, key)
}
func (e *Engine) put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	start := time.Now()
	defer func() { e.metrics.StorePut.Observe(time.Since(start).Seconds()) }()
	return e.store.Put(ctx, key, b, v)
}
func (e *Engine) NeedsFlush() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.size >= e.options.WALTarget || e.unsavedBytes() >= e.options.ObjectTarget || e.holdDue() || e.holdPressure()
}
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
		if !e.options.BackgroundMaintenance {
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
				if level > 0 && !e.options.AppendOnlyAggregates {
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
		if e.options.BackgroundMaintenance {
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
			}
			next.HeldWatermarks = plan.watermarks
			if err := e.appendTrash(ctx, &next, plan.obsolete, 0); err != nil {
				return err
			}
		}
		b, err := encode(next)
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
	e.mu.Lock()
	defer e.mu.Unlock()
	if err != nil {
		if errors.Is(err, storage.ErrConflict) {
			e.fatal = fmt.Errorf("another writer changed manifest: %w", err)
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
	e.committed = cloneManifest(next)
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
	if !e.options.BackgroundMaintenance {
		e.collectGarbage(ctx)
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
		case <-e.flushWanted:
			// After a failed checkpoint, retry only on the tick, not per delivery.
			if time.Since(failed) < time.Second {
				continue
			}
		}
		if e.NeedsFlush() {
			if err := e.Flush(ctx); err != nil {
				failed = time.Now()
				slog.Error("object-store checkpoint failed; WAL retained", "error", err)
			}
		} else if !e.options.BackgroundMaintenance && e.publishMu.TryLock() {
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
