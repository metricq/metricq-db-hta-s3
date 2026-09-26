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
	"sort"
	"sync"
	"time"

	"github.com/metricq/metricq-db-hta-go/hta"
	"github.com/metricq/metricq-db-hta-go/storage"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
)

var ErrPressure = errors.New("WAL or builder high watermark reached")

type Options struct {
	AppendOnlyAggregates  bool              `json:"append_only_aggregates"`
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
	return o
}

type entry struct {
	Metric string
	Record hta.Record
}
type manifest struct {
	TrashOffset  int
	TrashCleanup string

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
	pending                  []entry
	pendingBytes             int64
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
	e := &Engine{store: s, wal: w, options: o, metrics: m, sharedNodes: newIndexPageCache(), sharedBlocks: newDataBlockCache(), state: manifest{Version: 2, Series: map[string]*hta.Series{}, Roots: map[string]map[int64]blob{}}}
	success := false
	defer func() {
		if !success {
			w.file.Close()
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
	e.metrics.WALBytes.Set(float64(e.wal.size))
	e.metrics.Pressure.Set(float64(e.wal.size) / float64(e.options.WALHigh))
	e.metrics.BuilderBytes.Set(float64(e.pendingBytes))
	e.metrics.Series.Set(float64(len(e.state.Series)))
	e.metrics.Head.Set(float64(e.sequence))
	e.metrics.Checkpoint.Set(float64(e.state.Sequence))
	blocked := float64(0)
	if e.wal.size >= e.options.WALHigh || e.pendingBytes >= e.options.BuilderHard {
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
			e.pending = append(e.pending, entry{b.Metric, r})
			e.pendingBytes += int64(96 + len(b.Metric))
		}) {
			return fmt.Errorf("WAL contains unprocessable point for %q at %d", b.Metric, p.Time)
		}
		e.metrics.Samples.Inc()
	}
	return nil
}
func (e *Engine) Ingest(ctx context.Context, name string, chunk *metricq.DataChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chunk == nil || len(chunk.TimeDelta) != len(chunk.Value) {
		return fmt.Errorf("malformed DataChunk")
	}
	points := make([]hta.Point, len(chunk.Value))
	var ts int64
	for i, d := range chunk.TimeDelta {
		if (d > 0 && ts > math.MaxInt64-d) || (d < 0 && ts < math.MinInt64-d) {
			return fmt.Errorf("timestamp delta overflow")
		}
		ts += d
		points[i] = hta.Point{Time: ts, Value: chunk.Value[i]}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		return e.fatal
	}
	s, ok := e.state.Series[name]
	if !ok {
		return fmt.Errorf("unconfigured metric %q", name)
	}
	// Plan the exact state/record expansion before claiming durability. This also
	// bounds sparse-series aggregation and prevents large deliveries exceeding RAM.
	clone := *s
	clone.Levels = make(map[int64]hta.Level, len(s.Levels))
	for k, v := range s.Levels {
		clone.Levels[k] = v
	}
	var extra int64
	var prepared []entry
	available := e.options.BuilderHard - e.pendingBytes
	accepted := make([]hta.Point, 0, len(points))
	dropped := 0
	for _, p := range points {
		if clone.Insert(p, func(r hta.Record) {
			extra += int64(96 + len(name))
			// Keep the speculative buffer bounded even for rejected deliveries.
			if extra <= available {
				prepared = append(prepared, entry{Metric: name, Record: r})
			}
		}) {
			accepted = append(accepted, p)
		} else {
			dropped++
		}
	}
	recordDropped := func() {
		if dropped > 0 {
			e.metrics.Skipped.Add(float64(dropped))
			slog.Warn("discarded duplicate, non-monotonic or nonfinite samples before WAL append", "metric", name, "count", dropped)
		}
	}
	if len(accepted) == 0 {
		recordDropped()
		return nil
	}
	b := batch{ReceivedAt: time.Now().UnixNano(), Config: s.Config, Metric: name, Points: accepted}
	payload, err := encode(b)
	if err != nil {
		return err
	}
	if len(payload) > maxFrame {
		return fmt.Errorf("DataChunk too large")
	}
	if int64(len(payload)+frameHeader) > e.options.WALHard || extra > e.options.BuilderHard {
		return fmt.Errorf("delivery exceeds configured WAL/builder capacity")
	}
	if e.wal.size >= e.options.WALHigh || e.wal.size+int64(len(payload)+frameHeader) > e.options.WALHard || e.pendingBytes+extra > e.options.BuilderHard {
		e.metrics.Backpressure.Set(1)
		return ErrPressure
	}
	start := time.Now()
	err = e.wal.append(e.sequence+1, payload)
	e.metrics.Sync.Observe(time.Since(start).Seconds())
	if err != nil {
		e.metrics.WALErrors.Inc()
		e.fatal = err
		return err
	}
	recordDropped()
	e.sequence++
	// Publish the already validated state only after the WAL frame is durable.
	// WAL replay independently reconstructs exactly this state using apply.
	e.state.Series[name] = &clone
	e.pending = append(e.pending, prepared...)
	e.pendingBytes += extra
	if e.oldestWAL == 0 {
		e.oldestWAL = b.ReceivedAt
	}
	e.metrics.Samples.Add(float64(len(accepted)))
	e.updateMetrics()
	return nil
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
	return e.wal.size >= e.options.WALTarget || e.pendingBytes >= e.options.ObjectTarget
}
func (e *Engine) Flush(ctx context.Context) (err error) {
	e.publishMu.Lock()
	defer e.publishMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		return e.fatal
	}
	if e.sequence == e.state.Sequence && e.version != "" {
		if !e.options.BackgroundMaintenance {
			e.collectGarbage(ctx)
		}
		return nil
	}
	start := time.Now()
	defer func() {
		e.metrics.Flush.Observe(time.Since(start).Seconds())
		if err != nil {
			e.metrics.CommitErrors.Inc()
		}
	}()
	// The engine lock fixes a consistent checkpoint. An unavailable store stops
	// ingestion only during the bounded PUT attempt, then WAL ingestion can resume.
	next := e.state
	refDelta := make(map[string]int64)
	var publishedData, publishedIndex *pack
	var stagedTails map[string]tailPath
	next.Sequence = e.sequence
	next.Generation++
	next.Roots = make(map[string]map[int64]blob, len(e.state.Roots))
	for metric, levels := range e.state.Roots {
		next.Roots[metric] = make(map[int64]blob, len(levels))
		for l, root := range levels {
			next.Roots[metric][l] = root
		}
	}
	// Sorting packs hot metrics into dedicated objects and combines small streams.
	records := append([]entry(nil), e.pending...)
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		return a.Record.Level < b.Record.Level
	})
	if len(records) > 0 {
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
		updates := make(map[string]map[int64]indexUpdate)
		for i := 0; i < len(records); {
			j := i + 1
			for j < len(records) && records[j].Metric == records[i].Metric && records[j].Record.Level == records[i].Record.Level {
				j++
			}
			metric, level := records[i].Metric, records[i].Record.Level
			if updates[metric] == nil {
				updates[metric] = map[int64]indexUpdate{}
			}
			update := indexUpdate{}
			part := make([]hta.Record, 0, maxDataBlockRecords)
			// Fill the last partial aggregate block across checkpoints. Reading
			// only this bounded tail keeps coarse levels independent of flush
			// frequency. Raw blocks are appended; published blobs remain immutable.
			if level > 0 && !e.options.AppendOnlyAggregates {
				tail, tailErr := e.lastIndexEntry(ctx, next.Roots[metric][level])
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
					if records[i].Record.Time <= tail.Last {
						return fmt.Errorf("aggregate tail overlaps new records")
					}
					update.replaceTail = true
				}
			}
			for k := i; k < j; {
				end := min(k+maxDataBlockRecords-len(part), j)
				for ; k < end; k++ {
					part = append(part, records[k].Record)
				}
				b, encErr := encode(part)
				if encErr != nil {
					return encErr
				}
				item := indexEntry{First: part[0].Time, Last: part[len(part)-1].LastTime(), Blob: dataPack.add(b), Records: len(part)}
				update.items = append(update.items, item)
				dataPack.descriptors = append(dataPack.descriptors, BlockInfo{Metric: metric, Level: level, Entry: item})
				part = part[:0]
			}
			updates[metric][level] = update
			i = j
		}
		empty := ""
		if _, err = e.put(ctx, dataPack.key, dataPack.buf.Bytes(), &empty); err != nil {
			return err
		}
		e.metrics.Objects.Inc()
		e.metrics.Bytes.Add(float64(dataPack.buf.Len()))
		for metric, levels := range updates {
			for level, update := range levels {
				indexPack.metric, indexPack.level = metric, level
				root, appendErr := e.updateIndex(ctx, next.Roots[metric][level], update.items, indexPack, update.replaceTail)
				if appendErr != nil {
					return appendErr
				}
				next.Roots[metric][level] = root
				stagedTails[streamKey(metric, level)] = rightmostPath(root, indexPack.nodes)
			}
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
	b, err := encode(next)
	if err != nil {
		return err
	}
	version, err := e.put(ctx, "manifest", b, &e.version)
	if err != nil {
		// PUT may have succeeded even if its response was lost. Confirm the exact
		// checkpoint before deleting any WAL; never guess on transport failure.
		actual, v, getErr := e.get(ctx, "manifest")
		if getErr == nil && bytes.Equal(actual, b) {
			version = v
			err = nil
		} else {
			if errors.Is(err, storage.ErrConflict) {
				e.fatal = fmt.Errorf("another writer changed manifest: %w", err)
			}
			return err
		}
	}
	e.state = next
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
	e.pending = nil
	e.pendingBytes = 0
	e.oldestWAL = 0
	if err = e.wal.checkpoint(e.state.Sequence); err != nil {
		e.fatal = err
		return err
	}
	if err = e.wal.truncate(0); err != nil {
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if e.NeedsFlush() {
				if err := e.Flush(ctx); err != nil {
					slog.Error("object-store checkpoint failed; WAL retained", "error", err)
				}
			} else {
				e.mu.Lock()
				if !e.closed && e.fatal == nil {
					if !e.options.BackgroundMaintenance {
						e.collectGarbage(ctx)
					}
				}
				e.mu.Unlock()
			}
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
	return e.wal.file.Close()
}
