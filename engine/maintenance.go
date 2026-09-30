package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/metricq/metricq-db-hta-s3/storage"
)

// CompactionOptions configures background compaction; see
// docs/operations/tuning.md for the effect of each option.
type CompactionOptions struct {
	// Continuous resumes a backlog immediately after the cycle start window,
	// and wakes on successful checkpoints. I/O pacing and WAL pressure still apply.
	Continuous bool `json:"compaction_continuous"`
	// Enabled runs compaction cycles.
	Enabled bool `json:"compaction_enabled"`
	// CycleIntervalSeconds is the period of compaction cycles.
	CycleIntervalSeconds int64 `json:"compaction_cycle_interval_seconds"`
	// CycleMaxSeconds is the window in which a cycle starts consecutive jobs.
	CycleMaxSeconds int64 `json:"compaction_cycle_max_seconds"`
	// JobTimeoutSeconds bounds one job.
	JobTimeoutSeconds int64 `json:"compaction_job_timeout_seconds"`
	// JobMaxBlocks bounds the source blocks of one job.
	JobMaxBlocks int `json:"compaction_job_max_blocks"`
	// JobMaxBytes bounds the source bytes of one job.
	JobMaxBytes int64 `json:"compaction_job_max_bytes"`
	// OutputObjectBytes is the target size of output packs.
	OutputObjectBytes int64 `json:"compaction_output_object_bytes"`
	// IOBytesPerSecond limits all maintenance reads and writes.
	IOBytesPerSecond int64 `json:"compaction_io_bytes_per_second"`
	// MergeEnabled merges adjacent small blocks of a stream.
	MergeEnabled bool `json:"compaction_merge_enabled"`
	// MergeCooldownSeconds excludes younger objects from merges.
	MergeCooldownSeconds int64 `json:"compaction_merge_cooldown_seconds"`
	// ReclaimDeadFraction is the dead-byte share at which objects are evacuated.
	ReclaimDeadFraction float64 `json:"compaction_reclaim_dead_fraction"`
	// LocalityMinRanges is the number of physical ranges a metric level must
	// span before it is laid out contiguously; no temporal grouping.
	LocalityMinRanges int `json:"compaction_locality_min_ranges"`
	// LocalityDisabled turns level locality off.
	LocalityDisabled bool `json:"compaction_locality_disabled"`
}

func (o CompactionOptions) defaults() CompactionOptions {
	if o.LocalityMinRanges == 0 {
		o.LocalityMinRanges = 4
	}
	if o.CycleMaxSeconds == 0 {
		o.CycleMaxSeconds = 10
	}
	if o.JobTimeoutSeconds == 0 {
		o.JobTimeoutSeconds = 60
	}
	if o.CycleIntervalSeconds == 0 {
		o.CycleIntervalSeconds = 60
	}
	if o.JobMaxBytes == 0 {
		o.JobMaxBytes = 32 << 20
	}
	if o.JobMaxBlocks == 0 {
		o.JobMaxBlocks = 128
	}
	if o.OutputObjectBytes == 0 {
		o.OutputObjectBytes = 4 << 20
	}
	if o.IOBytesPerSecond == 0 {
		o.IOBytesPerSecond = 8 << 20
	}
	if o.ReclaimDeadFraction == 0 {
		o.ReclaimDeadFraction = .4
	}
	return o
}

type trashPage struct {
	Prev       blob
	Generation uint64
	Keys       []string
	NotBefore  int64
}

func (e *Engine) appendTrash(ctx context.Context, next *manifest, keys []string, notBefore int64) error {
	seen := make(map[string]bool)
	var unique []string
	for _, key := range keys {
		if key != "" && !seen[key] {
			seen[key] = true
			unique = append(unique, key)
		}
	}
	for i := 0; i < len(unique); i += 64 {
		p, err := e.newMetadataPack("trash")
		if err != nil {
			return err
		}
		part := unique[i:min(i+64, len(unique))]
		b, err := encode(trashPage{Prev: next.TrashHead, Generation: next.Generation, Keys: part, NotBefore: notBefore})
		if err != nil {
			return err
		}
		ref := p.add(b)
		absent := ""
		if _, err = e.put(ctx, p.key, p.buf.Bytes(), &absent); err != nil {
			return err
		}
		next.TrashHead = ref
		next.TrashObjects += int64(len(part))
	}
	return nil
}

// Caller holds mu. Compaction/GC publications retain committed HTA and WAL
// sequence while the live series may already include newer fsynced samples.
func (e *Engine) publishMaintenance(ctx context.Context, next manifest) error {
	if !e.publishMu.TryLock() {
		return ErrPressure
	}
	defer e.publishMu.Unlock()
	return e.publishMaintenanceLocked(ctx, next, e.preparationStore())
}

// Caller holds publishMu, then mu. Immutable block copying happens before
// acquiring publishMu; only metadata rebasing/publication serialize with Flush.
func (e *Engine) publishMaintenanceLocked(ctx context.Context, next manifest, store storage.Store) error {
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		return e.fatal
	}
	next.Series = e.committed.Series
	next.Sequence = e.committed.Sequence
	base := e.committed
	expected := e.version
	publisher := &Engine{store: store, metrics: e.metrics, options: e.options}
	e.mu.Unlock()
	b, err := publisher.encodeManifest(ctx, &next, base)
	if err != nil {
		e.mu.Lock()
		return err
	}
	committed := cloneMaintenanceManifest(next)
	version, err := publisher.put(ctx, "manifest", b, &expected)
	if err != nil {
		actual, v, getErr := publisher.get(ctx, "manifest")
		if getErr == nil && bytes.Equal(actual, b) {
			version = v
			err = nil
		} else {
			if errors.Is(err, storage.ErrConflict) {
				e.mu.Lock()
				e.fatal = fmt.Errorf("maintenance publisher fenced: %w", err)
				e.mu.Unlock()
			}
			e.mu.Lock()
			return err
		}
	}
	e.mu.Lock()
	e.committed = committed
	live := e.state.Series
	liveRoots := e.state.Roots
	e.state = next
	e.state.Series = live
	for name, roots := range liveRoots {
		if _, ok := e.state.Roots[name]; !ok {
			e.state.Roots[name] = roots
		}
	}
	e.version = version
	e.updateMaintenanceMetrics(next)
	return nil
}
func (e *Engine) catalogCheckpoint(ctx context.Context, next *manifest, data, index *pack) error {
	if !e.state.CatalogReady {
		next.CatalogReady = true
	}
	changes := make(map[string]*ObjectInfo)
	now := time.Now().UnixNano()
	for _, p := range []*pack{data, index} {
		if p == nil {
			continue
		}
		o := &ObjectInfo{Key: p.key, Size: int64(p.buf.Len()), Blocks: append([]BlockInfo(nil), p.descriptors...), Created: now, Modified: now}
		for _, b := range o.Blocks {
			o.LiveBytes += b.Entry.Blob.Length
		}
		changes[p.key] = o
	}
	var trash []string
	if index != nil {
		retired := make(map[blob]bool, len(index.retired))
		for _, ref := range index.retired {
			retired[ref] = true
		}
		if err := e.retireCatalogBlocks(ctx, changes, retired); err != nil {
			return err
		}
	}
	for key, o := range changes {
		if len(o.Blocks) == 0 {
			trash = append(trash, key)
			changes[key] = nil
		}
	}
	retired, err := e.catalogChanges(ctx, next, changes)
	if err != nil {
		return err
	}
	trash = append(trash, retired...)
	return e.appendTrash(ctx, next, trash, 0)
}
func (e *Engine) objectSize(ctx context.Context, key string) (int64, error) {
	if s, ok := e.store.(storage.Statter); ok {
		return s.Stat(ctx, key)
	}
	b, _, err := e.get(ctx, key)
	return int64(len(b)), err
}

// The one-time bootstrap streams descriptor batches, so metadata memory is
// bounded. Partial catalog checkpoints are restartable by deduplicating blobs.
func (e *Engine) bootstrapCatalog(ctx context.Context) error {
	if e.state.CatalogReady {
		return nil
	}
	changes := make(map[string]*ObjectInfo)
	bytesPending := 0
	flush := func() error {
		if len(changes) == 0 {
			return nil
		}
		next := cloneMaintenanceManifest(e.committed)
		next.rootDirtyKnown = true
		next.Generation = e.state.Generation + 1
		retired, err := e.catalogChanges(ctx, &next, changes)
		if err != nil {
			return err
		}
		if err = e.appendTrash(ctx, &next, retired, 0); err != nil {
			return err
		}
		if err = e.publishMaintenance(ctx, next); err != nil {
			return err
		}
		changes = make(map[string]*ObjectInfo)
		bytesPending = 0
		return nil
	}
	add := func(b BlockInfo) error {
		ref := b.Entry.Blob
		o := changes[ref.Key]
		if o == nil {
			old, ok, err := e.catalogGet(ctx, e.state.Catalog, ref.Key)
			if err != nil {
				return err
			}
			if !ok {
				size, err := e.objectSize(ctx, ref.Key)
				if err != nil {
					return err
				}
				old = ObjectInfo{Key: ref.Key, Size: size, Created: time.Now().UnixNano(), Modified: time.Now().UnixNano()}
			}
			old.Blocks = append([]BlockInfo(nil), old.Blocks...)
			o = &old
			changes[ref.Key] = o
		}
		for _, old := range o.Blocks {
			if old.Entry.Blob == ref {
				return nil
			}
		}
		o.Blocks = append(o.Blocks, b)
		o.LiveBytes += ref.Length
		bytesPending += 256 + len(b.Metric)
		if bytesPending >= 1<<20 {
			return flush()
		}
		return nil
	}
	var walk func(string, int64, blob) error
	walk = func(metric string, level int64, ref blob) error {
		if ref.Key == "" {
			return nil
		}
		n, err := e.readNode(ctx, ref)
		if err != nil {
			return err
		}
		if err = add(BlockInfo{Metric: metric, Level: level, Index: true, Entry: indexEntry{First: n.Entries[0].First, Last: n.Entries[len(n.Entries)-1].Last, Blob: ref}}); err != nil {
			return err
		}
		for _, edge := range n.Entries {
			if n.Leaf {
				err = add(BlockInfo{Metric: metric, Level: level, Entry: edge})
			} else {
				err = walk(metric, level, edge.Blob)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	// Bootstrap publication changes only catalog roots; save the history roots.
	roots := e.state.Roots
	names := make([]string, 0, len(roots))
	for name := range roots {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for level, root := range roots[name] {
			if err := walk(name, level, root); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if e.version == "" {
		e.state.CatalogReady = true
		e.state.MaintenanceStatsReady = true
		return nil
	}
	next := cloneMaintenanceManifest(e.committed)
	next.rootDirtyKnown = true
	next.Generation = e.state.Generation + 1
	next.CatalogReady = true
	var legacy []string
	for key := range e.state.Garbage {
		legacy = append(legacy, key)
	}
	next.Garbage = nil
	if err := e.appendTrash(ctx, &next, legacy, 0); err != nil {
		return err
	}
	return e.publishMaintenance(ctx, next)
}

// RunMaintenance owns compaction and physical deletion. Neither operation is
// invoked by ingest/history handlers or the ordinary flush loop in this mode.
func (e *Engine) RunMaintenance(ctx context.Context) {
	if err := e.bootstrapTails(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("stream tail classification failed; fragment metrics unavailable", "error", err)
	}
	gcTicker := time.NewTicker(time.Second)
	defer gcTicker.Stop()
	options := e.options.CompactionOptions.defaults()
	compactTicker := time.NewTicker(time.Duration(options.CycleIntervalSeconds) * time.Second)
	defer compactTicker.Stop()
	cycle := func() {
		if !options.Enabled {
			return
		}
		deadline := time.Now().Add(time.Duration(options.CycleMaxSeconds) * time.Second)
		continueWork := false
		for ctx.Err() == nil && time.Now().Before(deadline) {
			continueWork = false
			e.mu.Lock()
			before := e.compactionCompletions
			e.mu.Unlock()
			err := e.CompactOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("background compaction failed", "error", err)
				}
				break
			}
			if e.reclaimDue() {
				if err = e.Reclaim(ctx); err != nil {
					break
				}
			}
			e.mu.Lock()
			progress := e.compactionCompletions != before
			pending := e.state.CompactionJob.Key != ""
			more := e.compactionScanMore
			pressure := e.wal.total() >= e.options.WALHigh || e.unsavedBytes() >= e.options.CheckpointUnsavedBytes
			e.mu.Unlock()
			continueWork = (progress || more) && !pending && !pressure
			if !continueWork {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
		}
		if options.Continuous && continueWork && ctx.Err() == nil {
			select {
			case e.maintenanceWanted <- struct{}{}:
			default:
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-gcTicker.C:
			if err := e.recoverCompaction(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("compaction recovery failed", "error", err)
			}
			if e.reclaimDue() {
				if err := e.Reclaim(ctx); err != nil && ctx.Err() == nil {
					slog.Warn("background reclaim failed", "error", err)
				}
			}
		case <-compactTicker.C:
			cycle()
		case <-e.maintenanceWanted:
			cycle()
		}
	}
}

func (e *Engine) newMetadataPack(prefix string) (*pack, error) {
	if e.activeMaintenance != "" {
		prefix += "/compact-" + e.activeMaintenance
	}
	p, err := newPack(prefix)
	if err == nil && e.activeMaintenance != "" {
		e.stagingKeys = append(e.stagingKeys, p.key)
	}
	return p, err
}
func (e *Engine) updateMaintenanceMetrics(m manifest) {
	pending := m.TrashObjects
	if m.TrashCleanup != "" {
		pending++
	}
	pending += int64(len(m.TrashCleanups))
	e.metrics.GCPending.Set(float64(pending))
	e.metrics.CandidateObjects.Set(float64(m.CandidateObjects))
	e.metrics.SmallBlocks.Set(float64(m.SmallBlocks))
	e.metrics.SmallBlockBytes.Set(float64(m.SmallBlockBytes))
	e.updateTailMetrics(m)
	if !m.MaintenanceStatsReady {
		e.metrics.CandidateObjects.Set(math.NaN())
		e.metrics.SmallBlocks.Set(math.NaN())
		e.metrics.SmallBlockBytes.Set(math.NaN())
	}
	e.metrics.LiveObjects.Set(float64(m.LiveObjects))
	jobPending := 0.0
	if m.CompactionJob.Key != "" {
		jobPending = 1
	}
	e.metrics.CompactionJobPending.Set(jobPending)
	e.metrics.LiveObjectBytes.Set(float64(m.LiveObjectBytes))
	e.metrics.DeadObjectBytes.Set(float64(m.StoredObjectBytes - m.LiveObjectBytes))
}

// MaintenanceStatus summarizes checkpoint and maintenance state.
type MaintenanceStatus struct {
	SmallBlockStatsAvailable bool   `json:"small_block_stats_available"`
	SmallBlocks              int64  `json:"small_data_blocks"`
	SmallBlockBytes          int64  `json:"small_data_block_bytes"`
	Generation               uint64 `json:"generation"`
	Checkpoint               uint64 `json:"checkpoint_sequence"`
	WALHead                  uint64 `json:"wal_head_sequence"`
	PendingObjects           int64  `json:"pending_objects"`
	LiveBytes                int64  `json:"live_bytes"`
	DeadBytes                int64  `json:"dead_bytes"`
	JobPending               bool   `json:"job_pending"`
}

// MaintenanceStatus returns the current checkpoint and maintenance state.
func (e *Engine) MaintenanceStatus() MaintenanceStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	pending := e.state.TrashObjects
	if e.state.TrashCleanup != "" {
		pending++
	}
	pending += int64(len(e.state.TrashCleanups))
	return MaintenanceStatus{SmallBlockStatsAvailable: e.state.MaintenanceStatsReady, SmallBlocks: e.state.SmallBlocks, SmallBlockBytes: e.state.SmallBlockBytes, Generation: e.state.Generation, Checkpoint: e.state.Sequence, WALHead: e.sequence, PendingObjects: pending, LiveBytes: e.state.LiveObjectBytes, DeadBytes: e.state.StoredObjectBytes - e.state.LiveObjectBytes, JobPending: e.state.CompactionJob.Key != ""}
}
