package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/metricq/metricq-db-hta-go/storage"
)

type CompactionOptions struct {
	Enabled          bool    `json:"enabled"`
	IntervalSeconds  int64   `json:"interval_seconds"`
	CooldownSeconds  int64   `json:"cooldown_seconds"`
	MaxJobBytes      int64   `json:"max_job_bytes"`
	MaxBlocks        int     `json:"max_blocks"`
	ObjectBytes      int64   `json:"object_bytes"`
	BytesPerSecond   int64   `json:"bytes_per_second"`
	DeadFraction     float64 `json:"dead_fraction"`
	MergeSmallBlocks bool    `json:"merge_small_blocks"`
}

func (o CompactionOptions) defaults() CompactionOptions {
	if o.IntervalSeconds == 0 {
		o.IntervalSeconds = 60
	}
	if o.MaxJobBytes == 0 {
		o.MaxJobBytes = 32 << 20
	}
	if o.MaxBlocks == 0 {
		o.MaxBlocks = 128
	}
	if o.ObjectBytes == 0 {
		o.ObjectBytes = 4 << 20
	}
	if o.BytesPerSecond == 0 {
		o.BytesPerSecond = 8 << 20
	}
	if o.DeadFraction == 0 {
		o.DeadFraction = .4
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
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		return e.fatal
	}
	next.Series = cloneManifest(e.committed).Series
	next.Sequence = e.committed.Sequence
	b, err := encode(next)
	if err != nil {
		return err
	}
	version, err := e.put(ctx, "manifest", b, &e.version)
	if err != nil {
		actual, v, getErr := e.get(ctx, "manifest")
		if getErr == nil && bytes.Equal(actual, b) {
			version = v
			err = nil
		} else {
			if errors.Is(err, storage.ErrConflict) {
				e.fatal = fmt.Errorf("maintenance publisher fenced: %w", err)
			}
			return err
		}
	}
	e.committed = cloneManifest(next)
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
		for _, ref := range index.retired {
			o, ok := changes[ref.Key]
			if !ok {
				old, found, err := e.catalogGet(ctx, e.state.Catalog, ref.Key)
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("retired block absent from catalog: %s", ref.Key)
				}
				old.Blocks = append([]BlockInfo(nil), old.Blocks...)
				o = &old
				changes[ref.Key] = o
			}
			found := false
			for i, b := range o.Blocks {
				if b.Entry.Blob == ref {
					o.LiveBytes -= ref.Length
					o.Blocks = append(o.Blocks[:i], o.Blocks[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("retired blob absent from catalog: %s@%d", ref.Key, ref.Offset)
			}
			o.Modified = now
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
		next := cloneManifest(e.committed)
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
		return nil
	}
	next := cloneManifest(e.committed)
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
	gcTicker := time.NewTicker(time.Second)
	defer gcTicker.Stop()
	options := e.options.Compaction.defaults()
	compactTicker := time.NewTicker(time.Duration(options.IntervalSeconds) * time.Second)
	defer compactTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-gcTicker.C:
			if err := e.recoverCompaction(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("compaction recovery failed", "error", err)
			}
			if err := e.Reclaim(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("background reclaim failed", "error", err)
			}
		case <-compactTicker.C:
			if options.Enabled {
				if err := e.CompactOnce(ctx); err != nil && ctx.Err() == nil {
					slog.Warn("background compaction failed", "error", err)
				}
			}
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
	e.metrics.GCPending.Set(float64(m.TrashObjects))
	e.metrics.LiveObjectBytes.Set(float64(m.LiveObjectBytes))
	e.metrics.DeadObjectBytes.Set(float64(m.StoredObjectBytes - m.LiveObjectBytes))
}

type MaintenanceStatus struct {
	Generation     uint64 `json:"generation"`
	Checkpoint     uint64 `json:"checkpoint_sequence"`
	WALHead        uint64 `json:"wal_head_sequence"`
	PendingObjects int64  `json:"pending_objects"`
	LiveBytes      int64  `json:"live_bytes"`
	DeadBytes      int64  `json:"dead_bytes"`
	JobPending     bool   `json:"job_pending"`
}

func (e *Engine) MaintenanceStatus() MaintenanceStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	pending := e.state.TrashObjects
	if e.state.TrashCleanup != "" {
		pending++
	}
	return MaintenanceStatus{Generation: e.state.Generation, Checkpoint: e.state.Sequence, WALHead: e.sequence, PendingObjects: pending, LiveBytes: e.state.LiveObjectBytes, DeadBytes: e.state.StoredObjectBytes - e.state.LiveObjectBytes, JobPending: e.state.CompactionJob.Key != ""}
}
