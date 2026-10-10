package engine

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/metricq/metricq-db-hta-s3/storage"
)

// orphanSweepInterval separates the two sweeps that must both find an object
// unreachable. Uploads are published within seconds; an object that no
// manifest references across an interval was left by an interrupted
// checkpoint or job (a failed request, a timeout, a stopped process).
const orphanSweepInterval = time.Hour

var errNonadvancingInventory = errors.New("nonadvancing inventory cursor")

// orphanSweepDue reports whether the next orphan sweep is due.
func (e *Engine) orphanSweepDue() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lastOrphanSweep.IsZero() {
		// The first sweep runs one interval after the start.
		e.lastOrphanSweep = time.Now()
		return false
	}
	return time.Since(e.lastOrphanSweep) >= orphanSweepInterval
}

// SweepOrphans lists the bucket and compares it with everything the
// published manifest reaches. Keys unreachable now and in the previous sweep
// go into the trash journal, so reclamation deletes them like any retired
// object. Listing and reachability run without the publication lock.
func (e *Engine) SweepOrphans(ctx context.Context) error {
	lister, ok := e.store.(storage.Lister)
	if !ok {
		return nil
	}
	e.mu.Lock()
	e.lastOrphanSweep = time.Now()
	if e.closed || e.fatal != nil || e.version == "" {
		e.mu.Unlock()
		return nil
	}
	generation := e.committed.Generation
	snapshot := &Engine{store: e.store, options: e.options, metrics: e.metrics, state: cloneMaintenanceManifest(e.committed), sharedCatalog: e.sharedCatalog}
	e.pin(generation)
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.unpin(generation); e.mu.Unlock() }()
	reach, protected, err := snapshot.reachableKeys(ctx)
	if err != nil {
		return err
	}
	var unreachable []string
	token := ""
	for {
		page, next, err := lister.List(ctx, "", token, 1000)
		if err != nil {
			return err
		}
		for _, key := range page {
			if !reach[key] && !hasAnyPrefix(key, protected) {
				unreachable = append(unreachable, key)
			}
		}
		if next == "" {
			break
		}
		if next == token {
			return errNonadvancingInventory
		}
		token = next
	}
	var confirmed []string
	candidates := make(map[string]bool, len(unreachable))
	for _, key := range unreachable {
		if e.orphanCandidates[key] {
			confirmed = append(confirmed, key)
		} else {
			candidates[key] = true
		}
	}
	e.orphanCandidates = candidates
	e.metrics.OrphanCandidates.Set(float64(len(candidates)))
	if len(confirmed) == 0 {
		return nil
	}
	sort.Strings(confirmed)
	err = e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
		next := cloneMaintenanceManifest(snapshot.committed)
		next.rootDirtyKnown = true
		next.Generation = snapshot.state.Generation + 1
		if err := snapshot.appendTrash(ctx, &next, confirmed, 0); err != nil {
			return manifest{}, err
		}
		return next, nil
	})
	if err != nil {
		// Still orphans next time; confirm them again then.
		for _, key := range confirmed {
			e.orphanCandidates[key] = true
		}
		return err
	}
	e.metrics.OrphansRetired.Add(float64(len(confirmed)))
	slog.Info("retired orphaned objects", "count", len(confirmed), "first", confirmed[0])
	return nil
}

func hasAnyPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// reachableKeys returns every key the manifest of this snapshot references,
// including keys awaiting deletion in the trash journal, and the staging
// prefixes of a pending compaction job (recovery removes those).
func (e *Engine) reachableKeys(ctx context.Context) (map[string]bool, []string, error) {
	m := e.state
	reach := map[string]bool{"manifest": true}
	mark := func(key string) {
		if key != "" {
			reach[key] = true
		}
	}
	for key := range metadataObjects(m.CheckpointState, m.seriesPages) {
		mark(key)
	}
	for key := range metadataObjects(m.StreamIndex, m.rootPages) {
		mark(key)
	}
	for key := range heldObjects(m.HeldState, m.heldPages) {
		mark(key)
	}
	for _, ref := range m.Held {
		mark(ref.Key)
	}
	for key := range m.Garbage {
		mark(key)
	}
	for _, key := range m.TrashCleanups {
		mark(key)
	}
	// TrashComplete only marks the end of the finished journal; its page is
	// deleted with the cleanups and never read again.
	mark(m.TrashBatch.Key)
	mark(m.TrashPending.Key)
	for ref := m.TrashHead; ref.Key != "" && ref != m.TrashComplete; {
		mark(ref.Key)
		b, err := e.readBlob(ctx, ref)
		if err != nil {
			return nil, nil, err
		}
		var page trashPage
		if err = decode(b, &page); err != nil {
			return nil, nil, err
		}
		for _, key := range page.Keys {
			mark(key)
		}
		ref = page.Prev
	}
	var protected []string
	if m.CompactionJob.Key != "" {
		mark(m.CompactionJob.Key)
		job, err := e.readJob(ctx)
		if err != nil {
			return nil, nil, err
		}
		protected = job.OutputPrefixes
	}
	// Tree pages as stored: entries keep their inventory references.
	w := catalogWriter{e: e, ctx: ctx}
	var walk func(ref blob, objects bool) error
	walk = func(ref blob, objects bool) error {
		if ref.Key == "" {
			return nil
		}
		mark(ref.Key)
		n, err := w.read(ref)
		if err != nil {
			return err
		}
		if objects {
			for _, o := range n.Items {
				mark(o.Key)
				for _, page := range o.Inventory {
					mark(page.Ref.Key)
				}
			}
		}
		for _, child := range n.Children {
			if err := walk(child.Ref, objects); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(m.Catalog, true); err != nil {
		return nil, nil, err
	}
	if err := walk(m.Candidates, false); err != nil {
		return nil, nil, err
	}
	return reach, protected, nil
}
