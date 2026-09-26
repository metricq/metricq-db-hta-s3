package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/metricq/metricq-db-hta-go/hta"
	"github.com/metricq/metricq-db-hta-go/storage"
)

type CompactionJob struct {
	CleanupAfter int64

	ID             string
	Stage          string
	Generation     uint64
	Inputs         []BlockInfo
	OutputPrefixes []string
	Created        int64
}
type replacement struct {
	Entry indexEntry
	Drop  bool
	Index bool
}

func (e *Engine) readJob(ctx context.Context) (CompactionJob, error) {
	var job CompactionJob
	if e.state.CompactionJob.Key == "" {
		return job, nil
	}
	b, err := e.readBlob(ctx, e.state.CompactionJob)
	if err == nil {
		err = decode(b, &job)
	}
	return job, err
}
func (e *Engine) writeJob(ctx context.Context, job CompactionJob) (blob, error) {
	p, err := newPack("jobs")
	if err != nil {
		return blob{}, err
	}
	b, err := encode(job)
	if err != nil {
		return blob{}, err
	}
	ref := p.add(b)
	absent := ""
	_, err = e.put(ctx, p.key, p.buf.Bytes(), &absent)
	return ref, err
}
func (e *Engine) pin(generation uint64) { e.pins[generation]++ }
func (e *Engine) unpin(generation uint64) {
	e.pins[generation]--
	if e.pins[generation] == 0 {
		delete(e.pins, generation)
	}
}

// Each coordinator has one in-process worker. This mutex never guards ingestion.
// Persist all possible data/index output names before uploading a job's blocks.
func (e *Engine) reserveCompaction(ctx context.Context) (CompactionJob, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return CompactionJob{}, fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		e.mu.Unlock()
		return CompactionJob{}, e.fatal
	}
	if !e.options.BackgroundMaintenance {
		e.mu.Unlock()
		return CompactionJob{}, fmt.Errorf("background maintenance not enabled")
	}
	if e.state.CompactionJob.Key != "" {
		e.mu.Unlock()
		return CompactionJob{}, fmt.Errorf("interrupted compaction requires recovery")
	}
	if e.version == "" || !e.state.CatalogReady {
		e.mu.Unlock()
		return CompactionJob{}, nil
	}
	options := e.options.Compaction.defaults()
	if e.wal.size >= e.options.WALHigh || e.pendingBytes >= e.options.ObjectTarget {
		e.mu.Unlock()
		return CompactionJob{}, nil
	}
	generation := e.state.Generation
	snapshot := &Engine{store: e.store, options: e.options, metrics: e.metrics, state: manifest{Catalog: e.state.Catalog, Candidates: e.state.Candidates}}
	scanAfter := e.candidateCursor
	e.pin(generation)
	e.mu.Unlock()
	keepPin := false
	defer func() {
		if !keepPin {
			e.mu.Lock()
			e.unpin(generation)
			e.mu.Unlock()
		}
	}()
	var inputs []BlockInfo
	var copied int64
	cutoff := time.Now().Add(-time.Duration(options.CooldownSeconds) * time.Second).UnixNano()
	var selectErr error
	cursor, err := snapshot.catalogScan(ctx, snapshot.state.Candidates, scanAfter, 256, func(candidate ObjectInfo) bool {
		object, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, candidate.Target)
		if err != nil {
			selectErr = err
			return false
		}
		if !ok || object.Modified > cutoff || object.Size == 0 || float64(object.Size-object.LiveBytes)/float64(object.Size) < options.DeadFraction {
			return true
		}
		// Select complete objects when they fit, otherwise a bounded evacuation.
		for _, b := range object.Blocks {
			if b.Entry.Blob.Length > options.MaxJobBytes-copied || len(inputs) >= options.MaxBlocks {
				continue
			}
			inputs = append(inputs, b)
			copied += b.Entry.Blob.Length
		}
		return len(inputs) < options.MaxBlocks && copied < options.MaxJobBytes
	})
	e.mu.Lock()
	defer e.mu.Unlock()
	e.candidateCursor = cursor
	if e.closed {
		return CompactionJob{}, fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		return CompactionJob{}, e.fatal
	}
	if e.state.CompactionJob.Key != "" {
		return CompactionJob{}, fmt.Errorf("job already active")
	}
	if err := ctx.Err(); err != nil {
		return CompactionJob{}, err
	}
	if err != nil {
		return CompactionJob{}, err
	}
	if selectErr != nil {
		return CompactionJob{}, selectErr
	}
	if len(inputs) == 0 {
		return CompactionJob{}, nil
	}
	idPack, err := newPack("job")
	if err != nil {
		return CompactionJob{}, err
	}
	id := strings.TrimPrefix(idPack.key, "job/")
	job := CompactionJob{ID: id, Stage: "reserved", Generation: generation, Inputs: inputs, Created: time.Now().UnixNano()}
	// Register all staging namespaces before uploads, including COW metadata.
	for _, prefix := range []string{"data/", "index/", "catalog/", "candidates/", "trash/"} {
		job.OutputPrefixes = append(job.OutputPrefixes, prefix+"compact-"+id+"/")
	}
	ref, err := e.writeJob(ctx, job)
	if err != nil {
		return CompactionJob{}, err
	}
	next := cloneManifest(e.committed)
	next.Generation = e.state.Generation + 1
	next.CompactionJob = ref
	if err = e.publishMaintenance(ctx, next); err != nil {
		return CompactionJob{}, err
	}
	keepPin = true
	return job, nil
}

type rateBudget struct {
	start       time.Time
	bytes, rate int64
}

func (b *rateBudget) take(ctx context.Context, n int64) error {
	b.bytes += n
	if b.rate <= 0 {
		return fmt.Errorf("nonpositive compaction rate")
	}
	due := b.start.Add(time.Duration(float64(b.bytes) / float64(b.rate) * float64(time.Second)))
	wait := time.Until(due)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (e *Engine) copyJob(ctx context.Context, job CompactionJob) (map[blob]replacement, []*pack, error) {
	options := e.options.Compaction.defaults()
	reader := &Engine{store: e.store, metrics: e.metrics, options: e.options}
	budget := rateBudget{start: time.Now(), rate: options.BytesPerSecond}
	replacements := make(map[blob]replacement)
	inputs := append([]BlockInfo(nil), job.Inputs...)
	sort.Slice(inputs, func(i, j int) bool {
		a, b := inputs[i], inputs[j]
		if a.Index != b.Index {
			return !a.Index
		}
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		if a.Level != b.Level {
			return a.Level < b.Level
		}
		return a.Entry.First < b.Entry.First
	})
	var packs []*pack
	var current *pack
	counts := map[string]int{"data": 0, "index": 0}
	upload := func() error {
		if current == nil {
			return nil
		}
		if err := budget.take(ctx, int64(current.buf.Len())); err != nil {
			return err
		}
		absent := ""
		if _, err := reader.put(ctx, current.key, current.buf.Bytes(), &absent); err != nil {
			return err
		}
		e.metrics.CompactionWriteBytes.Add(float64(current.buf.Len()))
		packs = append(packs, current)
		current = nil
		return nil
	}
	add := func(info BlockInfo, encoded []byte) (indexEntry, error) {
		prefix := "data"
		if info.Index {
			prefix = "index"
		}
		if current != nil && (!strings.HasPrefix(current.key, prefix+"/") || int64(current.buf.Len()+len(encoded)) > options.ObjectBytes) {
			if err := upload(); err != nil {
				return indexEntry{}, err
			}
		}
		if current == nil {
			current = &pack{key: fmt.Sprintf("%s/compact-%s/%d", prefix, job.ID, counts[prefix])}
			counts[prefix]++
		}
		info.Entry.Blob = current.add(encoded)
		current.descriptors = append(current.descriptors, info)
		return info.Entry, nil
	}
	for i := 0; i < len(inputs); {
		b := inputs[i]
		if err := budget.take(ctx, b.Entry.Blob.Length); err != nil {
			return nil, nil, err
		}
		encoded, err := reader.readBlob(ctx, b.Entry.Blob)
		if err == nil {
			e.metrics.CompactionReadBytes.Add(float64(len(encoded)))
		}
		if err != nil {
			return nil, nil, err
		}
		end := i + 1
		// Only adjacent blocks proven consecutive in the pinned index may merge.
		if options.MergeSmallBlocks && !b.Index && b.Entry.Records > 0 && b.Entry.Records < maxDataBlockRecords {
			for end < len(inputs) && !inputs[end].Index && inputs[end].Metric == b.Metric && inputs[end].Level == b.Level && inputs[end].Entry.Records > 0 && b.Entry.Records+inputs[end].Entry.Records <= maxDataBlockRecords {
				b.Entry.Records += inputs[end].Entry.Records
				end++
			}
			if end > i+1 {
				e.mu.Lock()
				root := e.state.Roots[b.Metric][b.Level]
				e.mu.Unlock()
				var refs []blob
				err = reader.indexRange(ctx, root, inputs[i].Entry.First, inputs[end-1].Entry.Last, &refs)
				if err != nil {
					return nil, nil, err
				}
				consecutive := len(refs) == end-i
				for j, ref := range refs {
					if j >= end-i || ref != inputs[i+j].Entry.Blob {
						consecutive = false
					}
				}
				if !consecutive {
					end = i + 1
					b = inputs[i]
				}
			}
		}
		if end > i+1 {
			var records []hta.Record
			if err = decode(encoded, &records); err != nil {
				return nil, nil, err
			}
			for j := i + 1; j < end; j++ {
				if err = budget.take(ctx, inputs[j].Entry.Blob.Length); err != nil {
					return nil, nil, err
				}
				bytes, err := reader.readBlob(ctx, inputs[j].Entry.Blob)
				if err == nil {
					e.metrics.CompactionReadBytes.Add(float64(len(bytes)))
				}
				if err != nil {
					return nil, nil, err
				}
				var extra []hta.Record
				if err = decode(bytes, &extra); err != nil {
					return nil, nil, err
				}
				records = append(records, extra...)
			}
			if len(records) != b.Entry.Records || len(records) > maxDataBlockRecords {
				return nil, nil, fmt.Errorf("invalid consolidation record count")
			}
			for j, r := range records {
				if r.Level != b.Level || (j > 0 && r.Time <= records[j-1].LastTime()) {
					return nil, nil, fmt.Errorf("invalid consolidation order")
				}
			}
			b.Entry.Last = inputs[end-1].Entry.Last
			encoded, err = encode(records)
			if err != nil {
				return nil, nil, err
			}
		}
		target, err := add(b, encoded)
		if err != nil {
			return nil, nil, err
		}
		replacements[inputs[i].Entry.Blob] = replacement{Entry: target, Index: b.Index}
		for j := i + 1; j < end; j++ {
			replacements[inputs[j].Entry.Blob] = replacement{Drop: true, Entry: inputs[j].Entry}
		}
		i = end
	}
	if err := upload(); err != nil {
		return nil, nil, err
	}
	return replacements, packs, nil
}

// Rewrites just the affected historical paths against the current roots.
func (e *Engine) replaceHistorical(ctx context.Context, root blob, replacements map[blob]replacement, p *pack, used map[blob]bool, pages *int) ([]indexEntry, error) {
	*pages++
	if *pages > 4096 {
		return nil, fmt.Errorf("compaction index page budget exceeded")
	}
	n, err := e.readNode(ctx, root)
	if err != nil {
		return nil, err
	}
	changed := false
	var entries []indexEntry
	for _, edge := range n.Entries {
		if n.Leaf {
			if r, ok := replacements[edge.Blob]; ok {
				changed = true
				if !r.Drop {
					entries = append(entries, r.Entry)
					used[r.Entry.Blob] = true
				}
			} else {
				entries = append(entries, edge)
			}
		} else {
			// Metadata page addresses are immutable, but a copied page may contain
			// descendants needing replacement; descend only into relevant time ranges.
			relevant := false
			for old, r := range replacements {
				if old == root {
					continue
				}
				if old != root && ((r.Index && r.Entry.First >= edge.First && r.Entry.Last <= edge.Last) || (!r.Index && r.Entry.Last >= edge.First && r.Entry.First <= edge.Last)) {
					relevant = true
					break
				}
			}
			if _, ok := replacements[edge.Blob]; ok {
				relevant = true
			}
			if !relevant {
				entries = append(entries, edge)
				continue
			}
			children, err := e.replaceHistorical(ctx, edge.Blob, replacements, p, used, pages)
			if err != nil {
				return nil, err
			}
			if len(children) != 1 || children[0] != edge {
				changed = true
			}
			entries = append(entries, children...)
		}
	}
	if changed {
		p.retired = append(p.retired, root)
		if len(entries) == 0 {
			return nil, nil
		}
		return writeNodes(p, n.Leaf, entries)
	}
	if r, ok := replacements[root]; ok {
		if r.Drop {
			return nil, fmt.Errorf("cannot drop an index page")
		}
		p.retired = append(p.retired, root)
		used[r.Entry.Blob] = true
		return []indexEntry{{First: n.Entries[0].First, Last: n.Entries[len(n.Entries)-1].Last, Blob: r.Entry.Blob}}, nil
	}
	return []indexEntry{{First: n.Entries[0].First, Last: n.Entries[len(n.Entries)-1].Last, Blob: root}}, nil
}

func (e *Engine) prepareCompaction(ctx context.Context, job CompactionJob, replacements map[blob]replacement, packs []*pack, discarded []string) (manifest, error) {
	active, err := e.readJob(ctx)
	if err != nil {
		return manifest{}, err
	}
	if active.ID != job.ID || active.Stage != "reserved" {
		return manifest{}, fmt.Errorf("compaction job fenced")
	}

	// Exact current catalog references validate data AND index source identity.
	for _, input := range job.Inputs {
		object, ok, err := e.catalogGet(ctx, e.state.Catalog, input.Entry.Blob.Key)
		if err != nil {
			return manifest{}, err
		}
		found := false
		if ok {
			for _, b := range object.Blocks {
				if b == input {
					found = true
					break
				}
			}
		}
		if !found {
			return manifest{}, fmt.Errorf("compaction input replaced concurrently")
		}
	}
	e.activeMaintenance = job.ID
	defer func() { e.activeMaintenance = "" }()
	next := cloneManifest(e.committed)
	next.Generation = e.state.Generation + 1
	p, err := newPack("index/compact-" + job.ID)
	if err != nil {
		return manifest{}, err
	}
	e.stagingKeys = append(e.stagingKeys, p.key)
	used := make(map[blob]bool)
	affected := make(map[string]map[int64]map[blob]replacement)
	for _, input := range job.Inputs {
		if affected[input.Metric] == nil {
			affected[input.Metric] = make(map[int64]map[blob]replacement)
		}
		if affected[input.Metric][input.Level] == nil {
			affected[input.Metric][input.Level] = make(map[blob]replacement)
		}
		affected[input.Metric][input.Level][input.Entry.Blob] = replacements[input.Entry.Blob]
	}
	pages := 0
	for metric, levels := range affected {
		for level, repl := range levels {
			p.metric, p.level = metric, level
			edges, err := e.replaceHistorical(ctx, next.Roots[metric][level], repl, p, used, &pages)
			if err != nil {
				return manifest{}, err
			}
			for len(edges) > 1 {
				edges, err = writeNodes(p, false, edges)
				if err != nil {
					return manifest{}, err
				}
			}
			if len(edges) == 0 {
				return manifest{}, fmt.Errorf("compaction removed entire history")
			}
			next.Roots[metric][level] = edges[0].Blob
		}
	}
	if int64(p.buf.Len()) > e.options.Compaction.MaxJobBytes {
		return manifest{}, fmt.Errorf("compaction index output budget exceeded")
	}
	if p.buf.Len() > 0 {
		absent := ""
		if _, err = e.put(ctx, p.key, p.buf.Bytes(), &absent); err != nil {
			return manifest{}, err
		}
		e.metrics.CompactionWriteBytes.Add(float64(p.buf.Len()))
		packs = append(packs, p)
	}
	// Include all retired original blocks, but each rewritten index node once.
	retired := make(map[blob]bool)
	for _, input := range job.Inputs {
		retired[input.Entry.Blob] = true
	}
	for _, ref := range p.retired {
		retired[ref] = true
	}
	changes := make(map[string]*ObjectInfo)
	now := time.Now().UnixNano()
	for _, output := range packs {
		o := &ObjectInfo{Key: output.key, Size: int64(output.buf.Len()), Created: now, Modified: now}
		for _, b := range output.descriptors {
			if output == p || used[b.Entry.Blob] {
				o.Blocks = append(o.Blocks, b)
				o.LiveBytes += b.Entry.Blob.Length
			}
		}
		changes[o.Key] = o
	}
	for ref := range retired {
		o := changes[ref.Key]
		if o == nil {
			old, ok, err := e.catalogGet(ctx, e.state.Catalog, ref.Key)
			if err != nil {
				return manifest{}, err
			}
			if !ok {
				return manifest{}, fmt.Errorf("missing retired catalog object")
			}
			old.Blocks = append([]BlockInfo(nil), old.Blocks...)
			o = &old
			changes[ref.Key] = o
		}
		found := false
		for i, b := range o.Blocks {
			if b.Entry.Blob == ref {
				o.Blocks = append(o.Blocks[:i], o.Blocks[i+1:]...)
				o.LiveBytes -= ref.Length
				found = true
				break
			}
		}
		if !found {
			return manifest{}, fmt.Errorf("missing retired compaction block")
		}
		o.Modified = now
	}
	trash := append([]string(nil), discarded...)
	for key, o := range changes {
		if len(o.Blocks) == 0 {
			changes[key] = nil
			trash = append(trash, key)
		}
	}
	metadataTrash, err := e.catalogChanges(ctx, &next, changes)
	if err != nil {
		return manifest{}, err
	}
	trash = append(trash, metadataTrash...)
	trash = append(trash, e.state.CompactionJob.Key)
	next.CompactionJob = blob{}
	if err = e.appendTrash(ctx, &next, trash, 0); err != nil {
		return manifest{}, err
	}
	return next, nil
}

func (e *Engine) applyCompaction(ctx context.Context, job CompactionJob, replacements map[blob]replacement, packs []*pack) error {
	var discarded []string
	for attempt := 0; attempt < 2; attempt++ {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return fmt.Errorf("engine closed")
		}
		if e.fatal != nil {
			err := e.fatal
			e.mu.Unlock()
			return err
		}
		if e.wal.size >= e.options.WALHigh {
			e.mu.Unlock()
			return ErrPressure
		}
		version := e.version
		snapshot := &Engine{store: e.store, options: e.options, metrics: e.metrics, state: cloneManifest(e.committed), committed: cloneManifest(e.committed), sharedNodes: e.sharedNodes, activeMaintenance: job.ID}
		e.mu.Unlock()
		next, err := snapshot.prepareCompaction(ctx, job, replacements, packs, discarded)
		if err != nil {
			return err
		}
		e.mu.Lock()
		if e.version != version {
			e.mu.Unlock()
			discarded = append(discarded, snapshot.stagingKeys...)
			e.metrics.CompactionConflicts.Inc()
			continue
		}
		err = e.publishMaintenance(ctx, next)
		e.mu.Unlock()
		if err == nil {
			e.metrics.Compactions.Inc()
		}
		return err
	}
	return fmt.Errorf("compaction publication changed concurrently; job rescheduled")
}

func (e *Engine) abortCompaction(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state.CompactionJob.Key == "" {
		return nil
	}
	job, err := e.readJob(ctx)
	if err != nil {
		return err
	}
	if job.Stage == "aborted" {
		return nil
	}
	job.Stage = "aborted"
	job.CleanupAfter = time.Now().Add(time.Minute).UnixNano()
	ref, err := e.writeJob(ctx, job)
	if err != nil {
		return err
	}
	next := cloneManifest(e.committed)
	next.Generation = e.state.Generation + 1
	next.CompactionJob = ref
	if err = e.appendTrash(ctx, &next, []string{e.state.CompactionJob.Key}, 0); err != nil {
		return err
	}
	return e.publishMaintenance(ctx, next)
}

// A restart kills the only worker and releases its in-memory pins. Fence the
// interrupted job before sweeping its registered namespace after a grace period.
func (e *Engine) recoverCompaction(ctx context.Context) error {
	e.maintenanceMu.Lock()
	defer e.maintenanceMu.Unlock()
	if err := e.abortCompaction(ctx); err != nil {
		return err
	}
	e.mu.Lock()
	job, err := e.readJob(ctx)
	e.mu.Unlock()
	if err != nil {
		return err
	}
	if job.ID == "" || time.Now().UnixNano() < job.CleanupAfter {
		return nil
	}
	lister, ok := e.store.(storage.Lister)
	if !ok {
		return fmt.Errorf("compaction recovery requires prefix inventory")
	}
	var keys []string
	for _, prefix := range job.OutputPrefixes {
		token := ""
		for {
			page, next, err := lister.List(ctx, prefix, token, 256)
			if err != nil {
				return err
			}
			keys = append(keys, page...)
			if len(keys) > 16384 {
				return fmt.Errorf("staging cleanup object budget exceeded")
			}
			if next == "" {
				break
			}
			if next == token {
				return fmt.Errorf("nonadvancing inventory cursor")
			}
			token = next
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	active, err := e.readJob(ctx)
	if err != nil {
		return err
	}
	if active.ID != job.ID || active.Stage != "aborted" {
		return fmt.Errorf("recovery job changed")
	}
	next := cloneManifest(e.committed)
	next.Generation = e.state.Generation + 1
	next.CompactionJob = blob{}
	keys = append(keys, e.state.CompactionJob.Key)
	if err = e.appendTrash(ctx, &next, keys, 0); err != nil {
		return err
	}
	return e.publishMaintenance(ctx, next)
}

func (e *Engine) CompactOnce(ctx context.Context) error {
	e.maintenanceMu.Lock()
	defer e.maintenanceMu.Unlock()
	e.mu.Lock()
	pending := e.state.CompactionJob.Key != ""
	e.mu.Unlock()
	if pending {
		return nil
	}
	job, err := e.reserveCompaction(ctx)
	if err != nil {
		return err
	}
	if job.ID == "" {
		return nil
	}
	defer func() { e.mu.Lock(); e.unpin(job.Generation); e.mu.Unlock() }()
	replacements, packs, err := e.copyJob(ctx, job)
	if err == nil {
		err = e.applyCompaction(ctx, job, replacements, packs)
	}
	if err != nil {
		e.metrics.CompactionErrors.Inc()
		if ctx.Err() == nil {
			if abortErr := e.abortCompaction(ctx); abortErr != nil {
				return fmt.Errorf("compaction: %v; abort: %w", err, abortErr)
			}
		}
	}
	return err
}

func (e *Engine) Reclaim(ctx context.Context) error {
	e.maintenanceMu.Lock()
	defer e.maintenanceMu.Unlock()
	deleter, ok := e.store.(storage.Deleter)
	if !ok {
		return nil
	}
	if !e.options.BackgroundMaintenance {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.collectGarbage(ctx)
		return nil
	}
	e.mu.Lock()
	if e.closed || e.fatal != nil {
		e.mu.Unlock()
		return nil
	}
	if cleanup := e.state.TrashCleanup; cleanup != "" {
		e.mu.Unlock()
		if err := deleter.Delete(ctx, cleanup); err != nil {
			e.metrics.GCErrors.Inc()
			return err
		}
		e.mu.Lock()
		next := cloneManifest(e.committed)
		next.Generation++
		next.TrashCleanup = ""
		err := e.publishMaintenance(ctx, next)
		e.mu.Unlock()
		return err
	}
	if e.state.TrashBatch.Key == "" {
		if e.state.TrashHead == e.state.TrashComplete {
			e.mu.Unlock()
			return nil
		}
		next := cloneManifest(e.committed)
		next.Generation = e.state.Generation + 1
		next.TrashBatch = e.state.TrashHead
		next.TrashPending = e.state.TrashHead
		if err := e.publishMaintenance(ctx, next); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	ref := e.state.TrashPending
	offset := e.state.TrashOffset
	b, err := e.readBlob(ctx, ref)
	if err != nil {
		e.mu.Unlock()
		return err
	}
	var page trashPage
	if err = decode(b, &page); err != nil {
		e.mu.Unlock()
		return err
	}
	if offset < 0 || offset >= len(page.Keys) {
		e.mu.Unlock()
		return fmt.Errorf("invalid trash cursor")
	}
	if time.Now().UnixNano() < page.NotBefore {
		e.mu.Unlock()
		return nil
	}
	for generation, count := range e.pins {
		if count > 0 && generation < page.Generation {
			e.mu.Unlock()
			return nil
		}
	}
	e.mu.Unlock()
	// The retirement journal is durable and addresses cannot be resurrected.
	deleteCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	done := 0
	var deleteErr error
	for _, key := range page.Keys[offset:min(offset+16, len(page.Keys))] {
		if err := deleter.Delete(deleteCtx, key); err != nil {
			e.metrics.GCErrors.Inc()
			deleteErr = err
			break
		}
		done++
		e.metrics.GCDeleted.Inc()
	}
	cancel()
	if done == 0 {
		return deleteErr
	}
	e.mu.Lock()
	next := cloneManifest(e.committed)
	next.Generation = e.state.Generation + 1
	next.TrashOffset = offset + done
	next.TrashObjects -= int64(done)
	finished := next.TrashOffset == len(page.Keys)
	if finished {
		next.TrashCleanup = ref.Key
		next.TrashPending = page.Prev
		next.TrashOffset = 0
	}
	if next.TrashPending == next.TrashComplete {
		next.TrashComplete = next.TrashBatch
		next.TrashBatch = blob{}
		next.TrashPending = blob{}
	}
	err = e.publishMaintenance(ctx, next)
	e.mu.Unlock()
	if err != nil {
		return err
	}

	return deleteErr
}
