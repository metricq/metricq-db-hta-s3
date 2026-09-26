package engine

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	snapshot := &Engine{store: e.preparationStore(), options: e.options, metrics: e.metrics, state: cloneManifest(e.committed), sharedNodes: e.sharedNodes, nodeCache: make(map[blob]indexNode), nodeReadLimit: 4096, catalogReadBudget: 32 << 20}
	scanAfter := e.candidateCursor
	seedObject, seedOffset := e.compactionSeedObject, e.compactionSeedOffset
	evacuateObject := e.compactionEvacuateObject
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
	selected := make(map[blob]bool)
	tried := make(map[string]bool)
	var copied int64
	previousCandidate := scanAfter
	resumeCandidate, resumeObject := "", ""
	resumeOffset := 0
	cutoff := time.Now().Add(-time.Duration(options.CooldownSeconds) * time.Second).UnixNano()
	var selectErr error
	seedLimited := false
	add := func(b BlockInfo) bool {
		if selected[b.Entry.Blob] {
			return true
		}
		if len(inputs) >= options.MaxBlocks || b.Entry.Blob.Length > options.MaxJobBytes-copied {
			return false
		}
		inputs = append(inputs, b)
		selected[b.Entry.Blob] = true
		copied += b.Entry.Blob.Length
		return true
	}
	nextEvacuation := ""
	if evacuateObject != "" {
		o, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, evacuateObject)
		if err != nil {
			return CompactionJob{}, err
		}
		if ok && o.Modified <= cutoff && float64(o.Size-o.LiveBytes)/float64(o.Size) >= options.DeadFraction {
			for _, b := range o.Blocks {
				if !add(b) {
					break
				}
			}
			if len(inputs) < len(o.Blocks) && len(inputs) > 0 {
				nextEvacuation = o.Key
			}
		}
	}
	cursor := scanAfter
	var err error
	if len(inputs) == 0 {
		cursor, err = snapshot.catalogScan(ctx, snapshot.state.Candidates, scanAfter, 256, func(candidate ObjectInfo) bool {
			object, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, candidate.Target)
			if err != nil {
				selectErr = err
				return false
			}
			if !ok || object.Modified > cutoff || object.Size == 0 {
				previousCandidate = candidate.Key
				return true
			}
			dirty := float64(object.Size-object.LiveBytes)/float64(object.Size) >= options.DeadFraction
			if dirty {
				// Partial evacuation is required for mixed packs larger than a job.
				// Retain the source key across passes until its last live block moves.
				remaining := 0
				for _, b := range object.Blocks {
					if !add(b) {
						remaining++
					}
				}
				if remaining > 0 && len(inputs) > 0 {
					nextEvacuation = object.Key
					return false
				}
			}

			if options.MergeSmallBlocks {
				beginSeed := 0
				if candidate.Key == seedObject {
					beginSeed = min(seedOffset, len(object.Blocks))
				}
				for seedIndex := beginSeed; seedIndex < len(object.Blocks); seedIndex++ {
					seed := object.Blocks[seedIndex]
					if seed.Index || seed.Entry.Records <= 0 || seed.Entry.Records > maxDataBlockRecords/2 {
						continue
					}
					stream := fmt.Sprintf("%s/%020d", seed.Metric, seed.Level)
					if tried[stream] {
						continue
					}
					root := snapshot.state.Roots[seed.Metric][seed.Level]
					// An immutable single-block root cannot supply a merge partner.
					// Known singleton tails must not consume the 64-stream search
					// budget on every pass. A flush changes the root/hash, so this
					// shortcut cannot hide subsequently appended fragments.
					if snapshot.sharedNodes != nil {
						if node, ok := snapshot.sharedNodes.get(root); ok && node.Leaf && len(node.Entries) == 1 {
							continue
						}
					}
					tried[stream] = true
					begin := seed.Entry.First
					prior, err := snapshot.indexNeighborEntry(ctx, root, begin, true)
					if err != nil {
						selectErr = err
						return false
					}
					if prior.Records > 0 && prior.Records+seed.Entry.Records <= maxDataBlockRecords {
						begin = prior.First
					}
					entries, err := snapshot.indexEntriesAfter(ctx, root, begin, options.MaxBlocks-len(inputs))
					if err != nil {
						selectErr = err
						return false
					}
					var group []BlockInfo
					records := 0
					bytes := int64(0)
					for _, entry := range entries {
						if entry.Records <= 0 || entry.Records >= maxDataBlockRecords || records+entry.Records > maxDataBlockRecords {
							break
						}
						o, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, entry.Blob.Key)
						if err != nil {
							selectErr = err
							return false
						}
						if !ok || o.Modified > cutoff {
							break
						}
						if entry.Blob.Length > options.MaxJobBytes-copied-bytes {
							break
						}
						group = append(group, BlockInfo{Metric: seed.Metric, Level: seed.Level, Entry: entry})
						records += entry.Records
						if !selected[entry.Blob] {
							bytes += entry.Blob.Length
						}
					}
					if len(group) > 1 {
						for _, b := range group {
							add(b)
						}
					}
					if len(inputs) >= options.MaxBlocks || len(tried) >= 64 {
						seedLimited = len(inputs) < options.MaxBlocks && len(tried) >= 64
						resumeCandidate = previousCandidate
						resumeObject = candidate.Key
						resumeOffset = seedIndex + 1
						return false
					}
				}
			}
			previousCandidate = candidate.Key
			return len(inputs) < options.MaxBlocks && copied < options.MaxJobBytes
		})
	}
	e.mu.Lock()
	e.compactionEvacuateObject = nextEvacuation
	e.candidateCursor = cursor
	e.compactionSeedObject = resumeObject
	e.compactionSeedOffset = resumeOffset
	if resumeObject != "" {
		e.candidateCursor = resumeCandidate
	}
	e.compactionScanMore = len(inputs) == 0 && (seedLimited || cursor != "")
	if e.closed {
		e.mu.Unlock()
		return CompactionJob{}, fmt.Errorf("engine closed")
	}
	if e.fatal != nil {
		err := e.fatal
		e.mu.Unlock()
		return CompactionJob{}, err
	}
	if e.state.CompactionJob.Key != "" {
		e.mu.Unlock()
		return CompactionJob{}, fmt.Errorf("job already active")
	}
	e.mu.Unlock()
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
		e.metrics.CompactionNoop.Inc()
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
	jobWriter := &Engine{store: e.preparationStore(), metrics: e.metrics}
	ref, err := jobWriter.writeJob(ctx, job)
	if err != nil {
		return CompactionJob{}, err
	}
	e.publishMu.Lock()
	defer e.publishMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	next := cloneManifest(e.committed)
	next.Generation = e.state.Generation + 1
	next.CompactionJob = ref
	if err = e.publishMaintenanceLocked(ctx, next); err != nil {
		return CompactionJob{}, err
	}
	keepPin = true
	return job, nil
}

type rateBudget struct {
	mu          sync.Mutex
	start       time.Time
	bytes, rate int64
}

func (b *rateBudget) take(ctx context.Context, n int64) error {
	b.mu.Lock()
	b.bytes += n
	if b.rate <= 0 {
		b.mu.Unlock()
		return fmt.Errorf("nonpositive compaction rate")
	}
	due := b.start.Add(time.Duration(float64(b.bytes) / float64(b.rate) * float64(time.Second)))
	b.mu.Unlock()
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

// All preparation reads/writes share the same sustained byte budget across
// consecutive jobs, including catalog metadata and newly rebuilt index pages.
type maintenanceStore struct {
	storage.Store
	budget *rateBudget
}

func (s maintenanceStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, v, err := s.Store.Get(ctx, key)
	if err == nil {
		err = s.budget.take(ctx, int64(len(b)))
	}
	return b, v, err
}
func (s maintenanceStore) Put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	if err := s.budget.take(ctx, int64(len(b))); err != nil {
		return "", err
	}
	return s.Store.Put(ctx, key, b, v)
}
func (s maintenanceStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := s.budget.take(ctx, length); err != nil {
		return nil, err
	}
	if ranged, ok := s.Store.(storage.RangeGetter); ok {
		return ranged.GetRange(ctx, key, offset, length)
	}
	b, _, err := s.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 || offset > int64(len(b)) || length > int64(len(b))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return b[offset : offset+length], nil
}
func (e *Engine) preparationStore() storage.Store {
	if e.compactionBudget == nil {
		return e.store
	}
	return maintenanceStore{Store: e.store, budget: e.compactionBudget}
}

// Publication metadata is bounded by the job/page budgets. Account this short
// burst after releasing publishMu so rate limiting cannot hold up a Flush.
type publicationStore struct {
	storage.Store
	bytes atomic.Int64
}

func (s *publicationStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, version, err := s.Store.Get(ctx, key)
	s.bytes.Add(int64(len(b)))
	return b, version, err
}
func (s *publicationStore) Put(ctx context.Context, key string, b []byte, version *string) (string, error) {
	s.bytes.Add(int64(len(b)))
	return s.Store.Put(ctx, key, b, version)
}
func (s *publicationStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if ranged, ok := s.Store.(storage.RangeGetter); ok {
		b, err := ranged.GetRange(ctx, key, offset, length)
		s.bytes.Add(int64(len(b)))
		return b, err
	}
	b, _, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 || offset > int64(len(b)) || length > int64(len(b))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	return b[offset : offset+length], nil
}

func (e *Engine) copyJob(ctx context.Context, job CompactionJob) (map[blob]replacement, []*pack, error) {
	options := e.options.Compaction.defaults()
	reader := &Engine{store: e.preparationStore(), metrics: e.metrics, options: e.options}
	indexReader := &Engine{store: e.preparationStore(), metrics: e.metrics, options: e.options, sharedNodes: e.sharedNodes, nodeCache: make(map[blob]indexNode), nodeReadLimit: 4096}
	budget := rateBudget{start: time.Now(), rate: options.BytesPerSecond}
	take := func(n int64) error {
		if e.compactionBudget != nil {
			return ctx.Err()
		}
		return budget.take(ctx, n)
	}
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
	// Fetch at most eight compressed source blocks ahead. Consumption remains
	// ordered, so merging and output hashes do not depend on S3 response order.
	cached := make(map[int][]byte)
	load := func(index int) ([]byte, error) {
		if b, ok := cached[index]; ok {
			delete(cached, index)
			return b, nil
		}
		end := min(index+8, len(inputs))
		blocks := make([][]byte, end-index)
		errs := make([]error, end-index)
		var wg sync.WaitGroup
		for j := index; j < end; j++ {
			if _, ok := cached[j]; ok {
				continue
			}
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				k := j - index
				if errs[k] = take(inputs[j].Entry.Blob.Length); errs[k] != nil {
					return
				}
				blocks[k], errs[k] = reader.readBlob(ctx, inputs[j].Entry.Blob)
				if errs[k] == nil {
					e.metrics.CompactionReadBytes.Add(float64(len(blocks[k])))
				}
			}(j)
		}
		wg.Wait()
		for k, err := range errs {
			if err != nil {
				return nil, err
			}
			if blocks[k] != nil {
				cached[index+k] = blocks[k]
			}
		}
		b := cached[index]
		delete(cached, index)
		return b, nil
	}
	var packs []*pack
	var current *pack
	counts := map[string]int{"data": 0, "index": 0}
	upload := func() error {
		if current == nil {
			return nil
		}
		if err := take(int64(current.buf.Len())); err != nil {
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
		encoded, err := load(i)
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
				for j := i + 1; j < end; j++ {
					next, err := indexReader.indexNeighbor(ctx, root, inputs[j-1].Entry.Last, false)
					if err != nil {
						return nil, nil, err
					}
					if next != inputs[j].Entry.Blob {
						end = j
						break
					}
				}
				b = inputs[i]
				for j := i + 1; j < end; j++ {
					b.Entry.Records += inputs[j].Entry.Records
				}

			}
		}
		if end > i+1 {
			var records []hta.Record
			if err = decode(encoded, &records); err != nil {
				return nil, nil, err
			}
			for j := i + 1; j < end; j++ {
				bytes, err := load(j)
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
		if !n.Leaf && len(entries) == 1 {
			return entries, nil
		}
		if !n.Leaf {
			// Merge a bounded set of sibling leaves. Reading at most fanout
			// pages here avoids a historical scan during index contraction.
			var combined []indexEntry
			var children []blob
			for _, edge := range entries {
				var child indexNode
				var err error
				if edge.Blob.Key == p.key {
					child, err = decodeNode(p.buf.Bytes()[edge.Blob.Offset : edge.Blob.Offset+edge.Blob.Length])
				} else {
					child, err = e.readNode(ctx, edge.Blob)
				}
				if err != nil {
					return nil, err
				}
				if !child.Leaf || len(combined)+len(child.Entries) > indexFanout {
					combined = nil
					break
				}
				combined = append(combined, child.Entries...)
				children = append(children, edge.Blob)
			}
			if len(combined) > 0 {
				for _, ref := range children {
					if ref.Key != p.key && !strings.HasPrefix(ref.Key, "index/compact-"+e.activeMaintenance+"/") {
						p.retired = append(p.retired, ref)
					}
				}
				return writeNodes(p, true, combined)
			}
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

	sourceModified := int64(0)
	// Exact current catalog references validate data AND index source identity.
	for _, input := range job.Inputs {
		object, ok, err := e.catalogGet(ctx, e.state.Catalog, input.Entry.Blob.Key)
		if err != nil {
			return manifest{}, err
		}
		if object.Modified > sourceModified {
			sourceModified = object.Modified
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
	// Reachability excludes intermediate pages made obsolete by contraction.
	outputPacks := make(map[string]*pack)
	for _, output := range packs {
		outputPacks[output.key] = output
	}
	used = make(map[blob]bool)
	var mark func(blob) error
	mark = func(ref blob) error {
		output := outputPacks[ref.Key]
		if output == nil || used[ref] {
			return nil
		}
		used[ref] = true
		if !strings.HasPrefix(ref.Key, "index/") {
			return nil
		}
		n, err := decodeNode(output.buf.Bytes()[ref.Offset : ref.Offset+ref.Length])
		if err != nil {
			return err
		}
		for _, edge := range n.Entries {
			if err = mark(edge.Blob); err != nil {
				return err
			}
		}
		return nil
	}
	for metric, levels := range affected {
		for level := range levels {
			if err = mark(next.Roots[metric][level]); err != nil {
				return manifest{}, err
			}
		}
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
		o := &ObjectInfo{Key: output.key, Size: int64(output.buf.Len()), Created: now, Modified: sourceModified}
		for _, b := range output.descriptors {
			if used[b.Entry.Blob] {
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
		// Retirement does not make surviving data newly ingested.
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
	// Data copying is finished. Serialize only the bounded metadata rewrite
	// against the newest roots, without holding the ingestion mutex during I/O.
	e.publishMu.Lock()
	publication := &publicationStore{Store: e.store}
	defer func() {
		e.publishMu.Unlock()
		if e.compactionBudget != nil {
			// A successful publication stays successful even if cancellation
			// interrupts the subsequent pacing delay. The byte debt persists.
			_ = e.compactionBudget.take(ctx, publication.bytes.Load())
		}
	}()
	{
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
		snapshot := &Engine{store: publication, options: e.options, metrics: e.metrics, state: cloneManifest(e.committed), committed: cloneManifest(e.committed), sharedNodes: e.sharedNodes, activeMaintenance: job.ID, nodeReadLimit: 4096, catalogReadBudget: 32 << 20}
		e.mu.Unlock()
		next, err := snapshot.prepareCompaction(ctx, job, replacements, packs, nil)
		if err != nil {
			return err
		}
		e.mu.Lock()
		err = e.publishMaintenanceLocked(ctx, next)
		e.mu.Unlock()
		if err == nil {
			e.mu.Lock()
			e.compactionCompletions++
			e.mu.Unlock()
			e.metrics.Compactions.Inc()
			e.metrics.CompactionInputBlocks.Add(float64(len(job.Inputs)))
			outputBlocks := 0
			for _, r := range replacements {
				if !r.Drop {
					outputBlocks++
				}
			}
			e.metrics.CompactionOutputBlocks.Add(float64(outputBlocks))
		}
		return err
	}
}

// Build maintenance metadata without the ingestion mutex. Generation pins keep
// captured metadata alive until publication or discard.
var errMaintenanceNoop = fmt.Errorf("no maintenance change")

func (e *Engine) editMaintenance(ctx context.Context, edit func(*Engine) (manifest, error)) error {
	e.publishMu.Lock()
	defer e.publishMu.Unlock()
	e.mu.Lock()
	if e.closed || e.fatal != nil {
		e.mu.Unlock()
		return fmt.Errorf("engine unavailable")
	}
	generation := e.committed.Generation
	snapshot := &Engine{store: e.preparationStore(), options: e.options, metrics: e.metrics, state: cloneManifest(e.committed), committed: cloneManifest(e.committed)}
	e.pin(generation)
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.unpin(generation); e.mu.Unlock() }()
	next, err := edit(snapshot)
	if err == errMaintenanceNoop {
		return nil
	}
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.publishMaintenanceLocked(ctx, next)
}
func (e *Engine) abortCompaction(ctx context.Context) error {
	e.mu.Lock()
	pending := e.state.CompactionJob.Key != ""
	e.mu.Unlock()
	if !pending {
		return nil
	}
	return e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
		job, err := snapshot.readJob(ctx)
		if err != nil {
			return manifest{}, err
		}
		next := cloneManifest(snapshot.committed)
		if job.Stage == "aborted" {
			return manifest{}, errMaintenanceNoop
		}
		job.Stage = "aborted"
		job.CleanupAfter = time.Now().Add(time.Minute).UnixNano()
		ref, err := snapshot.writeJob(ctx, job)
		if err != nil {
			return manifest{}, err
		}
		next.Generation++
		next.CompactionJob = ref
		if err = snapshot.appendTrash(ctx, &next, []string{snapshot.state.CompactionJob.Key}, 0); err != nil {
			return manifest{}, err
		}
		return next, nil
	})
}

// A restart kills the only worker and releases its in-memory pins. Fence the
// interrupted job before sweeping its registered namespace after a grace period.
func (e *Engine) recoverCompaction(ctx context.Context) error {
	e.maintenanceMu.Lock()
	defer e.maintenanceMu.Unlock()
	if err := e.abortCompaction(ctx); err != nil {
		return err
	}
	var job CompactionJob
	if err := e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
		var err error
		job, err = snapshot.readJob(ctx)
		if err != nil {
			return manifest{}, err
		}
		return manifest{}, errMaintenanceNoop
	}); err != nil {
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
	return e.editMaintenance(ctx, func(snapshot *Engine) (manifest, error) {
		active, err := snapshot.readJob(ctx)
		if err != nil {
			return manifest{}, err
		}
		if active.ID != job.ID || active.Stage != "aborted" {
			return manifest{}, fmt.Errorf("recovery job changed")
		}
		next := cloneManifest(snapshot.committed)
		next.Generation++
		next.CompactionJob = blob{}
		keys = append(keys, snapshot.state.CompactionJob.Key)
		if err = snapshot.appendTrash(ctx, &next, keys, 0); err != nil {
			return manifest{}, err
		}
		return next, nil
	})
}

func (e *Engine) CompactOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(e.options.Compaction.defaults().MaxDurationSeconds)*time.Second)
	defer cancel()
	e.maintenanceMu.Lock()
	defer e.maintenanceMu.Unlock()
	if e.compactionBudget == nil || time.Since(e.lastCompactionEnd) > time.Second {
		e.compactionBudget = &rateBudget{start: time.Now(), rate: e.options.Compaction.BytesPerSecond}
	}
	defer func() { e.lastCompactionEnd = time.Now() }()
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
	start := time.Now()
	e.metrics.CompactionActive.Set(1)
	defer func() {
		e.metrics.CompactionActive.Set(0)
		e.metrics.CompactionDuration.Observe(time.Since(start).Seconds())
	}()
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
	batch := e.state.TrashBatch
	ref := e.state.TrashPending
	offset := e.state.TrashOffset
	if batch.Key == "" {
		if e.state.TrashHead == e.state.TrashComplete {
			e.mu.Unlock()
			return nil
		}
		batch = e.state.TrashHead
		ref = batch
		offset = 0
	}
	e.mu.Unlock()
	b, err := e.readBlob(ctx, ref)
	if err != nil {
		return err
	}
	e.mu.Lock()
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
	next.TrashBatch = batch
	next.TrashPending = ref
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
