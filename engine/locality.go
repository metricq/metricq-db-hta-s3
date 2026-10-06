package engine

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"
)

type localityScan struct {
	Root        blob
	After       int64
	RetryAt     int64
	ObjectLimit int
}

type localityStream struct {
	Metric string
	Level  int64
	Key    string
}

// localitySection is a run of consecutive index entries of one stream stored
// contiguously in one object: a query reads it with one range request.
type localitySection struct {
	begin, end int // entries[begin:end]
	bytes      int64
}

// localitySections splits consecutive entries into physically contiguous runs.
func localitySections(entries []indexEntry) []localitySection {
	var sections []localitySection
	for i, entry := range entries {
		if i > 0 {
			prev := entries[i-1].Blob
			if entry.Blob.Key == prev.Key && entry.Blob.Offset >= prev.Offset && entry.Blob.Offset-prev.Offset-prev.Length <= maxCoalescedGap {
				last := &sections[len(sections)-1]
				last.end = i + 1
				last.bytes += entry.Blob.Length
				continue
			}
		}
		sections = append(sections, localitySection{begin: i, end: i + 1, bytes: entry.Blob.Length})
	}
	return sections
}

// sectionTier groups section sizes by powers of fanIn below target: tier 1
// holds sections of at least target/fanIn, tier 2 at least target/fanIn², and
// so on. Full sections (at least target) are tier 0 and never rewritten.
func sectionTier(bytes, target int64, fanIn int) int {
	tier := 0
	for limit := target; bytes < limit && limit > 0; limit /= int64(fanIn) {
		tier++
	}
	return tier
}

// localityMerge picks the oldest run of fanIn consecutive non-full sections
// of one size tier, extended by further sections of that tier while the
// result stays within target. Sections thus grow geometrically (fanIn small
// sections become one of the next tier): a stream keeps its settled sections
// plus at most fanIn-1 per smaller tier, and every byte is rewritten about
// log_fanIn(target/section) times. In the top tier (at least target/fanIn)
// fanIn sections never fit into target, so two suffice there. Without such a
// run, the oldest fanIn sections fitting into target merge, so mixed sizes
// left by bounded jobs cannot accumulate.
func localityMerge(sections []localitySection, target int64, fanIn int) (int, int) {
	tiers := make([]int, len(sections))
	for i, s := range sections {
		tiers[i] = sectionTier(s.bytes, target, fanIn)
	}
	for i := 0; i < len(sections); i++ {
		if tiers[i] == 0 {
			continue
		}
		j, total := i, int64(0)
		for j < len(sections) && tiers[j] == tiers[i] && total+sections[j].bytes <= target {
			total += sections[j].bytes
			j++
		}
		need := fanIn
		if tiers[i] == 1 {
			need = 2
		}
		if j-i >= need {
			return i, j
		}
	}
	// Fallback for mixed sizes: only unsettled sections (below half the
	// target) count, so long histories of settled sections never trigger it.
	unsettled := 0
	for _, sec := range sections {
		if sec.bytes < target/2 {
			unsettled++
		}
	}
	if unsettled > 4*fanIn {
		for i := 0; i+fanIn <= len(sections); i++ {
			j, total := i, int64(0)
			for j < len(sections) && sections[j].bytes < target/2 && total+sections[j].bytes <= target {
				total += sections[j].bytes
				j++
			}
			if j-i >= fanIn {
				return i, j
			}
		}
	}
	return 0, 0
}

// selectLocality packs consecutive blocks of a metric level into contiguous
// sections that grow up to compaction_output_object_bytes, so a timeline query
// needs few range requests. Bounds are physical bytes/blocks, never time
// windows; the open suffix (partial tail blocks) is left to merging.
func (e *Engine) selectLocality(ctx context.Context, snapshot *Engine, options CompactionOptions, objectLimit int) ([]BlockInfo, bool, error) {
	var streams []localityStream
	for name, levels := range snapshot.state.Roots {
		for level, root := range levels {
			if root.Key != "" {
				streams = append(streams, localityStream{name, level, streamKey(name, level)})
			}
		}
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].Key < streams[j].Key })
	if e.localityScans == nil {
		e.localityScans = make(map[string]localityScan)
	}
	defer func() {
		pending := 0
		for _, s := range streams {
			if state := e.localityScans[s.Key]; state.Root != snapshot.state.Roots[s.Metric][s.Level] || state.RetryAt != 0 {
				pending++
			}
		}
		e.metrics.LocalityPendingStreams.Set(float64(pending))
	}()
	start := sort.Search(len(streams), func(i int) bool { return streams[i].Key > e.localityCursor })
	if start == len(streams) {
		start = 0
		e.localityCursor = ""
	}
	target := options.OutputObjectBytes
	fanIn := options.LocalityFanIn
	cutoff := time.Now().Add(-time.Duration(options.MergeCooldownSeconds) * time.Second).UnixNano()
	// Unfinished sections of a stream span at most fanIn-1 sections per tier;
	// reading this many entries beyond the full prefix covers them.
	limit := max(options.JobMaxBlocks*4, 1024)
	checked := 0
	var selected []BlockInfo
	var selectedBytes int64
	selectedObjects := map[string]bool{}
	for i := start; i < len(streams); i++ {
		if len(selected) >= options.JobMaxBlocks-1 || selectedBytes >= options.JobMaxBytes {
			return selected, true, nil
		}
		s := streams[i]
		root := snapshot.state.Roots[s.Metric][s.Level]
		state := e.localityScans[s.Key]
		e.localityCursor = s.Key
		if state.Root == root && state.ObjectLimit == objectLimit && (state.RetryAt == 0 || time.Now().UnixNano() < state.RetryAt) {
			continue
		}
		if checked >= 64 || snapshot.nodeReads >= 2048 {
			e.localityCursor = previousStream(streams, max(start, i))
			return selected, true, nil
		}
		checked++
		after := state.After
		if after == 0 {
			after = math.MinInt64
		}
		entries, err := snapshot.indexEntriesAfter(ctx, root, after, limit)
		if err != nil {
			return nil, false, err
		}
		if len(entries) < limit {
			// The open suffix still grows; merging completes it.
			blocks, _ := openSuffix(entries)
			entries = entries[:len(entries)-blocks]
		}
		sections := localitySections(entries)
		// Sections of at least half the target can no longer pair up within
		// it: a settled prefix is skipped from now on, so scans stay short.
		next := after
		for _, sec := range sections {
			if sec.bytes < target/2 || entries[sec.end-1].Last == math.MaxInt64 {
				break
			}
			next = entries[sec.end-1].Last + 1
		}
		first, end := localityMerge(sections, target, fanIn)
		if first == end {
			e.localityScans[s.Key] = localityScan{Root: root, After: next, ObjectLimit: objectLimit}
			continue
		}
		group := entries[sections[first].begin:sections[end-1].end]
		var groupBytes int64
		objects := map[string]bool{}
		for _, entry := range group {
			groupBytes += entry.Blob.Length
			objects[entry.Blob.Key] = true
		}
		if len(selected)+len(group) > options.JobMaxBlocks || selectedBytes+groupBytes > options.JobMaxBytes || len(selectedObjects)+countNewObjects(objects, selectedObjects) > objectLimit {
			if len(selected) > 0 {
				// The next job takes this stream first.
				e.localityCursor = previousStream(streams, i)
				return selected, true, nil
			}
			// Too large even alone (small limits): retry when limits change.
			e.localityScans[s.Key] = localityScan{Root: root, After: next, ObjectLimit: objectLimit}
			continue
		}
		cooling := int64(0)
		for key := range objects {
			o, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, key)
			if errors.Is(err, errCatalogBudget) {
				e.localityCursor = previousStream(streams, i)
				return selected, true, nil
			}
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, errors.New("locality source absent from catalog")
			}
			if o.Modified > cutoff {
				cooling = max(cooling, o.Modified)
			}
		}
		if cooling != 0 {
			e.localityScans[s.Key] = localityScan{Root: root, After: next, RetryAt: cooling + int64(time.Duration(options.MergeCooldownSeconds)*time.Second), ObjectLimit: objectLimit}
			continue
		}
		for _, entry := range group {
			selected = append(selected, BlockInfo{Metric: s.Metric, Level: s.Level, Entry: entry})
		}
		selectedBytes += groupBytes
		for key := range objects {
			selectedObjects[key] = true
		}
		// Until publication changes the root this is the retry point.
		e.localityScans[s.Key] = localityScan{After: next}
	}
	e.localityCursor = ""
	return selected, len(selected) > 0, nil
}

func countNewObjects(group, selected map[string]bool) int {
	n := 0
	for key := range group {
		if !selected[key] {
			n++
		}
	}
	return n
}
