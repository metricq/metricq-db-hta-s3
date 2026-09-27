package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type localityScan struct {
	Root        blob
	After       int64
	RetryAt     int64
	ObjectLimit int
}

func afterLocalityEntry(entry indexEntry) int64 {
	if entry.Last == math.MaxInt64 {
		return entry.Last
	}
	return entry.Last + 1
}

type localityStream struct {
	Metric string
	Level  int64
	Key    string
}

// Select consecutive index entries of one canonical metric/level. Bounds are
// physical bytes/blocks, never time windows. Sealed locality packs are excluded
// from future jobs; appending data cannot rewrite their historical prefix.
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
	cutoff := time.Now().Add(-time.Duration(options.MergeCooldownSeconds) * time.Second).UnixNano()
	checked := 0
	var selected []BlockInfo
	var selectedBytes int64
	selectedObjects := map[string]bool{}
	accept := func(stream localityStream, work []BlockInfo) {
		selected = append(selected, work...)
		for _, b := range work {
			selectedBytes += b.Entry.Blob.Length
			selectedObjects[b.Entry.Blob.Key] = true
		}
		// Until publication this is the retry point, not the end of the section.
		e.localityScans[stream.Key] = localityScan{After: work[0].Entry.First}
	}
scanStreams:
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
			e.localityCursor = streams[max(start, i-1)].Key
			return selected, true, nil
		}
		checked++
		cooldown := false
		entries, err := snapshot.indexEntriesAfter(ctx, root, state.After, options.JobMaxBlocks-len(selected))
		if err != nil {
			return nil, false, err
		}
		var group []BlockInfo
		var bytes int64
		spans := 0
		objects := make(map[string]bool)
		flush := func() ([]BlockInfo, bool) {
			// An isolated full block offers no range reduction. Four physical ranges
			// amortize publication/rewrite cost, without waiting for a temporal boundary.
			if spans >= options.LocalityMinRanges {
				return group, true
			}
			return nil, false
		}
		advance := state.After
		for _, entry := range entries {
			if strings.Contains(entry.Blob.Key, "/locality/") {
				if work, ok := flush(); ok {
					accept(s, work)
					continue scanStreams
				}
				group = nil
				bytes = 0
				spans = 0
				objects = make(map[string]bool)
				advance = afterLocalityEntry(entry)
				continue
			}
			if len(group) == 0 && !selectedObjects[entry.Blob.Key] && len(selectedObjects) >= objectLimit {
				return selected, true, nil
			}
			newObject := !objects[entry.Blob.Key]
			if len(group) > 0 && (bytes+entry.Blob.Length > options.OutputObjectBytes || bytes+entry.Blob.Length > options.JobMaxBytes-selectedBytes || (newObject && !selectedObjects[entry.Blob.Key] && len(selectedObjects)+countNewObjects(objects, selectedObjects) >= objectLimit)) {
				if work, ok := flush(); ok {
					accept(s, work)
					continue scanStreams
				}
				// The preceding extent already has good physical locality or cannot
				// improve within this job's object target. Continue with the next extent.
				advance = entry.First
				group = nil
				bytes = 0
				spans = 0
				objects = make(map[string]bool)
			}
			if entry.Blob.Length > options.OutputObjectBytes || entry.Blob.Length > options.JobMaxBytes-selectedBytes {
				advance = afterLocalityEntry(entry)
				continue
			}
			if newObject {
				o, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, entry.Blob.Key)
				if errors.Is(err, errCatalogBudget) {
					return selected, true, nil
				}
				if err != nil {
					return nil, false, err
				}
				if !ok {
					return nil, false, fmt.Errorf("locality source absent from catalog")
				}
				if o.Modified > cutoff {
					// Recheck this root after cooldown, including its small unfinished suffix.
					after := entry.First
					if len(group) > 0 {
						after = group[0].Entry.First
					}
					e.localityScans[s.Key] = localityScan{Root: root, After: after, RetryAt: o.Modified + int64(time.Duration(options.MergeCooldownSeconds)*time.Second), ObjectLimit: objectLimit}
					cooldown = true
					group = nil
					break
				}
			}
			if len(group) == 0 {
				spans++
			} else {
				prev := group[len(group)-1].Entry.Blob
				if prev.Key != entry.Blob.Key || entry.Blob.Offset < prev.Offset || entry.Blob.Offset-prev.Offset-prev.Length > maxCoalescedGap {
					spans++
				}
			}
			objects[entry.Blob.Key] = true
			group = append(group, BlockInfo{Metric: s.Metric, Level: s.Level, Entry: entry})
			bytes += entry.Blob.Length
		}
		if work, ok := flush(); ok {
			accept(s, work)
			continue scanStreams
		}
		if len(group) > 0 {
			advance = group[0].Entry.First
		}
		// A complete scan can sleep until this stream root changes. If the batch
		// was full, resume beyond it instead, retaining only its unfinished suffix.
		if len(entries) < options.JobMaxBlocks-len(selected) {
			if !cooldown {
				e.localityScans[s.Key] = localityScan{Root: root, After: advance, ObjectLimit: objectLimit}
			}
		} else {
			if len(group) > 0 && len(group) == len(entries) {
				advance = afterLocalityEntry(entries[len(entries)-1])
			}
			e.localityScans[s.Key] = localityScan{After: advance}
			e.localityCursor = "" // revisit this stream on a later bounded selection
			return selected, true, nil
		}
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
