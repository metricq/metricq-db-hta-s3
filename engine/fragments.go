package engine

import (
	"context"
	"errors"
	"math"
	"sort"
)

// fragmentScan remembers how far a stream is known to contain no partial
// block before its tail. Rechunking and merging never create such fragments
// in a clean prefix, so after a root change only newer entries are checked.
type fragmentScan struct {
	Root  blob  // root at which the stream was fully checked
	After int64 // first entry time not yet known to be clean
}

// selectFragment finds a partial block inside a stream (followed by another
// block) of any size, independent of candidate objects, and returns a run to
// rechunk from it. Merge candidates only cover seeds up to 512 records, so this
// also repairs larger fragments, e.g. from merges before tails were tracked.
// It checks a bounded number of changed streams per call.
func (e *Engine) selectFragment(ctx context.Context, snapshot *Engine, options CompactionOptions, objectLimit int, cutoff int64) ([]BlockInfo, bool, error) {
	var streams []localityStream
	for name, levels := range snapshot.state.Roots {
		for level, root := range levels {
			if root.Key != "" {
				streams = append(streams, localityStream{name, level, streamKey(name, level)})
			}
		}
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].Key < streams[j].Key })
	if e.fragmentScans == nil {
		e.fragmentScans = make(map[string]fragmentScan)
	}
	start := sort.Search(len(streams), func(i int) bool { return streams[i].Key > e.fragmentCursor })
	if start == len(streams) {
		start = 0
	}
	limit := options.JobMaxBlocks
	checked := 0
	for i := start; i < len(streams); i++ {
		s := streams[i]
		root := snapshot.state.Roots[s.Metric][s.Level]
		state, seen := e.fragmentScans[s.Key]
		if state.Root == root {
			continue
		}
		if checked >= 64 || snapshot.nodeReads >= 2048 {
			return nil, true, nil
		}
		checked++
		e.fragmentCursor = s.Key
		after := int64(math.MinInt64)
		if seen {
			after = state.After
		}
		entries, err := snapshot.indexEntriesAfter(ctx, root, after, limit)
		if err != nil {
			return nil, false, err
		}
		complete := len(entries) < limit
		fragment := -1
		for j := 0; j+1 < len(entries); j++ {
			if entries[j].Records > 0 && entries[j].Records < maxDataBlockRecords {
				fragment = j
				break
			}
		}
		if fragment < 0 {
			if len(entries) == 0 {
				e.fragmentScans[s.Key] = fragmentScan{Root: root, After: after}
				continue
			}
			// The last entry may still be (or become) an interior partial block.
			next := fragmentScan{After: entries[len(entries)-1].First}
			if complete {
				next.Root = root
			} else {
				// More entries follow: continue with this stream next call.
				e.fragmentCursor = previousStream(streams, i)
			}
			e.fragmentScans[s.Key] = next
			if !complete {
				return nil, true, nil
			}
			continue
		}
		run, budgetHit, err := snapshot.rechunkRun(ctx, s.Metric, s.Level, entries[fragment:], complete, options, objectLimit, cutoff)
		if err != nil {
			return nil, false, err
		}
		if budgetHit {
			e.fragmentCursor = previousStream(streams, i)
			return nil, true, nil
		}
		// Keep the prefix before the fragment; recheck the rest after publication
		// (the root changes) or after cooldown.
		e.fragmentScans[s.Key] = fragmentScan{After: entries[fragment].First}
		if len(run) > 1 {
			return run, true, nil
		}
	}
	e.fragmentCursor = ""
	return nil, false, nil
}

func previousStream(streams []localityStream, i int) string {
	if i == 0 {
		return ""
	}
	return streams[i-1].Key
}

// rechunkRun bounds a run starting at a fragment by job size, source objects
// and the cooldown of its newest objects. entries start at the fragment;
// complete means the last entry is the stream's tail.
func (e *Engine) rechunkRun(ctx context.Context, metric string, level int64, entries []indexEntry, complete bool, options CompactionOptions, objectLimit int, cutoff int64) ([]BlockInfo, bool, error) {
	var run []BlockInfo
	var bytes int64
	objects := make(map[string]bool)
	for _, entry := range entries {
		if entry.Records <= 0 || len(run) >= options.JobMaxBlocks || bytes+entry.Blob.Length > options.JobMaxBytes {
			break
		}
		if !objects[entry.Blob.Key] && len(objects) >= objectLimit && len(run) >= 2 {
			break
		}
		objects[entry.Blob.Key] = true
		run = append(run, BlockInfo{Metric: metric, Level: level, Entry: entry})
		bytes += entry.Blob.Length
	}
	// Newer blocks live in newer objects: trim cooling objects from the end.
	for len(run) > 1 {
		o, ok, err := e.catalogGet(ctx, e.state.Catalog, run[len(run)-1].Entry.Blob.Key)
		if errors.Is(err, errCatalogBudget) {
			return nil, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if ok && o.Modified <= cutoff {
			break
		}
		run = run[:len(run)-1]
	}
	var tail blob
	if complete && len(entries) > 0 {
		tail = entries[len(entries)-1].Blob
	}
	// Prefer the longest prefix whose remainder stays a merge seed or ends at
	// the tail; the fragment scan finds larger remainders again otherwise.
	total := 0
	for _, b := range run {
		total += b.Entry.Records
	}
	for cut, rest := len(run), total; cut >= 2; cut-- {
		if run[cut-1].Entry.Blob == tail || rest%maxDataBlockRecords <= maxDataBlockRecords/2 {
			run = run[:cut]
			break
		}
		rest -= run[cut-1].Entry.Records
	}
	if len(run) < 2 {
		return nil, false, nil
	}
	return run, false, nil
}
