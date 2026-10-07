package engine

import (
	"context"
	"errors"
)

// candidateSize caches the object size of a candidate entry written before
// candidate entries carried it, so consolidation reads its catalog entry once.
type candidateSize struct {
	modified, size int64
	pass           uint64
}

// selectConsolidation packs small objects together. Merge outputs and the
// index packs of jobs are small, and evacuating one object only shrinks it
// further, so without packing their number grows with the number of jobs.
// Index pages are rewritten with their ancestors into the job's index pack. Objects below compaction_output_object_bytes/8 (at most
// smallObjectBytes) are collected, oldest candidates first, until their live
// bytes fill one output object; a job runs once eight are found, or four that
// fill an eighth of the output.
func (e *Engine) selectConsolidation(ctx context.Context, snapshot *Engine, options CompactionOptions, objectLimit int, cutoff int64) ([]BlockInfo, bool, error) {
	limit := min(int64(smallObjectBytes), options.OutputObjectBytes/8)
	capacity := min(options.JobMaxBytes, options.OutputObjectBytes)
	if e.candidateSizes == nil {
		e.candidateSizes = make(map[string]candidateSize)
	}
	var picked []ObjectInfo
	var bytes int64
	blocks := 0
	var selectErr error
	previous := e.consolidationCursor
	resume, stopped := "", false
	cursor, err := snapshot.catalogScan(ctx, snapshot.state.Candidates, e.consolidationCursor, 256, func(c ObjectInfo) bool {
		size := c.Size
		if known, ok := e.candidateSizes[c.Key]; ok && known.modified == c.Modified {
			size = known.size
			known.pass = e.consolidationPass
			e.candidateSizes[c.Key] = known
		}
		if size >= limit {
			previous = c.Key
			return true
		}
		o, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, c.Target)
		if errors.Is(err, errCatalogBudget) {
			resume, stopped = previous, true
			return false
		}
		if err != nil {
			selectErr = err
			return false
		}
		if ok && c.Size == 0 {
			e.candidateSizes[c.Key] = candidateSize{modified: c.Modified, size: o.Size, pass: e.consolidationPass}
		}
		// An object one job cannot take whole stays as it is.
		if !ok || o.Size == 0 || o.Size >= limit || o.Modified > cutoff || len(o.Blocks) == 0 || len(o.Blocks) > options.JobMaxBlocks || o.LiveBytes > capacity {
			previous = c.Key
			return true
		}
		if len(picked) >= objectLimit || blocks+len(o.Blocks) > options.JobMaxBlocks || bytes+o.LiveBytes > capacity {
			// The next job starts with this object.
			resume, stopped = previous, true
			return false
		}
		picked = append(picked, o)
		bytes += o.LiveBytes
		blocks += len(o.Blocks)
		previous = c.Key
		return true
	})
	if err != nil {
		return nil, false, err
	}
	if selectErr != nil {
		return nil, false, selectErr
	}
	complete := cursor == "" && !stopped
	if stopped {
		cursor = resume
	}
	e.consolidationCursor = cursor
	more := !complete
	if complete {
		e.consolidationPass++
		for key, c := range e.candidateSizes {
			if c.pass+1 < e.consolidationPass {
				delete(e.candidateSizes, key)
			}
		}
	}
	// A job writes a data and an index pack of its own, so packing fewer than
	// four objects would not reduce their number and could repeat forever.
	if len(picked) < 4 || (len(picked) < 8 && bytes < limit) {
		return nil, more, nil
	}
	var inputs []BlockInfo
	for _, o := range picked {
		inputs = append(inputs, o.Blocks...)
	}
	return inputs, more, nil
}
