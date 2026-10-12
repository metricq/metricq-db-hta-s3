package engine

import (
	"context"
	"errors"
	"math"
)

// smallObjectLimit is the size below which an object is a repack start
// object even when fully live.
func (o CompactionOptions) smallObjectLimit() int64 {
	return min(int64(smallObjectBytes), o.OutputObjectBytes/2)
}

// repackNodeReads bounds the index pages one repack selection reads; it
// stops gathering at three quarters of it. A checkpoint object holds blocks
// of hundreds of streams, each planned from up to repackStreamEntries of its
// index entries after its settled prefix.
const (
	repackNodeReads     = 16384
	repackStreamEntries = 2048
)

// repackFill is the share of an output object a repack job gathers before
// it runs: its output is then nearly full and never a candidate again.
const repackFillPercent = 75

// mergeCascade applies localityMerge until no run of a stream's sections is
// due anymore, as if each merge were a job of its own, and returns the
// resulting runs of more than one original section as [begin, end) indices
// into sections. Merging in one job what tiered jobs would merge one after
// the other writes each byte once instead of once per step.
func mergeCascade(sections []localitySection, target int64, fanIn int) [][2]int {
	virtual := make([]localitySection, len(sections))
	for i, s := range sections {
		virtual[i] = localitySection{begin: i, end: i + 1, bytes: s.bytes}
	}
	for len(virtual) > 1 {
		first, end := localityMerge(virtual, target, fanIn)
		if first == end {
			break
		}
		merged := localitySection{begin: virtual[first].begin, end: virtual[end-1].end}
		for _, v := range virtual[first:end] {
			merged.bytes += v.bytes
		}
		virtual = append(append(virtual[:first:first], merged), virtual[end:]...)
	}
	var runs [][2]int
	for _, v := range virtual {
		if v.end-v.begin > 1 {
			runs = append(runs, [2]int{v.begin, v.end})
		}
	}
	return runs
}

// repackStreamPlan returns the blocks of one stream that a repack job moves
// besides the stream's blocks in the start object: the runs of sections the
// tier rule merges now. The open suffix (partial tail blocks) is left to
// merging; a settled prefix (sections of at least half the target) is
// remembered and not read again.
func (e *Engine) repackStreamPlan(ctx context.Context, snapshot *Engine, options CompactionOptions, metric string, level int64) ([]BlockInfo, error) {
	if options.LocalityDisabled {
		return nil, nil
	}
	root := snapshot.state.Roots[metric][level]
	if root.Key == "" {
		return nil, nil
	}
	key := streamKey(metric, level)
	after, ok := e.repackAfter[key]
	if !ok {
		after = math.MinInt64
	}
	limit := repackStreamEntries
	entries, err := snapshot.indexEntriesAfter(ctx, root, after, limit)
	if err != nil {
		return nil, err
	}
	if len(entries) < limit {
		blocks, _ := openSuffix(entries)
		entries = entries[:len(entries)-blocks]
	}
	target := options.OutputObjectBytes
	sections := localitySections(entries)
	next := after
	for _, sec := range sections {
		if sec.bytes < target/2 || entries[sec.end-1].Last == math.MaxInt64 {
			break
		}
		next = entries[sec.end-1].Last + 1
	}
	if e.repackAfter == nil {
		e.repackAfter = make(map[string]int64)
	}
	e.repackAfter[key] = next
	var out []BlockInfo
	for _, run := range mergeCascade(sections, target, options.LocalityFanIn) {
		for _, entry := range entries[sections[run[0]].begin:sections[run[1]-1].end] {
			out = append(out, BlockInfo{Metric: metric, Level: level, Entry: entry})
		}
	}
	return out, nil
}

// selectRepack plans one job for both layout goals at once: contiguous
// streams and full objects. Start objects are small (below half the object
// target) or have reached compaction_reclaim_dead_fraction. A job takes a
// start object whole together with, for each of its streams, the runs of
// sections the tier rule merges now, so moved sections join their stream once
// and nothing has to be pulled apart again later. It gathers start objects up
// to the job limits (compaction_job_max_bytes and _blocks, the source object
// limit, its index read budget) and runs once its input fills 75 % of an
// output object. Outputs are filled stream by stream without splitting a
// run, so at most the last output of a job stays partly empty. After a
// complete pass over the candidates a job also runs with less if it reclaims
// dead bytes or merges sections. A start object larger than a job moves
// stream by stream.
func (e *Engine) selectRepack(ctx context.Context, snapshot *Engine, options CompactionOptions, objectLimit int, cutoff int64) ([]BlockInfo, bool, error) {
	limit := options.smallObjectLimit()
	capacity := options.JobMaxBytes
	fill := min(capacity, options.OutputObjectBytes) * repackFillPercent / 100
	picked := map[blob]bool{}
	objects := map[string]bool{}
	var inputs []BlockInfo
	var bytes int64
	dirtySeen, mergesDue := false, false
	var selectErr error
	previous := e.repackCursor
	resume, stopped, limited := "", false, false
	// add takes a group of blocks entirely or not at all; the first group of a
	// job is cut to the job limits instead (a stream too large for one job).
	add := func(group []BlockInfo) bool {
		if len(inputs) == 0 {
			var n int64
			fresh := map[string]bool{}
			for i, b := range group {
				fresh[b.Entry.Blob.Key] = true
				n += b.Entry.Blob.Length
				if i >= options.JobMaxBlocks || (i > 0 && (n > capacity || len(fresh) > objectLimit)) {
					group = group[:i]
					break
				}
			}
		}
		var n int64
		count := 0
		fresh := map[string]bool{}
		seen := map[blob]bool{}
		for _, b := range group {
			if picked[b.Entry.Blob] || seen[b.Entry.Blob] {
				continue
			}
			seen[b.Entry.Blob] = true
			n += b.Entry.Blob.Length
			count++
			if !objects[b.Entry.Blob.Key] {
				fresh[b.Entry.Blob.Key] = true
			}
		}
		if len(inputs) > 0 && (bytes+n > capacity || len(inputs)+count > options.JobMaxBlocks || len(objects)+len(fresh) > objectLimit) {
			return false
		}
		for _, b := range group {
			if !picked[b.Entry.Blob] {
				picked[b.Entry.Blob] = true
				inputs = append(inputs, b)
				objects[b.Entry.Blob.Key] = true
			}
		}
		bytes += n
		return true
	}
	cursor, err := snapshot.catalogScan(ctx, snapshot.state.Candidates, e.repackCursor, 256, func(c ObjectInfo) bool {
		if len(inputs) > 0 && snapshot.nodeReads >= repackNodeReads*3/4 {
			// Out of read budget: run with what is gathered, so selection
			// always progresses instead of gathering the same objects again.
			resume, stopped, limited = previous, true, true
			return false
		}
		if c.Size >= limit && c.Size > 0 {
			// Large objects are candidates only for dead bytes or fragments;
			// the entry's rank tells their dead fraction without a catalog read.
			if dead := float64(999-candidateRank(c.Key)) / 999; dead < options.ReclaimDeadFraction {
				previous = c.Key
				return true
			}
		}
		o, ok, err := snapshot.catalogGet(ctx, snapshot.state.Catalog, c.Target)
		if errors.Is(err, errCatalogBudget) {
			resume, stopped, limited = previous, true, len(inputs) > 0
			return false
		}
		if err != nil {
			selectErr = err
			return false
		}
		if !ok || o.Size == 0 || o.Modified > cutoff || len(o.Blocks) == 0 {
			previous = c.Key
			return true
		}
		dirty := float64(o.Size-o.LiveBytes)/float64(o.Size) >= options.ReclaimDeadFraction
		// An index pack of hundreds of small pages cannot grow much by
		// packing; it moves only to reclaim dead bytes. A dirty object larger
		// than one output moves stream by stream over several jobs.
		settled := len(o.Blocks) >= options.JobMaxBlocks/4
		for _, b := range o.Blocks {
			settled = settled && b.Index
		}
		if !dirty && (o.Size >= limit || settled || o.LiveBytes > capacity) {
			previous = c.Key
			return true
		}
		// The start object's blocks per stream, each with its due merges.
		type stream struct {
			metric string
			level  int64
			index  bool
		}
		var order []stream
		groups := map[stream][]BlockInfo{}
		for _, b := range o.Blocks {
			s := stream{b.Metric, b.Level, b.Index}
			if _, seen := groups[s]; !seen {
				order = append(order, s)
			}
			groups[s] = append(groups[s], b)
		}
		whole, taken := true, false
		for _, s := range order {
			group := groups[s]
			if !s.index {
				due, err := e.repackStreamPlan(ctx, snapshot, options, s.metric, s.level)
				if err != nil {
					selectErr = err
					return false
				}
				mergesDue = mergesDue || len(due) > 0
				group = append(group, due...)
			}
			if !add(group) {
				whole = false
				break
			}
			taken = true
		}
		dirtySeen = dirtySeen || (dirty && taken)
		if !whole {
			// The job is full by its limits. The next job starts with this
			// object; one taken only in part (too large for a job) continues.
			resume, stopped, limited = previous, true, true
			return false
		}
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
	e.repackCursor = cursor
	if len(inputs) == 0 || (bytes < fill && !limited && !(complete && (dirtySeen || mergesDue))) {
		return nil, !complete, nil
	}
	return inputs, !complete, nil
}

// candidateRank is the rank a candidate key starts with (999 = no dead bytes).
func candidateRank(key string) int {
	rank := 0
	for i := 0; i < len(key) && key[i] >= '0' && key[i] <= '9'; i++ {
		rank = rank*10 + int(key[i]-'0')
	}
	return rank
}
