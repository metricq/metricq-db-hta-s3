package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"
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

// repackMergeWait bounds how long due merges wait for a nearly full job.
// Tests of small databases set it to zero.
var repackMergeWait = 30 * time.Minute

// repackFill is the share of an output object a repack job gathers before
// it runs: its output is then nearly full and never a candidate again.
const repackFillPercent = 75

// mergeCascade applies localityMerge until no run of a stream's sections is
// due anymore, as if each merge were a job of its own, and returns the
// resulting runs of more than one original section as [begin, end) indices
// into sections. Merging in one job what separate jobs would merge one after
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
// merge rule merges now. The open suffix (partial tail blocks) is left to
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
// sections the merge rule merges now, so moved sections join their stream once
// and nothing has to be pulled apart again later. It gathers start objects up
// to the job limits (compaction_job_max_bytes and _blocks, the source object
// limit, its index read budget) and runs once its input fills 75 % of an
// output object. Outputs are filled stream by stream without splitting a
// run, so at most the last output of a job stays partly empty. After a
// complete pass over the candidates a job also runs with less if it reclaims
// dead bytes. A job with due merges always runs. Streams fragmented inside
// large clean objects are in no start object, so the selection also walks
// the streams round robin and adds their due merges. A start object larger
// than a job moves stream by stream.
func (e *Engine) selectRepack(ctx context.Context, snapshot *Engine, options CompactionOptions, objectLimit int, cutoff int64) ([]BlockInfo, bool, error) {
	limit := options.smallObjectLimit()
	capacity := options.JobMaxBytes
	fill := min(capacity, options.OutputObjectBytes) * repackFillPercent / 100
	picked := map[blob]bool{}
	objects := map[string]bool{}
	var inputs []BlockInfo
	var bytes int64
	dirtySeen, mergesDue := false, false
	walkCursor, waitCursor := e.repackStreamCursor, e.repackStreamCursor
	// Where the job's bytes come from, for the debug log.
	var plan repackPlanStats
	type indexPack struct {
		o     ObjectInfo
		dirty bool
	}
	var indexPacks []indexPack
	indexDirty := false
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
		indexOnly := true
		for _, b := range o.Blocks {
			indexOnly = indexOnly && b.Index
		}
		if indexOnly {
			// Every job and checkpoint replaces index pages, so index packs
			// rank first by dead fraction. Taken first, their pages would fill
			// the job and starve the data merges; they join after the data.
			indexPacks = append(indexPacks, indexPack{o, dirty})
			indexDirty = indexDirty || dirty
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
			own := len(group)
			var due []BlockInfo
			if !s.index {
				var err error
				due, err = e.repackStreamPlan(ctx, snapshot, options, s.metric, s.level)
				if err != nil {
					selectErr = err
					return false
				}
				mergesDue = mergesDue || len(due) > 0
				group = append(group, due...)
			}
			before := bytes
			ownBytes := plan.unpicked(picked, group[:own])
			if !add(group) {
				whole = false
				break
			}
			plan.startBytes += ownBytes
			if len(due) > 0 {
				plan.dueBytes += bytes - before - ownBytes
				plan.stream(fmt.Sprintf("%s/%d", s.metric, s.level), bytes-before-ownBytes)
			}
			taken = true
		}
		if taken {
			plan.startObjects++
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
	// Streams fragmented inside large, clean objects (written by a checkpoint
	// after a restart, say) are in no start object. Walk the streams round
	// robin and add their due merges, skipping streams unchanged since a walk
	// found nothing due.
	if !options.LocalityDisabled && !(limited && len(inputs) > 0) {
		if e.repackClean == nil {
			e.repackClean = make(map[string]blob)
		}
		var streams []localityStream
		for name, levels := range snapshot.state.Roots {
			for level, root := range levels {
				if root.Key != "" {
					streams = append(streams, localityStream{name, level, streamKey(name, level)})
				}
			}
		}
		sort.Slice(streams, func(i, j int) bool { return streams[i].Key < streams[j].Key })
		start := sort.Search(len(streams), func(i int) bool { return streams[i].Key > e.repackStreamCursor })
		checked := 0
		walked, beforeDue := e.repackStreamCursor, ""
		dueFound := false
		for n := 0; n < len(streams); n++ {
			st := streams[(start+n)%len(streams)]
			root := snapshot.state.Roots[st.Metric][st.Level]
			if e.repackClean[st.Key] == root {
				continue
			}
			if checked >= 256 || snapshot.nodeReads >= repackNodeReads*3/4 {
				break
			}
			checked++
			due, err := e.repackStreamPlan(ctx, snapshot, options, st.Metric, st.Level)
			if err != nil {
				return nil, false, err
			}
			if len(due) == 0 {
				e.repackClean[st.Key] = root
				walked = st.Key
				continue
			}
			if !dueFound {
				dueFound, beforeDue = true, walked
			}
			before := bytes
			if !add(due) {
				limited = true
				break
			}
			plan.walkBytes += bytes - before
			plan.walkStreams++
			plan.stream(st.Key, bytes-before)
			mergesDue = true
			walked = st.Key
		}
		// The walk moves past due streams only with a job that merges them;
		// otherwise the next selection finds them again.
		walkCursor, waitCursor = walked, walked
		if dueFound {
			waitCursor = beforeDue
		}
	}
	// Dirty index packs fill what the data left, up to a quarter of the
	// job's blocks. Clean ones only gather in a job of index packs alone,
	// which runs after a complete pass when eight have accumulated or one is
	// dirty, so their number stays bounded without carrying them along in
	// every job.
	dataInputs := len(inputs)
	indexBlocks := 0
	for _, p := range indexPacks {
		if dataInputs > 0 && !p.dirty {
			continue
		}
		if indexBlocks+len(p.o.Blocks) > options.JobMaxBlocks/4 && indexBlocks > 0 {
			break
		}
		before := bytes
		if !add(p.o.Blocks) {
			break
		}
		plan.indexBytes += bytes - before
		indexBlocks += len(p.o.Blocks)
	}
	e.repackCursor = cursor
	if dataInputs == 0 {
		if len(inputs) == 0 || !(complete && (indexDirty || len(indexPacks) >= 8)) {
			return nil, !complete, nil
		}
		return inputs, !complete, nil
	}
	// A job waits for 75 % of an output, its limits or, at the end of a pass,
	// dead bytes to reclaim. Due merges wait as well, so jobs do not leave a
	// partly filled last output that becomes a start object again, but no
	// longer than repackMergeWait, so a quiet database still merges.
	if mergesDue && e.repackDueSince.IsZero() {
		e.repackDueSince = time.Now()
	}
	overdue := mergesDue && time.Since(e.repackDueSince) >= repackMergeWait
	if bytes < fill && !limited && !overdue && !(complete && dirtySeen) {
		e.repackStreamCursor = waitCursor
		return nil, !complete, nil
	}
	e.repackStreamCursor = walkCursor
	e.repackDueSince = time.Time{}
	plan.log(limited, mergesDue, complete && dirtySeen)
	return inputs, !complete, nil
}

// repackPlanStats splits a repack job's input by origin for the debug log.
type repackPlanStats struct {
	startObjects, walkStreams                   int
	startBytes, dueBytes, walkBytes, indexBytes int64
	streams                                     map[string]int64
}

// unpicked sums the blocks of group not yet in the job.
func (p *repackPlanStats) unpicked(picked map[blob]bool, group []BlockInfo) int64 {
	var n int64
	for _, b := range group {
		if !picked[b.Entry.Blob] {
			n += b.Entry.Blob.Length
		}
	}
	return n
}

func (p *repackPlanStats) stream(name string, bytes int64) {
	if p.streams == nil {
		p.streams = make(map[string]int64)
	}
	p.streams[name] += bytes
}

func (p *repackPlanStats) log(limited, merges, reclaim bool) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	names := make([]string, 0, len(p.streams))
	for name := range p.streams {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return p.streams[names[i]] > p.streams[names[j]] })
	var top []string
	for _, name := range names[:min(3, len(names))] {
		top = append(top, fmt.Sprintf("%s:%d", name, p.streams[name]))
	}
	slog.Debug("repack plan", "start_objects", p.startObjects, "start_bytes", p.startBytes, "due_bytes", p.dueBytes,
		"walk_streams", p.walkStreams, "walk_bytes", p.walkBytes, "index_bytes", p.indexBytes, "merge_streams", len(p.streams),
		"limited", limited, "merges", merges, "reclaim", reclaim, "top", strings.Join(top, ","))
}

// candidateRank is the rank a candidate key starts with (999 = no dead bytes).
func candidateRank(key string) int {
	rank := 0
	for i := 0; i < len(key) && key[i] >= '0' && key[i] <= '9'; i++ {
		rank = rank*10 + int(key[i]-'0')
	}
	return rank
}
