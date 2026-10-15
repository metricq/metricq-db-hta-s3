package engine

import (
	"context"
	"log/slog"
	"math"
	"sort"
)

// streamLayout is what repack planning knows about one stream: its sections
// behind the prefix of full sections and its open suffix. Checkpoints and
// jobs keep it current from the entries they write, so planning reads no
// index pages but the blocks of a run that is due. A stream without a
// current layout (after a restart, or a change the layout cannot follow) is
// read from the index once.
type streamLayout struct {
	root     blob          // the index root the layout describes
	sections []planSection // entries behind the full prefix, open suffix excluded
	open     []indexEntry  // the open suffix
	// The plan at root, once computed.
	planned bool
	due     []BlockInfo
	cuts    []blob
}

// planSection summarizes a section: size, largest block, number of
// entries, time range and last entry.
type planSection struct {
	bytes, block int64
	count        int
	first, last  int64
	tail         indexEntry
}

func (s planSection) full(target int64) bool {
	return s.bytes >= fullSectionBytes(target, s.block)
}

// repackPrefix is where a stream continues behind its prefix of full
// sections: the time after it and its last entry. Entries stored
// contiguously behind that entry still belong to its section.
type repackPrefix struct {
	after int64
	last  indexEntry
}

// contiguous tells whether entry is stored right behind prev, so that both
// belong to one section (see localitySections).
func contiguous(prev, entry *indexEntry) bool {
	return entry.Blob.Key == prev.Blob.Key && entry.Blob.Offset >= prev.Blob.Offset && entry.Blob.Offset-prev.Blob.Offset-prev.Blob.Length <= maxCoalescedGap
}

// appendSections adds entries in stream order to the sections, extending the
// last section while they are stored contiguously behind it. Without
// sections, entries stored contiguously behind the full prefix (nil for
// none) join its last section and move it on.
func appendSections(sections []planSection, prefix *repackPrefix, entries []indexEntry) []planSection {
	for _, entry := range entries {
		if n := len(sections); n > 0 {
			if last := &sections[n-1]; contiguous(&last.tail, &entry) {
				last.bytes += entry.Blob.Length
				last.block = max(last.block, entry.Blob.Length)
				last.count++
				last.last = entry.Last
				last.tail = entry
				continue
			}
		} else if prefix != nil && prefix.last.Blob.Key != "" && contiguous(&prefix.last, &entry) {
			*prefix = repackPrefix{after: entry.Last + 1, last: entry}
			continue
		}
		sections = append(sections, planSection{bytes: entry.Blob.Length, block: entry.Blob.Length, count: 1, first: entry.First, last: entry.Last, tail: entry})
	}
	return sections
}

// extend appends entries that follow the layout's last entry: the new open
// suffix stays open, the entries before it join the sections.
func (l *streamLayout) extend(prefix *repackPrefix, entries []indexEntry) {
	list := append(append([]indexEntry(nil), l.open...), entries...)
	n, _ := openSuffix(list)
	l.open = list[len(list)-n:]
	l.sections = appendSections(l.sections, prefix, list[:len(list)-n])
}

// prefixOf returns the stream's full prefix for changing it in place, nil
// if it has none. Caller holds layoutMu.
func (e *Engine) prefixOf(key string) *repackPrefix {
	if p, ok := e.repackAfter[key]; ok {
		return &p
	}
	return nil
}

func (e *Engine) setPrefix(key string, p *repackPrefix) {
	if p == nil {
		return
	}
	if e.repackAfter == nil {
		e.repackAfter = make(map[string]repackPrefix)
	}
	e.repackAfter[key] = *p
}

// layoutAppended follows a checkpoint that appended items to the stream, or
// replaced its last entry by them, moving its root from previous to root.
// Caller holds layoutMu.
func (e *Engine) layoutAppended(key string, previous, root blob, items []indexEntry, replaceTail bool) {
	l, ok := e.repackLayouts[key]
	if !ok {
		return
	}
	if l.root != previous {
		e.dropLayout(key, "checkpoint_root")
		return
	}
	if replaceTail {
		// Only a partial last block is replaced, and it is open.
		n := len(l.open)
		if n == 0 || l.open[n-1].First != items[0].First {
			e.dropLayout(key, "tail_replaced")
			return
		}
		l.open = l.open[:n-1]
	}
	prefix := e.prefixOf(key)
	l.extend(prefix, items)
	e.setPrefix(key, prefix)
	l.root, l.planned, l.due, l.cuts = root, false, nil, nil
}

// movedBlock is a block a job replaced: its old entry and its new one,
// unless the job dropped it.
type movedBlock struct {
	old, new indexEntry
	dropped  bool
}

// layoutMoved follows a job that replaced blocks of the stream, giving it
// root. Whole sections and open entries are replaced by the new entries. A
// job that took part of a section (a merge or rechunk, or a group cut to the
// job limits) leaves a layout this cannot follow, which is dropped and read
// again; one that moved blocks within the full prefix may have changed its
// sections (merged blocks shrink), so the stream is read from its start.
// Caller holds layoutMu.
func (e *Engine) layoutMoved(key string, moved []movedBlock, root blob) {
	prefix := e.prefixOf(key)
	replaced := make(map[blob]movedBlock, len(moved))
	for _, m := range moved {
		replaced[m.old.Blob] = m
		if prefix != nil && m.old.First < prefix.after {
			delete(e.repackAfter, key)
			e.dropLayout(key, "prefix_moved")
			return
		}
	}
	l, ok := e.repackLayouts[key]
	if !ok {
		return
	}
	var open []indexEntry
	for _, entry := range l.open {
		if _, ok := replaced[entry.Blob]; ok {
			delete(replaced, entry.Blob)
			continue
		}
		open = append(open, entry)
	}
	taken := make([]int, len(l.sections))
	for _, m := range replaced {
		i := sort.Search(len(l.sections), func(i int) bool { return l.sections[i].last >= m.old.First })
		if i == len(l.sections) || l.sections[i].first > m.old.First || l.sections[i].last < m.old.Last {
			e.dropLayout(key, "unknown_block")
			return
		}
		taken[i]++
	}
	var sections []planSection
	for i, sec := range l.sections {
		switch taken[i] {
		case 0:
			sections = append(sections, sec)
		case sec.count:
		default:
			e.dropLayout(key, "partial_section")
			return
		}
	}
	var added []indexEntry
	seen := make(map[blob]bool)
	for _, m := range moved {
		if !m.dropped && !seen[m.new.Blob] {
			seen[m.new.Blob] = true
			added = append(added, m.new)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].First < added[j].First })
	// New entries among the remaining sections form sections of their own
	// (they are in new objects); those behind them join the open suffix.
	end := int64(math.MinInt64)
	if n := len(sections); n > 0 {
		end = sections[n-1].last
	}
	split := sort.Search(len(added), func(i int) bool { return added[i].First > end })
	for _, sec := range appendSections(nil, nil, added[:split]) {
		i := sort.Search(len(sections), func(i int) bool { return sections[i].first > sec.first })
		sections = append(sections[:i], append([]planSection{sec}, sections[i:]...)...)
	}
	tail := append(open, added[split:]...)
	sort.Slice(tail, func(i, j int) bool { return tail[i].First < tail[j].First })
	l.sections, l.open = sections, nil
	l.extend(prefix, tail)
	e.setPrefix(key, prefix)
	l.root, l.planned, l.due, l.cuts = root, false, nil, nil
}

// dropLayout forgets a stream's layout that no longer follows its index,
// counting why. Caller holds layoutMu.
func (e *Engine) dropLayout(key, reason string) {
	if _, ok := e.repackLayouts[key]; ok {
		delete(e.repackLayouts, key)
		e.metrics.CompactionRepackLayoutDrops.WithLabelValues(reason).Inc()
	}
}

// layoutRelocated follows a job that moved only index pages of the stream
// to new objects: its entries are unchanged, its root is not. Caller holds
// layoutMu.
func (e *Engine) layoutRelocated(key string, root blob) {
	if l, ok := e.repackLayouts[key]; ok {
		l.root = root
	}
}

// layoutState tells whether the stream has a layout at root (planning it
// reads no index pages) and whether its plan there found nothing due.
func (e *Engine) layoutState(key string, root blob) (known, idle bool) {
	e.layoutMu.Lock()
	defer e.layoutMu.Unlock()
	l, ok := e.repackLayouts[key]
	if !ok || l.root != root {
		return false, false
	}
	return true, l.planned && len(l.due) == 0
}

// pruneLayouts forgets the layouts of streams that no longer exist.
func (e *Engine) pruneLayouts(streams []localityStream) {
	e.layoutMu.Lock()
	defer e.layoutMu.Unlock()
	if len(e.repackLayouts) <= 2*len(streams) {
		return
	}
	kept := make(map[string]*streamLayout, len(streams))
	for _, st := range streams {
		if l, ok := e.repackLayouts[st.Key]; ok {
			kept[st.Key] = l
		}
	}
	e.repackLayouts = kept
}

// forgetLayouts drops every layout, after a job failed: its plans may no
// longer match the index.
func (e *Engine) forgetLayouts() {
	e.layoutMu.Lock()
	e.repackLayouts = nil
	e.layoutMu.Unlock()
}

// repackStreamPlan returns the blocks of one stream that a repack job moves
// besides the stream's blocks in the start object: the runs of sections the
// merge rule merges now. The open suffix (partial tail blocks) is left to
// merging; a prefix of full sections is remembered and not read again.
//
// Planning works on the stream's layout (see streamLayout); only a stream
// without a current one is read from the index, behind its full prefix. The
// blocks of a due run are read when it is due. The plan is kept with the
// layout until the next change.
func (e *Engine) repackStreamPlan(ctx context.Context, snapshot *Engine, options CompactionOptions, metric string, level int64) ([]BlockInfo, error) {
	if options.LocalityDisabled {
		return nil, nil
	}
	root := snapshot.state.Roots[metric][level]
	if root.Key == "" {
		return nil, nil
	}
	key := streamKey(metric, level)
	target := options.OutputObjectBytes
	e.layoutMu.Lock()
	layout, known := e.repackLayouts[key]
	if known && layout.root != root {
		// Not dropped: a checkpoint may already have moved the layout past
		// this selection's snapshot.
		e.metrics.CompactionRepackLayoutDrops.WithLabelValues("snapshot_root").Inc()
	}
	known = known && layout.root == root
	if known && layout.planned {
		due, cuts := layout.due, layout.cuts
		e.layoutMu.Unlock()
		e.metrics.CompactionRepackPlans.WithLabelValues("cached").Inc()
		e.noteCuts(cuts)
		return due, nil
	}
	prefix := e.prefixOf(key)
	var sections []planSection
	if known {
		sections = append([]planSection(nil), layout.sections...)
	}
	e.layoutMu.Unlock()
	keep := true
	if known {
		e.metrics.CompactionRepackPlans.WithLabelValues("layout").Inc()
	} else {
		e.metrics.CompactionRepackPlans.WithLabelValues("read").Inc()
		after := int64(math.MinInt64)
		if prefix != nil {
			after = prefix.after
		}
		entries, err := snapshot.indexEntriesAfter(ctx, root, after, repackStreamEntries)
		if err != nil {
			return nil, err
		}
		layout = &streamLayout{root: root}
		if len(entries) < repackStreamEntries {
			n, _ := openSuffix(entries)
			layout.open = append([]indexEntry(nil), entries[len(entries)-n:]...)
			entries = entries[:len(entries)-n]
		} else {
			// A window short of the stream's end: plan it, keep nothing.
			keep = false
		}
		sections = appendSections(nil, prefix, entries)
	}
	// Leading full sections are final: the stream continues behind them.
	for len(sections) > 0 && sections[0].full(target) && sections[0].last != math.MaxInt64 {
		prefix = &repackPrefix{after: sections[0].last + 1, last: sections[0].tail}
		sections = sections[1:]
	}
	window := make([]localitySection, len(sections))
	for i, sec := range sections {
		window[i] = localitySection{begin: i, end: i + 1, bytes: sec.bytes, block: sec.block}
	}
	var out []BlockInfo
	var cuts []blob
	for _, run := range mergeCascade(window, target, options.LocalityFanIn) {
		count := 0
		for _, sec := range sections[run[0]:run[1]] {
			count += sec.count
		}
		first, last := sections[run[0]].first, sections[run[1]-1].last
		entries, err := snapshot.indexEntriesAfter(ctx, root, first, count)
		if err != nil {
			return nil, err
		}
		if len(entries) != count || entries[0].First != first || entries[len(entries)-1].Last != last {
			// The layout no longer matches the index: read it again next time.
			slog.Warn("repack layout does not match the index", "stream", key)
			e.layoutMu.Lock()
			e.dropLayout(key, "run_mismatch")
			e.layoutMu.Unlock()
			return nil, nil
		}
		var bytes int64
		for _, entry := range entries {
			out = append(out, BlockInfo{Metric: metric, Level: level, Entry: entry})
			bytes += entry.Blob.Length
		}
		if bytes >= target {
			// Copying cuts the run into full objects from its first block on.
			cuts = append(cuts, entries[0].Blob)
		}
	}
	e.noteCuts(cuts)
	e.layoutMu.Lock()
	defer e.layoutMu.Unlock()
	if current, ok := e.repackAfter[key]; prefix != nil && (!ok || current.after < prefix.after) {
		e.setPrefix(key, prefix)
	}
	if !keep {
		return out, nil
	}
	if known {
		// Keep the plan unless a checkpoint changed the layout meanwhile.
		if current, ok := e.repackLayouts[key]; !ok || current != layout || current.root != root {
			return out, nil
		}
	} else {
		// A checkpoint may have committed meanwhile, and its layout update
		// found no layout: a layout of this older snapshot would miss it.
		e.mu.Lock()
		current := e.committed.Roots[metric][level]
		e.mu.Unlock()
		if current != root {
			return out, nil
		}
		if e.repackLayouts == nil {
			e.repackLayouts = make(map[string]*streamLayout)
		}
		e.repackLayouts[key] = layout
	}
	layout.sections = sections
	layout.planned, layout.due, layout.cuts = true, out, cuts
	return out, nil
}
