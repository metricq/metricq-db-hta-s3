package engine

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
	block      int64 // largest block
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
				last.block = max(last.block, entry.Blob.Length)
				continue
			}
		}
		sections = append(sections, localitySection{begin: i, end: i + 1, bytes: entry.Blob.Length, block: entry.Blob.Length})
	}
	return sections
}

// fullSectionBytes is the size from which a section whose largest block has
// the given size counts as full: copying cuts a run where the next block no
// longer fits, so a cut section may lack up to about one block (its largest,
// at least a sixteenth of the target is allowed for). Full sections are final
// and never merged again; counting a cut section as not full would rewrite it
// forever. The bound depends on the section alone, so every plan of a stream
// agrees on it, whichever part of the stream it reads.
func fullSectionBytes(target, block int64) int64 {
	return target - max(block, target/16)
}

// full tells whether the section is full (see fullSectionBytes).
func (s localitySection) full(target int64) bool {
	return s.bytes >= fullSectionBytes(target, s.block)
}

// largestBlock is the largest block of the sections.
func largestBlock(sections []localitySection) int64 {
	var block int64
	for _, s := range sections {
		block = max(block, s.block)
	}
	return block
}

// localityMerge picks the oldest run of consecutive sections that starts with
// one that is not full and may merge, and extends it as far as it stays
// mergeable but no further than the first section that brings it to target.
// Copying cuts a merged run into full sections of target and a remainder,
// which joins the next merge at the front, so a stream's past consists of full
// sections and only its newest sections are smaller.
//
// A run that reaches target may always merge: its full sections are written
// for the last time. A run may end with a full section, so a smaller section
// before full ones is merged into them and its remainder passed on until the
// stream's newest sections; nothing else would ever merge it. Below target,
// the largest section must be at most the sum of the others and the run must
// hold at least fanIn sections. Such a
// merge at least doubles every section it rewrites, so a large section is
// never rewritten to absorb a few small ones: the small sections merge among
// themselves until they match it. Sizes need not fall into fixed tiers, so
// checkpoint sections of varying size do not block each other.
func localityMerge(sections []localitySection, target int64, fanIn int) (int, int) {
	for i := range sections {
		end := i
		var total, largest int64
		for j := i; j < len(sections); j++ {
			if sections[j].full(target) {
				if j > i {
					end = j + 1
				}
				break
			}
			total += sections[j].bytes
			largest = max(largest, sections[j].bytes)
			if n := j + 1 - i; n >= 2 && (total >= target || 2*largest <= total && n >= fanIn) {
				end = j + 1
			}
			if total >= target {
				break
			}
		}
		if end > i {
			return i, end
		}
	}
	return 0, 0
}

// cutSections returns the sizes a merged run of total bytes is copied into:
// full sections of target and a remainder.
func cutSections(total, target int64) []int64 {
	var out []int64
	for ; total >= target; total -= target {
		out = append(out, target)
	}
	if total > 0 {
		out = append(out, total)
	}
	return out
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
