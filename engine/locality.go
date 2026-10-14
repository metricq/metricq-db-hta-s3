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

// localityMerge picks the oldest run of consecutive unsettled sections
// (below half the target) that may merge, and extends it as far as it stays
// mergeable. A run may merge when it fits into target, its largest section is
// at most the sum of the others, and it holds at least fanIn sections or
// settles (reaches half the target). The merged section is thus at least
// twice as large as each of its parts: every byte is rewritten at most
// log2(target/section) times, and a large section is never rewritten to absorb
// a few small ones. Small sections merge among themselves first, until they
// match the large one. Sizes need not fall into fixed tiers, so checkpoint
// sections of varying size do not block each other.
func localityMerge(sections []localitySection, target int64, fanIn int) (int, int) {
	settled := target / 2
	for i := range sections {
		end := i
		var total, largest int64
		for j := i; j < len(sections) && sections[j].bytes < settled && total+sections[j].bytes <= target; j++ {
			total += sections[j].bytes
			largest = max(largest, sections[j].bytes)
			if n := j + 1 - i; n >= 2 && 2*largest <= total && (n >= fanIn || total >= settled) {
				end = j + 1
			}
		}
		if end > i {
			return i, end
		}
	}
	return 0, 0
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
