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
	// Fallback for mixed sizes: only sections below the top tier (a quarter
	// of the target) count and merge. Top-tier sections merge only with each
	// other above; otherwise a section just below half the target would be
	// rewritten whole to absorb a few small sections, again and again until it
	// settles.
	small := target / int64(fanIn)
	unsettled := 0
	for _, sec := range sections {
		if sec.bytes < small {
			unsettled++
		}
	}
	if unsettled > 4*fanIn {
		for i := 0; i+fanIn <= len(sections); i++ {
			j, total := i, int64(0)
			for j < len(sections) && sections[j].bytes < small && total+sections[j].bytes <= target {
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

func countNewObjects(group, selected map[string]bool) int {
	n := 0
	for key := range group {
		if !selected[key] {
			n++
		}
	}
	return n
}
