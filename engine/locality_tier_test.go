package engine

import "testing"

func sectionsOf(sizes ...int64) []localitySection {
	var out []localitySection
	for i, size := range sizes {
		out = append(out, localitySection{begin: i, end: i + 1, bytes: size})
	}
	return out
}

func TestLocalityMergeRule(t *testing.T) {
	const target = 4 << 20
	k := int64(1 << 10)
	m := int64(1 << 20)
	cases := []struct {
		name       string
		sizes      []int64
		first, end int
	}{
		{"three small sections wait", []int64{40 * k, 40 * k, 40 * k}, 0, 0},
		{"four similar sections merge", []int64{40 * k, 41 * k, 39 * k, 40 * k}, 0, 4},
		{"sizes across tier boundaries merge", []int64{3 * k, 5 * k, 3 * k, 4 * k}, 0, 4},
		{"large section waits for small ones to grow", []int64{300 * k, 40 * k, 40 * k, 40 * k, 40 * k}, 1, 5},
		{"run extends while it stays mergeable", []int64{2 * k, 2 * k, 2 * k, 18 * k, 2 * k, 10 * k, 5 * k, 2 * k, 4 * k}, 0, 9},
		{"outlier stops a run", []int64{2 * k, 2 * k, 2 * k, 2 * k, 900 * k}, 0, 4},
		{"two sections below target wait", []int64{1100 * k, 1100 * k}, 0, 0},
		{"a run reaching target merges whatever the sizes", []int64{1950 * m / 1000, 1500 * m / 1000, 1400 * m / 1000, 40 * k}, 0, 3},
		{"a run ends at the section reaching target", []int64{2100 * m / 1000, 2700 * m / 1000, 2900 * m / 1000, 2500 * m / 1000}, 0, 2},
		{"a large section waits below target", []int64{3700 * k, 4 * k, 4 * k, 4 * k, 4 * k}, 1, 5},
		{"a large section fills up", []int64{3700 * k, 400 * k}, 0, 2},
		{"full sections are never merged among themselves", []int64{target, 3900 * k, 40 * k, 40 * k, 40 * k, 40 * k}, 2, 6},
		{"a smaller section before full ones merges into them", []int64{40 * k, target, 3900 * k}, 0, 2},
		{"small sections before full ones merge into them", []int64{40 * k, 40 * k, 3900 * k, target}, 0, 3},
	}
	for _, c := range cases {
		first, end := localityMerge(sectionsOf(c.sizes...), target, 4)
		if first != c.first || end != c.end {
			t.Errorf("%s: got [%d,%d), want [%d,%d)", c.name, first, end, c.first, c.end)
		}
	}
}

// applyMerges appends chunks to a stream and merges as selected, cutting
// merged runs as copying does. It checks that merges below the target at
// least double every section they rewrite and that only the newest sections
// are not full, and returns the rewrite factor and the most sections that
// were not full at once.
func applyMerges(t *testing.T, sizes []int64, chunks func(step int) int64, steps int) ([]int64, float64, int) {
	t.Helper()
	const target = 4 << 20
	full := fullSectionBytes(target, 0)
	var written, appended int64
	most := 0
	for step := 0; step < steps; step++ {
		chunk := chunks(step)
		sizes = append(sizes, chunk)
		appended += chunk
		for {
			first, end := localityMerge(sectionsOf(sizes...), target, 4)
			if first == end {
				break
			}
			var merged, largest int64
			for _, size := range sizes[first:end] {
				merged += size
				largest = max(largest, size)
			}
			if merged < target && 2*largest > merged {
				t.Fatalf("step %d: merge %v grows its largest section by less than twice", step, sizes[first:end])
			}
			written += merged
			sizes = append(append(append([]int64(nil), sizes[:first]...), cutSections(merged, target)...), sizes[end:]...)
		}
		open := 0
		for i, size := range sizes {
			if size < full {
				open++
			} else if open > 0 {
				t.Fatalf("step %d: full section %d behind %d smaller ones: %v", step, i, open, sizes)
			}
		}
		most = max(most, open)
	}
	return sizes, float64(written) / float64(appended), most
}

// A stream of equal chunks becomes full sections plus a few small ones, and
// every byte is rewritten a bounded number of times.
func TestLocalityMergeConvergesGeometrically(t *testing.T) {
	sizes, amplification, most := applyMerges(t, nil, func(int) int64 { return 40 << 10 }, 2000)
	if amplification > 4.5 || most > 8 {
		t.Fatalf("rewrite factor %.2f, up to %d sections not full: %v", amplification, most, sizes)
	}
	t.Logf("%d sections, rewrite factor %.2f, up to %d not full", len(sizes), amplification, most)
}

// A large section that is not full is not rewritten to absorb a few small
// ones, whatever its size: the small sections merge among themselves.
func TestLocalityMergeLeavesLargeSectionsAlone(t *testing.T) {
	const target, fanIn = 4 << 20, 4
	for _, large := range []int64{3700 << 10, 1900 << 10, 900 << 10, 132 << 10} {
		sizes := []int64{large}
		for i := 0; i < 17; i++ {
			sizes = append(sizes, 3<<10+int64(i%3)<<12)
		}
		first, end := localityMerge(sectionsOf(sizes...), target, fanIn)
		if first == end {
			t.Fatalf("%d KiB: seventeen small sections not merged", large>>10)
		}
		if first == 0 {
			t.Fatalf("%d KiB section rewritten with small ones: %d..%d", large>>10, first, end)
		}
	}
}

// Sections of irregular size, such as those settled at half the target by an
// earlier rule, are merged and cut into full sections from the front.
func TestLocalityMergeCutsIntoFullSections(t *testing.T) {
	const target = 4 << 20
	m := int64(1 << 20)
	sizes := []int64{21 * m / 10, 27 * m / 10, 29 * m / 10, 25 * m / 10}
	runs := mergeCascade(sectionsOf(sizes...), target, 4)
	if len(runs) != 1 || runs[0] != [2]int{0, 4} {
		t.Fatalf("runs %v, want all four sections", runs)
	}
	total := sizes[0] + sizes[1] + sizes[2] + sizes[3]
	got := cutSections(total, target)
	if len(got) != 3 || got[0] != target || got[1] != target || got[2] != total-2*target {
		t.Fatalf("cut into %v", got)
	}
}

// With checkpoint sections of varying size, only the newest sections of a
// stream are not full, and they stay few.
func TestLocalityMergeBoundsRewrites(t *testing.T) {
	pattern := []int64{2, 2, 2, 3, 4, 5, 10, 18, 2, 3, 2, 4, 2}
	sizes, amplification, most := applyMerges(t, []int64{1900 << 10}, func(step int) int64 { return pattern[step*7%len(pattern)] << 10 }, 50000)
	if amplification > 7 || most > 14 {
		t.Fatalf("rewrite factor %.2f, up to %d sections not full", amplification, most)
	}
	t.Logf("%d sections, rewrite factor %.2f, up to %d not full", len(sizes), amplification, most)
}
