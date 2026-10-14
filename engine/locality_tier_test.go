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
		{"two sections that settle merge", []int64{1100 * k, 1100 * k}, 0, 2},
		{"top sections merge while they fit", []int64{1200 * k, 1500 * k, 1100 * k}, 0, 3},
		{"settled sections are never merged", []int64{2100 * k, 2100 * k, 4 * k, 4 * k}, 0, 0},
	}
	for _, c := range cases {
		first, end := localityMerge(sectionsOf(c.sizes...), target, 4)
		if first != c.first || end != c.end {
			t.Errorf("%s: got [%d,%d), want [%d,%d)", c.name, first, end, c.first, c.end)
		}
	}
}

// Appending small sections and merging as selected keeps few sections:
// few unsettled ones plus settled ones, and every byte is rewritten a
// bounded number of times.
func TestLocalityMergeConvergesGeometrically(t *testing.T) {
	const target = 4 << 20
	const chunk = 40 << 10
	var sizes []int64
	var written, appended int64
	for step := 0; step < 2000; step++ {
		sizes = append(sizes, chunk)
		appended += chunk
		for {
			first, end := localityMerge(sectionsOf(sizes...), target, 4)
			if first == end {
				break
			}
			var merged int64
			for _, size := range sizes[first:end] {
				merged += size
			}
			written += merged
			sizes = append(append(append([]int64(nil), sizes[:first]...), merged), sizes[end:]...)
		}
	}
	small := 0
	for _, size := range sizes {
		if size < target/2 {
			small++
		}
	}
	settled := len(sizes) - small
	// 2000 chunks of 40 KiB are 78 MiB: about 20-40 settled sections.
	if small > 12 || settled > 40 {
		t.Fatalf("%d sections (%d unsettled): %v", len(sizes), small, sizes)
	}
	if amplification := float64(written) / float64(appended); amplification > 5 {
		t.Fatalf("each byte rewritten %.1f times", amplification)
	}
	t.Logf("%d settled and %d unsettled sections, rewrite factor %.2f", settled, small, float64(written)/float64(appended))
}

// A large unsettled section is not rewritten to absorb a few small ones,
// whatever its size: the small sections merge among themselves. Before, a
// fallback for many mixed sections rewrote sections just below its size limit
// again and again.
func TestLocalityMergeLeavesLargeSectionsAlone(t *testing.T) {
	const target, fanIn = 4 << 20, 4
	for _, large := range []int64{1900 << 10, 900 << 10, 132 << 10} {
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

// Every merge at least doubles each section it rewrites, also with
// checkpoint sections of varying size, and the unsettled sections of a stream
// stay few.
func TestLocalityMergeBoundsRewrites(t *testing.T) {
	const target = 4 << 20
	pattern := []int64{2, 2, 2, 3, 4, 5, 10, 18, 2, 3, 2, 4, 2}
	sizes := []int64{1900 << 10}
	var written, appended int64
	maxUnsettled := 0
	for step := 0; step < 20000; step++ {
		chunk := pattern[step*7%len(pattern)] << 10
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
			if 2*largest > merged {
				t.Fatalf("step %d: merge %v grows its largest section by less than twice", step, sizes[first:end])
			}
			written += merged
			sizes = append(append(append([]int64(nil), sizes[:first]...), merged), sizes[end:]...)
		}
		unsettled := 0
		for _, size := range sizes {
			if size < target/2 {
				unsettled++
			}
		}
		maxUnsettled = max(maxUnsettled, unsettled)
	}
	amplification := float64(written) / float64(appended)
	if amplification > 7 || maxUnsettled > 14 {
		t.Fatalf("rewrite factor %.2f, up to %d unsettled sections", amplification, maxUnsettled)
	}
	t.Logf("rewrite factor %.2f, up to %d unsettled sections", amplification, maxUnsettled)
}
