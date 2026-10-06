package engine

import "testing"

func sectionsOf(sizes ...int64) []localitySection {
	var out []localitySection
	for i, size := range sizes {
		out = append(out, localitySection{begin: i, end: i + 1, bytes: size})
	}
	return out
}

func TestLocalityMergeTiers(t *testing.T) {
	const target = 4 << 20
	cases := []struct {
		name       string
		sizes      []int64
		first, end int
	}{
		{"three small sections wait", []int64{40 << 10, 40 << 10, 40 << 10}, 0, 0},
		{"four of one tier merge", []int64{40 << 10, 41 << 10, 39 << 10, 40 << 10}, 0, 4},
		{"oldest run first", []int64{300 << 10, 40 << 10, 40 << 10, 40 << 10, 40 << 10}, 1, 5},
		{"full sections are never merged", []int64{target, target, target, target}, 0, 0},
		// Top tier: four sections of at least target/4 never fit, two do.
		{"top tier merges while it fits", []int64{1200 << 10, 1500 << 10, 1100 << 10}, 0, 3},
		{"settled sections do not pair beyond target", []int64{2500 << 10, 2600 << 10}, 0, 0},
	}
	for _, c := range cases {
		first, end := localityMerge(sectionsOf(c.sizes...), target, 4)
		if first != c.first || end != c.end {
			t.Errorf("%s: got [%d,%d), want [%d,%d)", c.name, first, end, c.first, c.end)
		}
	}
}

// Appending small sections and merging as selected keeps few sections:
// at most fanIn-1 per tier plus settled ones, and every byte is rewritten a
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
