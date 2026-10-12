//go:build review

package engine

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// Read-only report of every stream's sections in a live bucket: how many,
// how large, and whether the tier rule has merges due that compaction has not
// made yet.
//
//	METRICQ_LIVE_S3_ENDPOINT=... go test -tags review ./engine -run TestReviewLiveStreamSections -v
func TestReviewLiveStreamSections(t *testing.T) {
	endpoint := os.Getenv("METRICQ_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set METRICQ_LIVE_S3_ENDPOINT")
	}
	ctx := context.Background()
	s, err := storage.NewS3(ctx, storage.S3Config{Bucket: os.Getenv("METRICQ_LIVE_S3_BUCKET"), Prefix: os.Getenv("METRICQ_LIVE_S3_PREFIX"), Endpoint: endpoint, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	snap, _, err := auditSnapshotRetry(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{store: s, metrics: NewMetrics(prometheus.NewRegistry()), state: snap.m}
	const target, fanIn = 4 << 20, 4
	type row struct {
		name              string
		entries, sections int
		due, dueBlocks    int
		open              int
	}
	var rows []row
	hist := map[int]int{}
	raced := 0
	for metric, levels := range snap.m.Roots {
		for level, root := range levels {
			if root.Key == "" {
				continue
			}
			e.nodeCache = make(map[blob]indexNode)
			entries, err := e.indexEntriesAfter(ctx, root, math.MinInt64, 1<<30)
			if err != nil {
				// Replaced by a publication during the walk.
				raced++
				continue
			}
			open, _ := openSuffix(entries)
			plan := entries[:len(entries)-open]
			sections := localitySections(plan)
			runs := mergeCascade(sections, target, fanIn)
			r := row{name: fmt.Sprintf("%s/%d", metric, level), entries: len(entries), sections: len(sections) + min(open, 1), open: open, due: len(runs)}
			for _, run := range runs {
				r.dueBlocks += sections[run[1]-1].end - sections[run[0]].begin
			}
			rows = append(rows, r)
			b := r.sections
			if b > 20 {
				b = 20
			}
			hist[b]++
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].sections > rows[j].sections })
	total, withDue, dueBlocks := 0, 0, 0
	for _, r := range rows {
		total += r.sections
		if r.due > 0 {
			withDue++
			dueBlocks += r.dueBlocks
		}
	}
	fmt.Printf("%d streams skipped (replaced during the walk)\n", raced)
	fmt.Printf("%d streams, %d sections (%.2f per stream); %d streams with merges due (%d blocks)\n", len(rows), total, float64(total)/float64(len(rows)), withDue, dueBlocks)
	var ks []int
	for k := range hist {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	for _, k := range ks {
		fmt.Printf("  %2d sections: %d streams\n", k, hist[k])
	}
	for _, r := range rows[:min(10, len(rows))] {
		fmt.Printf("  %-40s entries=%d sections=%d open=%d due runs=%d blocks=%d\n", r.name, r.entries, r.sections, r.open, r.due, r.dueBlocks)
	}
}
