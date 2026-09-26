//go:build integration

package integration

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/metricq/metricq-db-hta-go/engine"
	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
)

func TestCompactionS3(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	var writer *csv.Writer
	if path := os.Getenv("METRICQ_COMPACTION_OUTPUT"); path != "" {
		f, err := os.Create(benchmarkOutputPath(t, "METRICQ_COMPACTION_OUTPUT"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		writer = csv.NewWriter(f)
		defer writer.Flush()
		writer.Write([]string{"metrics", "samples", "data_index_bytes_before_gc", "data_index_bytes_before", "data_index_bytes_after", "raw_blocks_before", "raw_blocks_after", "compaction_and_gc_s", "response_comparisons"})
	}
	var counts []int
	for _, text := range strings.Split(env("METRICQ_COMPACTION_METRICS", "6,150"), ",") {
		n, err := strconv.Atoi(strings.TrimSpace(text))
		if err != nil || n < 1 {
			t.Fatalf("invalid compaction metric count %q", text)
		}
		counts = append(counts, n)
	}
	for _, count := range counts {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			id := fmt.Sprintf("hta-compact-%d-%d", time.Now().UnixNano(), count)
			backend, client := newS3(t, ctx, id)
			configs := make(map[string]hta.Config)
			name := func(i int) string { return fmt.Sprintf("canonical.%05d", i) }
			for i := 0; i < count; i++ {
				configs[name(i)] = hta.Config{IntervalMin: int64(time.Second), IntervalMax: int64(1000 * time.Second), IntervalFactor: 10}
			}
			options := engine.Options{WALDirectory: t.TempDir(), ObjectTarget: 4 << 20, BuilderHard: 32 << 20, BackgroundMaintenance: true, Compaction: engine.CompactionOptions{Enabled: true, MaxBlocks: 512, DeadFraction: .05, BytesPerSecond: 64 << 20, MergeSmallBlocks: true}}
			e, err := engine.Open(ctx, backend, options, configs, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			base := int64(1700000000) * int64(time.Second)
			for batch := 0; batch < 8; batch++ {
				for metric := 0; metric < count; metric++ {
					c := &metricq.DataChunk{}
					for j := 0; j < 16; j++ {
						stamp := base + int64(batch*16+j)*int64(time.Second)
						delta := int64(time.Second)
						if j == 0 {
							delta = stamp
						}
						c.TimeDelta = append(c.TimeDelta, delta)
						c.Value = append(c.Value, float64(metric+j%7))
					}
					if err := e.Ingest(ctx, name(metric), c); err != nil {
						t.Fatal(err)
					}
				}
				if err := e.Flush(ctx); err != nil {
					t.Fatal(err)
				}
			}
			physical := func() int64 {
				var total int64
				for _, prefix := range []string{"data/", "index/"} {
					token := ""
					for {
						input := &s3.ListObjectsV2Input{Bucket: aws.String(id), Prefix: aws.String(prefix)}
						if token != "" {
							input.ContinuationToken = &token
						}
						out, err := client.ListObjectsV2(ctx, input)
						if err != nil {
							t.Fatal(err)
						}
						for _, o := range out.Contents {
							total += aws.ToInt64(o.Size)
						}
						token = aws.ToString(out.NextContinuationToken)
						if token == "" {
							break
						}
					}
				}
				return total
			}
			requests := []*metricq.HistoryRequest{
				{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: base, EndTime: base + 127*int64(time.Second)},
				{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: base + 123, EndTime: base + 126*int64(time.Second), IntervalMax: int64(time.Second)},
				{Type: metricq.HistoryRequest_AGGREGATE, StartTime: base + 123, EndTime: base + 126*int64(time.Second)},
				{Type: metricq.HistoryRequest_LAST_VALUE},
			}
			expected := make(map[string][]*metricq.HistoryResponse)
			for i := 0; i < min(count, 6); i++ {
				for _, r := range requests {
					resp, err := e.Query(ctx, name(i), r)
					if err != nil {
						t.Fatal(err)
					}
					expected[name(i)] = append(expected[name(i)], resp)
				}
			}
			beforeGC := physical()
			for i := 0; e.MaintenanceStatus().PendingObjects > 0 && i < 2000; i++ {
				if err := e.Reclaim(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if e.MaintenanceStatus().PendingObjects != 0 {
				t.Fatal("baseline GC backlog remained")
			}
			before := physical()
			layoutBefore, err := auditLayout(ctx, backend)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			passes := 12
			if count >= 1500 {
				passes = 64
			}
			for i := 0; i < passes; i++ {
				if err := e.CompactOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; e.MaintenanceStatus().PendingObjects > 0 && i < 2000; i++ {
				if err := e.Reclaim(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if e.MaintenanceStatus().PendingObjects != 0 {
				t.Fatal("GC backlog remained")
			}
			elapsed := time.Since(started).Seconds()
			after := physical()
			layoutAfter, err := auditLayout(ctx, backend)
			if err != nil {
				t.Fatal(err)
			}
			if after >= before || layoutAfter.rawBlocks >= layoutBefore.rawBlocks {
				t.Fatalf("no reclamation/consolidation: bytes %d -> %d blocks %d -> %d", before, after, layoutBefore.rawBlocks, layoutAfter.rawBlocks)
			}
			if layoutAfter.rawRecords != int64(count*128) {
				t.Fatal("raw records lost")
			}
			e.Close()
			options.WALDirectory = t.TempDir()
			recovered, err := engine.Open(ctx, backend, options, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			comparisons := 0
			for metric, wants := range expected {
				for i, r := range requests {
					got, err := recovered.Query(ctx, metric, r)
					if err != nil {
						t.Fatal(err)
					}
					if err := compare(wants[i], got); err != nil {
						t.Fatal(err)
					}
					comparisons++
				}
			}
			if writer != nil {
				writer.Write([]string{fmt.Sprint(count), fmt.Sprint(count * 128), fmt.Sprint(beforeGC), fmt.Sprint(before), fmt.Sprint(after), fmt.Sprint(layoutBefore.rawBlocks), fmt.Sprint(layoutAfter.rawBlocks), fmt.Sprint(elapsed), fmt.Sprint(comparisons)})
				writer.Flush()
				if err := writer.Error(); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("data/index bytes %d -> %d; raw blocks %d -> %d; %.2fs; %d response comparisons", before, after, layoutBefore.rawBlocks, layoutAfter.rawBlocks, elapsed, comparisons)
		})
	}
}
