package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"math"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
)

// This file is a throwaway investigation, not a regression test: it measures
// whether the number of physical objects a wide, coarse FLEX_TIMELINE query
// touches tracks flush *frequency* (many small flushes fragmenting a coarse
// level into many objects) or tracks total *historical volume* at that level
// (records / maxDataBlockRecords, which only compaction of sealed blocks can
// reduce). The answer decides whether "denser coarse-level packing" or
// "compact old sealed blocks" is the real next step.

func profileConfig() map[string]hta.Config {
	return map[string]hta.Config{"x": {IntervalMin: 1000, IntervalMax: 10_000_000, IntervalFactor: 10}}
}

// buildHistory ingests totalPoints points, evenly spaced so the coarsest
// level (1e6) accumulates well over maxDataBlockRecords entries, flushing
// every flushEveryCalls Ingest calls (0 means: only a single final flush).
func buildHistory(t *testing.T, flushEveryCalls, totalPoints, pointsPerCall int) (*countedRangeStore, *Engine) {
	t.Helper()
	s := &countedRangeStore{memoryStore: newStore()}
	e, err := Open(context.Background(), s, Options{
		WALDirectory:           t.TempDir(),
		CheckpointUnsavedBytes: 256 << 20, IngestMemoryLimitBytes: 512 << 20,
		WALTarget: 256 << 20, WALHigh: 512 << 20, WALHard: 768 << 20,
	}, profileConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	const spacing = 20_000 // < levelStep/40, so every coarse bucket gets several distinct points
	ts := int64(0)
	calls := 0
	for done := 0; done < totalPoints; {
		n := min(pointsPerCall, totalPoints-done)
		c := &metricq.DataChunk{TimeDelta: make([]int64, n), Value: make([]float64, n)}
		for i := 0; i < n; i++ {
			ts += spacing
			c.TimeDelta[i] = spacing
			c.Value[i] = math.Sin(float64(done+i) / 17)
		}
		c.TimeDelta[0] = ts - spacing*int64(n-1)
		if err := e.Ingest(context.Background(), "x", c); err != nil {
			t.Fatal(err)
		}
		done += n
		calls++
		if flushEveryCalls > 0 && calls%flushEveryCalls == 0 {
			if err := e.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, e
}

func objectStats(s *countedRangeStore) (dataObjects, indexObjects int, dataBytes, indexBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range s.objects {
		switch {
		case len(key) >= 5 && key[:5] == "data/":
			dataObjects++
			dataBytes += int64(len(value))
		case len(key) >= 6 && key[:6] == "index/":
			indexObjects++
			indexBytes += int64(len(value))
		}
	}
	return
}

// BenchmarkFlushMany drives the same "many small flushes" pattern under
// -cpuprofile to see whether gob/gzip encoding, the index tree rewrite, or
// something else dominates flush CPU time.
func BenchmarkFlushMany(b *testing.B) {
	b.ReportAllocs()
	cfg := profileConfig()
	for i := 0; i < b.N; i++ {
		s := newStore()
		e, err := Open(context.Background(), s, Options{
			WALDirectory:           b.TempDir(),
			CheckpointUnsavedBytes: 256 << 20, IngestMemoryLimitBytes: 512 << 20,
			WALTarget: 256 << 20, WALHigh: 512 << 20, WALHard: 768 << 20,
		}, cfg, nil)
		if err != nil {
			b.Fatal(err)
		}
		const spacing = 20_000
		const n = 200
		ts := int64(0)
		for call := 0; call < 300; call++ {
			c := &metricq.DataChunk{TimeDelta: make([]int64, n), Value: make([]float64, n)}
			for j := 0; j < n; j++ {
				ts += spacing
				c.TimeDelta[j] = spacing
				c.Value[j] = math.Sin(float64(call*n+j) / 17)
			}
			c.TimeDelta[0] = ts - spacing*int64(n-1)
			if err := e.Ingest(context.Background(), "x", c); err != nil {
				b.Fatal(err)
			}
			if err := e.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
		e.Close()
	}
}

// BenchmarkCompressionLevels is a standalone, non-invasive A/B measurement:
// it never touches engine.encode/gzipWriters, it just gob-encodes a
// representative growing tail block (as Flush's tail-extend path does,
// re-encoding the whole accumulated block on every call) through different
// gzip levels, to see the real CPU/size tradeoff before touching production
// code.
func BenchmarkCompressionLevels(b *testing.B) {
	records := make([]hta.Record, 0, maxDataBlockRecords)
	for i := 0; i < maxDataBlockRecords; i++ {
		records = append(records, hta.Record{
			Time: int64(i) * 1000, Level: 1000, Repeat: 1,
			Aggregate: hta.Value(math.Sin(float64(i)/17), 1000, 1),
		})
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(records); err != nil {
		b.Fatal(err)
	}
	gobBytes := buf.Len()

	for _, level := range []int{gzip.NoCompression, gzip.HuffmanOnly, gzip.BestSpeed, gzip.DefaultCompression, gzip.BestCompression} {
		b.Run(levelName(level), func(b *testing.B) {
			var size int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bytes.Buffer
				w, err := gzip.NewWriterLevel(&out, level)
				if err != nil {
					b.Fatal(err)
				}
				if err := gob.NewEncoder(w).Encode(records); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
				size = out.Len()
			}
			b.ReportMetric(float64(gobBytes), "gob-bytes")
			b.ReportMetric(float64(size), "gz-bytes")
		})
	}
}

func levelName(l int) string {
	switch l {
	case gzip.NoCompression:
		return "NoCompression"
	case gzip.HuffmanOnly:
		return "HuffmanOnly"
	case gzip.BestSpeed:
		return "BestSpeed"
	case gzip.DefaultCompression:
		return "DefaultCompression"
	case gzip.BestCompression:
		return "BestCompression"
	default:
		return "Unknown"
	}
}

func TestProfileFlushFrequencyVsBlockCount(t *testing.T) {
	// 60000 points * 20000 spacing = 1.2e9 time units covered, i.e. > 1170
	// buckets at the coarsest level (1e6) -> that level alone needs at least
	// 2 sealed data blocks (maxDataBlockRecords=1024) no matter how packing
	// works.
	const totalPoints = 60_000
	const pointsPerCall = 200 // 300 Ingest calls total

	for _, tc := range []struct {
		name       string
		flushEvery int // in Ingest calls; 0 = single final flush
	}{
		{"OneBigFlush", 0},
		{"FlushEvery30Calls", 30}, // 10 flushes
		{"FlushEveryCall", 1},     // 300 flushes
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, e := buildHistory(t, tc.flushEvery, totalPoints, pointsPerCall)
			dataObjects, indexObjects, dataBytes, indexBytes := objectStats(s)

			s.ranges.Store(0)
			s.dataRanges.Store(0)
			s.indexRanges.Store(0)
			s.dataBytes.Store(0)
			req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: int64(totalPoints) * 20_000, IntervalMax: int64(totalPoints) * 20_000 / 100}
			resp, err := e.Query(context.Background(), "x", req)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: %d ingest/flush objects written (data=%d objs/%d bytes, index=%d objs/%d bytes)",
				tc.name, dataObjects+indexObjects, dataObjects, dataBytes, indexObjects, indexBytes)
			t.Logf("%s: full-history FLEX query (%d display points): %d total GETs, %d data GETs/%d bytes, %d index GETs",
				tc.name, len(resp.TimeDelta), s.ranges.Load(), s.dataRanges.Load(), s.dataBytes.Load(), s.indexRanges.Load())
		})
	}
}
