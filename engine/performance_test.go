package engine

import (
	"context"
	"math"
	"testing"

	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
)

func benchmarkEngine(b *testing.B) (*Engine, *memoryStore) {
	b.Helper()
	s := newStore()
	e, err := Open(context.Background(), s, Options{WALDirectory: b.TempDir(), ObjectTarget: 128 << 20, BuilderHard: 256 << 20, WALTarget: 256 << 20, WALHigh: 512 << 20, WALHard: 768 << 20}, map[string]hta.Config{"x": {IntervalMin: 500000000, IntervalMax: 10000000000000000, IntervalFactor: 10}}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close() })
	return e, s
}

func benchmarkChunk(first, count int) *metricq.DataChunk {
	c := &metricq.DataChunk{TimeDelta: make([]int64, count), Value: make([]float64, count)}
	for i := range c.Value {
		c.TimeDelta[i] = 500000000
		c.Value[i] = math.Sin(float64(first+i) / 31)
	}
	c.TimeDelta[0] = 1700000000000000000 + int64(first)*500000000
	return c
}

// Includes a real WAL sync per 500-point delivery, but no broker or S3 network.
func BenchmarkIngest(b *testing.B) {
	e, _ := benchmarkEngine(b)
	c := benchmarkChunk(0, 500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e.NeedsFlush() {
			b.StopTimer()
			if err := e.Flush(context.Background()); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		c.TimeDelta[0] = 1700000000000000000 + int64(i)*500*500000000
		if err := e.Ingest(context.Background(), "x", c); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*500)/b.Elapsed().Seconds(), "points/s")
}

// Isolates encoding, index construction and publication for a fixed checkpoint.
func BenchmarkCheckpoint(b *testing.B) {
	b.ReportAllocs()
	var dataBytes, indexBytes int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e, s := benchmarkEngine(b)
		if err := e.Ingest(context.Background(), "x", benchmarkChunk(0, 100000)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := e.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for key, value := range s.objects {
			if len(key) >= 5 && key[:5] == "data/" {
				dataBytes += int64(len(value))
			} else if len(key) >= 6 && key[:6] == "index/" {
				indexBytes += int64(len(value))
			}
		}
	}
	b.ReportMetric(float64(dataBytes)/float64(b.N), "data-bytes/op")
	b.ReportMetric(float64(indexBytes)/float64(b.N), "index-bytes/op")
}
