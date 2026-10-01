//go:build review

package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

// Synthetic mature manifest: eight aggregate levels plus raw roots per metric.
// Hashes vary per root; repeated zero hashes would unrealistically compress well.
func capacityManifest(n int) manifest {
	m := manifest{Version: 2, Series: map[string]*hta.Series{}, Roots: map[string]map[int64]blob{}}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("canonical.metric.%05d", i)
		s := hta.New(hta.Config{IntervalMin: int64(time.Second), IntervalMax: int64(10000000) * int64(time.Second), IntervalFactor: 10})
		s.First = hta.Point{Time: 1700000000000000000, Value: float64(i)}
		s.Last = hta.Point{Time: 1800000000000000000, Value: float64(i) + .5}
		m.Series[name] = s
		m.Roots[name] = map[int64]blob{}
		for level := int64(0); ; {
			hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", name, level)))
			m.Roots[name][level] = blob{Key: fmt.Sprintf("index/%x", hash[:16]), Hash: hash, Offset: int64(i) * 2048, Length: 2048}
			if level > 0 {
				s.Levels[level] = hta.Level{Time: s.Last.Time - s.Last.Time%level, Aggregate: hta.Value(float64(i)+.5, level, 3)}
			}
			if level == s.Config.IntervalMax {
				break
			}
			if level == 0 {
				level = s.Config.IntervalMin
			} else {
				level *= 10
			}
		}
	}
	return m
}

func BenchmarkReviewManifest(b *testing.B) {
	for _, n := range []int{150, 1500, 15000} {
		m := capacityManifest(n)
		b.Run(fmt.Sprintf("metrics=%d", n), func(b *testing.B) {
			b.Run("clone", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					c := cloneManifest(m)
					if len(c.Series) != n {
						b.Fatal("clone")
					}
				}
			})
			b.Run("encode", func(b *testing.B) {
				b.ReportAllocs()
				var size int
				for i := 0; i < b.N; i++ {
					data, err := encode(m)
					if err != nil {
						b.Fatal(err)
					}
					size = len(data)
				}
				b.ReportMetric(float64(size), "manifest-B")
			})
		})
	}
}

type capacityReadStore struct {
	*rangeGCStore
	data     capacityReadCounter
	index    capacityReadCounter
	metadata capacityReadCounter
}

type capacityReadCounter struct {
	gets      atomic.Int64
	rangeGets atomic.Int64
	bytes     atomic.Int64
}

type capacityReadSample struct {
	gets, rangeGets, bytes int64
}

func (c *capacityReadCounter) reset() {
	c.gets.Store(0)
	c.rangeGets.Store(0)
	c.bytes.Store(0)
}

func (c *capacityReadCounter) sample() capacityReadSample {
	return capacityReadSample{c.gets.Load(), c.rangeGets.Load(), c.bytes.Load()}
}

func (s *capacityReadStore) counter(key string) *capacityReadCounter {
	switch {
	case strings.HasPrefix(key, "data/"):
		return &s.data
	case strings.HasPrefix(key, "index/"):
		return &s.index
	default:
		return &s.metadata
	}
}

func (s *capacityReadStore) resetReads() {
	s.data.reset()
	s.index.reset()
	s.metadata.reset()
}

func (s *capacityReadStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, etag, err := s.rangeGCStore.Get(ctx, key)
	c := s.counter(key)
	c.gets.Add(1)
	if err == nil {
		c.bytes.Add(int64(len(b)))
	}
	return b, etag, err
}

func (s *capacityReadStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b, err := s.rangeGCStore.GetRange(ctx, key, offset, length)
	c := s.counter(key)
	c.rangeGets.Add(1)
	if err == nil {
		c.bytes.Add(int64(len(b)))
	}
	return b, err
}

// Same logical history, full blocks either spread over checkpoints or contiguous.
// No simulated network delay: count all store GETs and record CPU/store time.
func TestReviewFullBlockQueryLocality(t *testing.T) {
	ctx := context.Background()
	var reference *metricq.HistoryResponse
	for _, scenario := range []struct{ blocks, flushes int }{
		{16, 16}, {16, 1},
		{80, 80}, {80, 1}, // More than one 64-entry index leaf.
	} {
		t.Run(fmt.Sprintf("blocks=%d/flushes=%d", scenario.blocks, scenario.flushes), func(t *testing.T) {
			s := &capacityReadStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}}
			opts := maintenanceOptions(t.TempDir(), true)
			opts.CheckpointAppendOnlyAggregates = true
			opts.HoldMaxAgeSeconds = 3600
			opts.IngestMemoryLimitBytes = 128 << 20
			opts.HoldMemoryBytes = 64 << 20
			opts.CompactionOptions.JobMaxBlocks = 512
			opts.CompactionOptions.OutputObjectBytes = 4 << 20
			e, err := Open(ctx, s, opts, map[string]hta.Config{"x": {IntervalMin: int64(time.Second), IntervalMax: int64(100000 * time.Second), IntervalFactor: 10}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			total := scenario.blocks * maxDataBlockRecords
			const base = int64(1700000000) * int64(time.Second)
			for start := 0; start < total; start += total / scenario.flushes {
				c := &metricq.DataChunk{}
				for j := start; j < start+total/scenario.flushes; j++ {
					d := int64(time.Second)
					if j == start {
						d = base + int64(j)*int64(time.Second)
					}
					c.TimeDelta = append(c.TimeDelta, d)
					c.Value = append(c.Value, math.Sin(float64(j)/31))
				}
				if err = e.Ingest(ctx, "x", c); err != nil {
					t.Fatal(err)
				}
				if err = e.Flush(ctx); err != nil {
					t.Fatal(err)
				}
			}
			// 1000 display positions: the chosen 1-second level supplies 9000
			// buckets, combined nine at a time by FLEX_TIMELINE.
			req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: base, EndTime: base + 9000*int64(time.Second), IntervalMax: 9 * int64(time.Second)}
			measure := func(label string) {
				var times []time.Duration
				var data, index, metadata capacityReadSample
				for i := 0; i < 10; i++ {
					e.sharedNodes = newIndexPageCache()
					e.sharedCatalog = newCatalogPageCache()
					e.sharedBlocks = newDataBlockCache()
					s.resetReads()
					start := time.Now()
					r, err := e.Query(ctx, "x", req)
					elapsed := time.Since(start)
					if err != nil {
						t.Fatal(err)
					}
					if len(r.Aggregate) != 1000 || len(r.TimeDelta) != 1000 {
						t.Fatalf("expected 1000 aggregate display positions, got %d", len(r.TimeDelta))
					}
					if reference == nil {
						reference = r
					} else if !proto.Equal(reference, r) {
						t.Fatal("different response")
					}
					times = append(times, elapsed)
					d, x, m := s.data.sample(), s.index.sample(), s.metadata.sample()
					if want := 1 + scenario.blocks/indexFanout; x.rangeGets < int64(want) {
						t.Fatalf("cold %d-block query used only %d index GETs; expected at least %d levels", scenario.blocks, x.rangeGets, want)
					}
					data.gets += d.gets
					data.rangeGets += d.rangeGets
					data.bytes += d.bytes
					index.gets += x.gets
					index.rangeGets += x.rangeGets
					index.bytes += x.bytes
					metadata.gets += m.gets
					metadata.rangeGets += m.rangeGets
					metadata.bytes += m.bytes
				}
				sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
				t.Logf("%s cold FLEX: median=%s p95=%s total_GETs=%.1f data_GETs=%.1f index_GETs=%.1f metadata_GETs=%.1f total_bytes=%.0f data_bytes=%.0f index_bytes=%.0f metadata_bytes=%.0f full_GETs=%.1f range_GETs=%.1f rows=%d candidates=%d",
					label, times[5], times[9], float64(data.gets+data.rangeGets+index.gets+index.rangeGets+metadata.gets+metadata.rangeGets)/10,
					float64(data.gets+data.rangeGets)/10, float64(index.gets+index.rangeGets)/10, float64(metadata.gets+metadata.rangeGets)/10,
					float64(data.bytes+index.bytes+metadata.bytes)/10, float64(data.bytes)/10, float64(index.bytes)/10, float64(metadata.bytes)/10,
					float64(data.gets+index.gets+metadata.gets)/10, float64(data.rangeGets+index.rangeGets+metadata.rangeGets)/10,
					len(reference.TimeDelta), e.state.CandidateObjects)
			}
			measure("before")
			before := e.compactionCompletions
			for i := 0; i < 32; i++ {
				if err = e.CompactOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("completed compactions=%d", e.compactionCompletions-before)
			measure("after")
			if scenario.flushes > 1 {
				e.sharedNodes = newIndexPageCache()
				e.sharedCatalog = newCatalogPageCache()
				e.sharedBlocks = newDataBlockCache()
				s.resetReads()
				if _, err := e.Query(ctx, "x", req); err != nil {
					t.Fatal(err)
				}
				if got := s.data.sample().rangeGets; got != 1 {
					t.Fatalf("locality compaction retains %d data GETs", got)
				}
			}
		})
	}
}

type capacityWriteStore struct {
	*rangeGCStore
	manifestPuts  atomic.Int64
	manifestBytes atomic.Int64
}

func (s *capacityWriteStore) Put(ctx context.Context, key string, data []byte, expected *string) (string, error) {
	if key == "manifest" {
		s.manifestPuts.Add(1)
		s.manifestBytes.Add(int64(len(data)))
	}
	return s.rangeGCStore.Put(ctx, key, data, expected)
}

// Isolate staggered expiry of already durable records, without new WAL input.
// The second case is a proposed scheduling policy, not current production code.
func TestReviewHoldExpiryPublications(t *testing.T) {
	for _, every := range []int{1, 30} {
		t.Run(fmt.Sprintf("check_every_%ds", every), func(t *testing.T) {
			ctx := context.Background()
			s := &capacityWriteStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}}
			opts := maintenanceOptions(t.TempDir(), true)
			opts.HoldMaxAgeSeconds = 3600
			opts.CheckpointAppendOnlyAggregates = true
			cfg := map[string]hta.Config{}
			var deliveries []Delivery
			for i := 0; i < 1500; i++ {
				name := fmt.Sprintf("canonical.metric.%05d", i)
				cfg[name] = hta.Config{IntervalMin: int64(time.Second), IntervalMax: 10000000 * int64(time.Second), IntervalFactor: 10}
				deliveries = append(deliveries, Delivery{Metric: name, Chunk: chunk(hta.Point{Time: 1700000000000000000, Value: float64(i)}, hta.Point{Time: 1700000001000000000, Value: float64(i) + .5})})
			}
			e, err := Open(ctx, s, opts, cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			now := time.Unix(10000, 0)
			e.now = func() time.Time { return now }
			if n, err := e.IngestBatch(ctx, deliveries); err != nil || n != len(deliveries) {
				t.Fatalf("ingest %d %v", n, err)
			}
			if err := e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			// 120 metrics reach their hold deadline one second apart. Other
			// metrics remain held. Only arrival ages change, not data contents.
			for i := 0; i < 120; i++ {
				name := fmt.Sprintf("canonical.metric.%05d", i)
				for level := range e.pending.streams[name] {
					e.held[streamKey(name, level)].since = now.Add(-time.Hour + time.Duration(i)*time.Second)
				}
			}
			s.manifestPuts.Store(0)
			s.manifestBytes.Store(0)
			began := time.Now()
			for tick := every; tick <= 120; tick += every {
				now = time.Unix(10000+int64(tick), 0)
				if e.NeedsFlush() {
					if err := e.Flush(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			t.Logf("expiry-only: manifest_PUTs=%d manifest_bytes=%d wall=%s remaining_held_records=%d", s.manifestPuts.Load(), s.manifestBytes.Load(), time.Since(began), e.pending.len())
			for i := 0; i < 120; i++ {
				name := fmt.Sprintf("canonical.metric.%05d", i)
				if len(streamBlocks(t, e, name, 0)) != 1 {
					t.Fatal("expired raw stream not written")
				}
				r, err := e.Query(ctx, name, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 1700000000000000000, EndTime: 1700000002000000000})
				if err != nil || len(r.Value) != 2 || r.Value[0] != float64(i) || r.Value[1] != float64(i)+.5 {
					t.Fatalf("response %v %v", r, err)
				}
			}
		})
	}
}

// Include immutable-page uploads in the maintenance cost, rather than timing
// only the now-small final CAS object. The backend has no network latency.
func BenchmarkReviewPagedMaintenance(b *testing.B) {
	for _, n := range []int{150, 1500, 15000} {
		b.Run(fmt.Sprintf("metrics=%d", n), func(b *testing.B) {
			ctx := context.Background()
			store := newStore()
			opened, err := Open(ctx, store, Options{WALDirectory: b.TempDir()}, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer opened.Close()
			opened.options.MaintenanceEnabled = true
			base := capacityManifest(n)
			if _, err = opened.encodeManifest(ctx, &base, manifest{}); err != nil {
				b.Fatal(err)
			}
			name := "canonical.metric.00000"
			b.ReportAllocs()
			b.ResetTimer()
			var manifestSize int
			var putBytes int64
			for i := 0; i < b.N; i++ {
				next := cloneMaintenanceManifest(base)
				next.Generation++
				copied := make(map[int64]blob, len(next.Roots[name]))
				for level, ref := range next.Roots[name] {
					copied[level] = ref
				}
				next.Roots[name] = copied
				ref := next.Roots[name][0]
				ref.Hash = sha256.Sum256([]byte(fmt.Sprintf("root-%d", i)))
				next.Roots[name][0] = ref
				oldKeys := make(map[string]bool, len(store.objects))
				for key := range store.objects {
					oldKeys[key] = true
				}
				encoded, err := opened.encodeManifest(ctx, &next, base)
				if err != nil {
					b.Fatal(err)
				}
				manifestSize = len(encoded)
				for key, bytes := range store.objects {
					if !oldKeys[key] {
						putBytes += int64(len(bytes))
					}
				}
				base = next
			}
			b.ReportMetric(float64(manifestSize), "manifest-B")
			b.ReportMetric(float64(putBytes)/float64(b.N), "metadata-PUT-B/op")
		})
	}
}
