//go:build review

package engine

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

type metadataTraffic struct{ Calls, Bytes int64 }
type metadataWorkloadStore struct {
	*rangeGCStore
	trafficMu sync.Mutex
	traffic   map[string]metadataTraffic
}

func (s *metadataWorkloadStore) count(op, key string, n int64) {
	kind := strings.SplitN(key, "/", 2)[0]
	s.trafficMu.Lock()
	defer s.trafficMu.Unlock()
	k := op + "/" + kind
	v := s.traffic[k]
	v.Calls++
	v.Bytes += n
	s.traffic[k] = v
}
func (s *metadataWorkloadStore) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	s.count("put", key, int64(len(b)))
	return s.rangeGCStore.Put(ctx, key, b, expected)
}
func (s *metadataWorkloadStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	b, v, err := s.rangeGCStore.Get(ctx, key)
	s.count("get", key, int64(len(b)))
	return b, v, err
}
func (s *metadataWorkloadStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	b, err := s.rangeGCStore.GetRange(ctx, key, offset, length)
	s.count("range", key, int64(len(b)))
	return b, err
}
func (s *metadataWorkloadStore) Delete(ctx context.Context, key string) error {
	s.count("delete", key, 0)
	return s.rangeGCStore.Delete(ctx, key)
}

// Opt-in sustained engine/WAL workload. Time advances virtually; data, fsync,
// compression, metadata, compaction and recovery are real. The object backend
// is memory with zero network latency; results are not S3 throughput claims.
func TestReviewMetadataHourlyWorkload(t *testing.T) {
	if os.Getenv("METRICQ_METADATA_REVIEW") != "1" {
		t.Skip("set METRICQ_METADATA_REVIEW=1")
	}
	hours := 2
	if v := os.Getenv("METRICQ_METADATA_HOURS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatal("invalid hours")
		}
		hours = n
	}
	cases := []string{"dense", "mixed"}
	if v := os.Getenv("METRICQ_METADATA_CASE"); v != "" {
		cases = []string{v}
	}
	for _, workload := range cases {
		t.Run(workload, func(t *testing.T) {
			ctx := context.Background()
			store := &metadataWorkloadStore{rangeGCStore: &rangeGCStore{gcStore: &gcStore{memoryStore: newStore()}}, traffic: map[string]metadataTraffic{}}
			opts := maintenanceOptions(t.TempDir(), true)
			opts.CheckpointAppendOnlyAggregates = true
			opts.HoldMaxAgeSeconds = 3600
			opts.HoldMemoryBytes = 256 << 20
			opts.IngestMemoryLimitBytes = 512 << 20
			opts.WALTarget = 32 << 20
			opts.WALHigh = 256 << 20
			opts.WALHard = 512 << 20
			opts.CompactionOptions.MergeCooldownSeconds = 0
			opts.CompactionOptions.IOBytesPerSecond = 1 << 40
			cfg := map[string]hta.Config{}
			periods := make([]int64, 1500)
			next := make([]int64, 1500)
			const base = int64(1700000000) * int64(time.Second)
			for i := range periods {
				period := int64(time.Second)
				if workload == "mixed" {
					switch {
					case i < 100:
						period = int64(time.Second) / 10
					case i < 1000:
					case i < 1300:
						period = 10 * int64(time.Second)
					case i < 1450:
						period = int64(time.Minute)
					case i < 1490:
						period = int64(time.Hour)
					default:
						period = 24 * int64(time.Hour)
					}
				} else if workload != "dense" {
					t.Fatal("unknown workload")
				}
				periods[i] = period
				next[i] = base
				if workload == "mixed" {
					next[i] += int64(i%30) * int64(time.Second)
				}
				cfg[fmt.Sprintf("canonical.metric.%04d", i)] = hta.Config{IntervalMin: int64(time.Second), IntervalMax: 10000000 * int64(time.Second), IntervalFactor: 10}
			}
			registry := prometheus.NewRegistry()
			e, err := Open(ctx, store, opts, cfg, NewMetrics(registry))
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
			defer server.Close()
			var scrapes atomic.Int64
			var scrapeFailures atomic.Int64
			stop := make(chan struct{})
			done := make(chan struct{})
			scrape := func() {
				r, err := http.Get(server.URL)
				if err != nil {
					scrapeFailures.Add(1)
					return
				}
				b, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil || r.StatusCode != 200 || !strings.Contains(string(b), "metricq_db_wal_") {
					scrapeFailures.Add(1)
				}
				scrapes.Add(1)
			}
			go func() {
				defer close(done)
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						scrape()
					}
				}
			}()
			defer func() { close(stop); <-done }()
			clock := time.Unix(10000, 0)
			e.now = func() time.Time { return clock }
			var ackTimes, flushTimes []time.Duration
			var samples int64
			began := time.Now()
			for seconds := 30; seconds <= hours*3600; seconds += 30 {
				clock = time.Unix(10000+int64(seconds), 0)
				end := base + int64(seconds)*int64(time.Second)
				deliveries := make([]Delivery, 0, 1500)
				for i, period := range periods {
					c := &metricq.DataChunk{}
					previous := int64(0)
					for next[i] < end {
						timestamp := next[i]
						c.TimeDelta = append(c.TimeDelta, timestamp-previous)
						c.Value = append(c.Value, math.Sin(float64(timestamp-base)/1e11)+float64(i)/1500)
						previous = timestamp
						next[i] += period
						samples++
					}
					if len(c.Value) > 0 {
						deliveries = append(deliveries, Delivery{Metric: fmt.Sprintf("canonical.metric.%04d", i), Chunk: c})
					}
				}
				start := time.Now()
				n, err := e.IngestBatch(ctx, deliveries)
				if err != nil || n != len(deliveries) {
					t.Fatalf("ingest tick %d: %d/%d %v", seconds, n, len(deliveries), err)
				}
				ackTimes = append(ackTimes, time.Since(start))
				start = time.Now()
				if err = e.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				flushTimes = append(flushTimes, time.Since(start))
				if os.Getenv("METRICQ_METADATA_MAINTENANCE") == "1" && seconds%600 == 0 {
					if err = e.CompactOnce(ctx); err != nil {
						t.Fatal(err)
					}
					if err = e.Reclaim(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if seconds%3600 == 0 {
					t.Logf("hour=%d samples=%d held=%d candidates=%d wall=%s", seconds/3600, samples, len(e.state.Held), e.state.CandidateObjects, time.Since(began))
				}
			}
			scrape()
			if scrapeFailures.Load() != 0 {
				t.Fatalf("prometheus scrape failures=%d", scrapeFailures.Load())
			}
			percentiles := func(values []time.Duration) (time.Duration, time.Duration) {
				sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
				return values[len(values)/2], values[(len(values)-1)*95/100]
			}
			a50, a95 := percentiles(ackTimes)
			f50, f95 := percentiles(flushTimes)
			t.Logf("samples=%d hours=%d ACK_batch_p50=%s ACK_batch_p95=%s flush_p50=%s flush_p95=%s wall=%s scrapes=%d", samples, hours, a50, a95, f50, f95, time.Since(began), scrapes.Load())
			keys := []string{}
			for k := range store.traffic {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := store.traffic[k]
				t.Logf("traffic %s calls=%d bytes=%d", k, v.Calls, v.Bytes)
			}
			stored := map[string]int64{}
			objects := 0
			store.mu.Lock()
			for key, b := range store.objects {
				stored[strings.SplitN(key, "/", 2)[0]] += int64(len(b))
				objects++
			}
			store.mu.Unlock()
			keys = nil
			for k := range stored {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				t.Logf("retained %s bytes=%d", k, stored[k])
			}
			t.Logf("retained objects=%d", objects)
			requests := []*metricq.HistoryRequest{}
			for _, interval := range []int64{0, 10, 3600} {
				requests = append(requests, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: base, EndTime: base + int64(hours*3600)*int64(time.Second), IntervalMax: interval * int64(time.Second)})
			}
			names := []string{"canonical.metric.0000", "canonical.metric.0999", "canonical.metric.1299", "canonical.metric.1499"}
			responses := []*metricq.HistoryResponse{}
			for _, name := range names {
				for _, req := range requests {
					r, err := e.Query(ctx, name, req)
					if err != nil {
						t.Fatal(err)
					}
					responses = append(responses, r)
				}
			}
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			opts.WALDirectory = t.TempDir()
			beforeCalls, beforeBytes := int64(0), int64(0)
			for k, v := range store.traffic {
				if strings.HasPrefix(k, "get/") || strings.HasPrefix(k, "range/") {
					beforeCalls += v.Calls
					beforeBytes += v.Bytes
				}
			}
			start := time.Now()
			recovered, err := Open(ctx, store, opts, cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			calls, bytes := int64(0), int64(0)
			for k, v := range store.traffic {
				if strings.HasPrefix(k, "get/") || strings.HasPrefix(k, "range/") {
					calls += v.Calls
					bytes += v.Bytes
				}
			}
			t.Logf("S3_only_recovery time=%s calls=%d bytes=%d", time.Since(start), calls-beforeCalls, bytes-beforeBytes)
			index := 0
			for _, name := range names {
				for _, req := range requests {
					r, err := recovered.Query(ctx, name, req)
					if err != nil || !proto.Equal(r, responses[index]) {
						t.Fatalf("recovery response %s/%d: %v", name, req.IntervalMax, err)
					}
					index++
				}
			}
		})
	}
}
