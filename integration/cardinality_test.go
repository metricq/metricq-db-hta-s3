//go:build integration

package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/engine"
	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

// Separate from the AMQP/legacy latency workload: this measures cardinality
// in the Go storage engine with a real S3 backend and fixed memory thresholds.
func cardinalities(t *testing.T) []int {
	t.Helper()
	var out []int
	for _, value := range strings.Split(env("METRICQ_CARDINALITY_METRICS", "6,150,1500"), ",") {
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || n < 1 {
			t.Fatalf("invalid metric count %q", value)
		}
		out = append(out, n)
	}
	return out
}

type cardinalityStore struct {
	*measuredStore
	dataPutBytes, indexPutBytes, manifestPutBytes, puts atomic.Int64
}

func (s *cardinalityStore) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	version, err := s.Store.Put(ctx, key, b, expected)
	if err == nil {
		s.puts.Add(1)
		switch {
		case strings.HasPrefix(key, "data/"):
			s.dataPutBytes.Add(int64(len(b)))
		case strings.HasPrefix(key, "index/"):
			s.indexPutBytes.Add(int64(len(b)))
		case key == "manifest":
			s.manifestPutBytes.Add(int64(len(b)))
		}
	}
	return version, err
}

// Gob matches exported fields by name. These audit-only types read published
// v2 references without adding a production API or decoding sample blocks.
type auditBlob struct {
	Key            string
	Offset, Length int64
	Hash           [32]byte
}
type auditEdge struct {
	First, Last int64
	Blob        auditBlob
	Records     int
}
type auditNode struct {
	Leaf    bool
	Entries []auditEdge
}
type auditManifest struct {
	Roots       map[string]map[int64]auditBlob
	StreamIndex auditBlob
}
type layoutStats struct{ rawBlocks, rawRecords, aggregateBlocks, aggregateRecords, fragmentedAggregates, indexNodes, liveDataBytes, liveIndexBytes int64 }

func auditDecode(b []byte, v any) error {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer r.Close()
	return gob.NewDecoder(r).Decode(v)
}
func auditLayout(ctx context.Context, s storage.Store) (layoutStats, error) {
	var stats layoutStats
	b, _, err := s.Get(ctx, "manifest")
	if err != nil {
		return stats, err
	}
	var manifest auditManifest
	if err = auditDecode(b, &manifest); err != nil {
		return stats, err
	}
	ranges := s.(storage.RangeGetter)
	if manifest.StreamIndex.Key != "" {
		load := func(ref auditBlob, v any) error {
			b, err := ranges.GetRange(ctx, ref.Key, ref.Offset, ref.Length)
			if err != nil {
				return err
			}
			if int64(len(b)) != ref.Length || sha256.Sum256(b) != ref.Hash {
				return fmt.Errorf("invalid audit metadata checksum")
			}
			return auditDecode(b, v)
		}
		var directory struct{ Pages [256]auditBlob }
		if err := load(manifest.StreamIndex, &directory); err != nil {
			return stats, err
		}
		manifest.Roots = make(map[string]map[int64]auditBlob)
		for _, ref := range directory.Pages {
			if ref.Key == "" {
				continue
			}
			var roots map[string]map[int64]auditBlob
			if err := load(ref, &roots); err != nil {
				return stats, err
			}
			for name, levels := range roots {
				manifest.Roots[name] = levels
			}
		}
	}
	var walk func(auditBlob, int64, *[]auditEdge) error
	walk = func(ref auditBlob, level int64, leaves *[]auditEdge) error {
		b, err := ranges.GetRange(ctx, ref.Key, ref.Offset, ref.Length)
		if err != nil {
			return err
		}
		if int64(len(b)) != ref.Length || sha256.Sum256(b) != ref.Hash {
			return fmt.Errorf("invalid audit index checksum")
		}
		var node auditNode
		if err = auditDecode(b, &node); err != nil {
			return err
		}
		if len(node.Entries) == 0 {
			return fmt.Errorf("empty audit index")
		}
		stats.indexNodes++
		stats.liveIndexBytes += ref.Length
		for _, edge := range node.Entries {
			if node.Leaf {
				*leaves = append(*leaves, edge)
			} else if err = walk(edge.Blob, level, leaves); err != nil {
				return err
			}
		}
		return nil
	}
	for _, levels := range manifest.Roots {
		for level, root := range levels {
			var leaves []auditEdge
			if err = walk(root, level, &leaves); err != nil {
				return stats, err
			}
			for i, edge := range leaves {
				stats.liveDataBytes += edge.Blob.Length
				if level == 0 {
					stats.rawBlocks++
					stats.rawRecords += int64(edge.Records)
				} else {
					stats.aggregateBlocks++
					stats.aggregateRecords += int64(edge.Records)
					if i < len(leaves)-1 && edge.Records != 1024 {
						stats.fragmentedAggregates++
					}
				}
			}
		}
	}
	return stats, nil
}

func cardinalityOutput(t *testing.T, key string, header []string) *csv.Writer {
	t.Helper()
	path := benchmarkOutputPath(t, key)
	var f *os.File
	if path == "" {
		f, _ = os.CreateTemp(t.TempDir(), "cardinality-*.csv")
	} else {
		var err error
		f, err = os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if f == nil {
		t.Fatal("cannot create benchmark CSV")
	}
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	t.Cleanup(func() {
		w.Flush()
		if err := w.Error(); err != nil {
			t.Error(err)
		}
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	return w
}
func cardinalityRow(t *testing.T, w *csv.Writer, values ...any) {
	t.Helper()
	row := make([]string, len(values))
	for i, v := range values {
		row[i] = fmt.Sprint(v)
	}
	if err := w.Write(row); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
}
func TestMetricCardinality(t *testing.T) {
	counts := cardinalities(t)
	points := benchmarkInteger(t, "METRICQ_CARDINALITY_POINTS", 128)
	chunkSize := benchmarkInteger(t, "METRICQ_CARDINALITY_CHUNK", 16)
	repetitions := benchmarkInteger(t, "METRICQ_CARDINALITY_REPETITIONS", 3)
	objectTarget := int64(benchmarkInteger(t, "METRICQ_CARDINALITY_OBJECT_TARGET_BYTES", 4<<20))
	builderHard := int64(benchmarkInteger(t, "METRICQ_CARDINALITY_BUILDER_HARD_BYTES", 32<<20))
	walTarget := int64(benchmarkInteger(t, "METRICQ_CARDINALITY_WAL_TARGET_BYTES", 16<<20))
	memoryLimit := int64(benchmarkInteger(t, "METRICQ_CARDINALITY_MEMORY_LIMIT_BYTES", 256<<20))
	previousLimit := debug.SetMemoryLimit(memoryLimit)
	t.Cleanup(func() { debug.SetMemoryLimit(previousLimit) })
	if points < 4 || builderHard < objectTarget {
		t.Fatal("at least four points and builder >= object target required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	summary := cardinalityOutput(t, "METRICQ_CARDINALITY_OUTPUT", []string{"metrics", "points_per_metric", "chunk_points", "object_target_bytes", "builder_hard_bytes", "wal_target_bytes", "go_soft_memory_limit_bytes", "logical_sample_bytes", "protobuf_input_bytes", "ingest_s", "ingest_points_per_s", "put_calls", "data_put_bytes", "index_put_bytes", "manifest_put_bytes", "s3_write_amplification", "live_data_bytes", "obsolete_data_bytes", "live_index_bytes", "obsolete_index_bytes", "raw_blocks", "mean_raw_records_per_block", "aggregate_blocks", "mean_aggregate_records_per_block", "fragmented_aggregate_blocks", "peak_heap_alloc_bytes"})
	queries := cardinalityOutput(t, "METRICQ_CARDINALITY_QUERY_OUTPUT", []string{"configured_metrics", "cache", "kind", "requested_metrics", "rep", "latency_ms", "data_range_gets", "index_range_gets", "range_bytes", "response_rows"})
	var active atomic.Pointer[prometheus.Registry]
	listener, err := net.Listen("tcp", env("METRICQ_CARDINALITY_LISTEN", "127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		registry := active.Load()
		if registry == nil {
			http.Error(w, "initializing", http.StatusServiceUnavailable)
			return
		}
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Close()
		if err := <-served; err != nil && err != http.ErrServerClosed {
			t.Error(err)
		}
	})
	t.Logf("Prometheus: http://%s/metrics", listener.Addr())
	for _, count := range counts {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			id := fmt.Sprintf("hta-cardinality-%d-%d", time.Now().UnixNano(), count)
			raw, _ := newS3(t, ctx, id)
			store := &cardinalityStore{measuredStore: &measuredStore{Store: raw, ranges: raw.(storage.RangeGetter)}}
			registry := prometheus.NewRegistry()
			registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
			metrics := engine.NewMetrics(registry)
			active.Store(registry)
			configs := make(map[string]hta.Config, count)
			name := func(i int) string { return fmt.Sprintf("metric.%06d", i) }
			for i := 0; i < count; i++ {
				configs[name(i)] = hta.Config{IntervalMin: int64(time.Second), IntervalMax: int64(time.Hour) * 24 * 365, IntervalFactor: 10}
			}
			opts := engine.Options{WALDirectory: t.TempDir(), CheckpointUnsavedBytes: objectTarget, IngestMemoryLimitBytes: builderHard, WALTarget: walTarget, WALHigh: walTarget * 2, WALHard: walTarget * 3}
			e, err := engine.Open(ctx, store, opts, configs, metrics)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { e.Close() })
			response, probeErr := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + listener.Addr().String() + "/metrics")
			if probeErr != nil {
				t.Fatal(probeErr)
			}
			body, probeErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			if probeErr != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), "metricq_db_wal_bytes") {
				t.Fatalf("Prometheus endpoint failed: status=%d error=%v", response.StatusCode, probeErr)
			}

			// Sample memory throughout both ingestion and queries. This is Go heap
			// usage, not a hard RSS cap; the other thresholds are configuration limits.
			var peak atomic.Uint64
			memoryDone := make(chan struct{})
			stopMemory := make(chan struct{})
			go func() {
				defer close(memoryDone)
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
						if peak.CompareAndSwap(old, m.HeapAlloc) {
							break
						}
					}
					select {
					case <-stopMemory:
						return
					case <-ticker.C:
					}
				}
			}()
			t.Cleanup(func() { close(stopMemory); <-memoryDone })
			base := int64(1700000000) * int64(time.Second)
			var inputBytes int64
			started := time.Now()
			progress := started
			// Round-robin: every metric advances by one delivery before any receives
			// its next delivery. No metric-count-dependent buffer or cache growth.
			for begin := 0; begin < points; begin += chunkSize {
				end := min(begin+chunkSize, points)
				for metric := 0; metric < count; metric++ {
					c := &metricq.DataChunk{}
					for point := begin; point < end; point++ {
						delta := int64(time.Second)
						if point == begin {
							delta = base + int64(point)*int64(time.Second)
						}
						c.TimeDelta = append(c.TimeDelta, delta)
						c.Value = append(c.Value, float64(metric)+math.Sin(float64(point)/31))
					}
					inputBytes += int64(proto.Size(c))
					if err = e.Ingest(ctx, name(metric), c); err != nil {
						t.Fatal(err)
					}
					if e.NeedsFlush() {
						if err = e.Flush(ctx); err != nil {
							t.Fatal(err)
						}
					}
				}
				if time.Since(progress) > 20*time.Second {
					t.Logf("metrics=%d points/metric=%d/%d elapsed=%.1fs", count, end, points, time.Since(started).Seconds())
					progress = time.Now()
				}
			}
			if err = e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started).Seconds()
			dataWritten, indexWritten, manifestWritten, putCalls := store.dataPutBytes.Load(), store.indexPutBytes.Load(), store.manifestPutBytes.Load(), store.puts.Load()
			// Layout inspection is excluded from ingestion/query timings and counters.
			layout, err := auditLayout(ctx, raw)
			if err != nil {
				t.Fatal(err)
			}
			if layout.rawRecords != int64(count)*int64(points) {
				t.Fatalf("raw records %d, want %d", layout.rawRecords, count*points)
			}
			if layout.fragmentedAggregates != 0 {
				t.Fatalf("%d fragmented aggregate blocks", layout.fragmentedAggregates)
			}
			rng := rand.New(rand.NewSource(42))
			window := int64(points-1) * int64(time.Second)
			for _, condition := range []struct {
				label                string
				kind                 metricq.HistoryRequest_RequestType
				interval, start, end int64
			}{
				{"raw-window", metricq.HistoryRequest_FLEX_TIMELINE, 0, base, base + min(window, int64(10*time.Second))},
				{"timeline-100", metricq.HistoryRequest_FLEX_TIMELINE, window / 100, base, base + window},
				{"timeline-1000", metricq.HistoryRequest_FLEX_TIMELINE, window / 1000, base, base + window},
				{"aggregate", metricq.HistoryRequest_AGGREGATE, 0, base, base + window},
			} {
				req := &metricq.HistoryRequest{Type: condition.kind, StartTime: condition.start, EndTime: condition.end, IntervalMax: condition.interval}
				// A complete sweep populates the shared cache with the working set for
				// this request shape; measured sweep requests get no immediate warmup.
				for metric := 0; metric < count; metric++ {
					if _, err = e.Query(ctx, name(metric), req); err != nil {
						t.Fatal(err)
					}
				}
				targets := []int{1}
				if count > 1 {
					targets = append(targets, min(6, count))
				}
				for _, target := range targets {
					for rep := 0; rep < repetitions; rep++ {
						first := rng.Intn(count)
						var expected []*metricq.HistoryResponse
						for _, mode := range []string{"cold", "sweep", "hot"} {
							func() {
								selected := e
								if mode != "sweep" {
									coldOpts := opts
									coldOpts.WALDirectory = t.TempDir()
									selected, err = engine.Open(ctx, store, coldOpts, nil, metrics)
									if err != nil {
										t.Fatal(err)
									}
								}
								if mode != "sweep" {
									defer selected.Close()
								}
								if mode == "hot" {
									for j := 0; j < target; j++ {
										if _, err = selected.Query(ctx, name((first+j)%count), req); err != nil {
											t.Fatal(err)
										}
									}
								}
								dataGets, indexGets, bytes := store.dataGets.Load(), store.indexGets.Load(), store.bytes.Load()
								began := time.Now()
								responses := make([]*metricq.HistoryResponse, target)
								for j := 0; j < target; j++ {
									responses[j], err = selected.Query(ctx, name((first+j)%count), req)
									if err != nil {
										t.Fatal(err)
									}
								}
								latency := float64(time.Since(began)) / float64(time.Millisecond)
								var rows int
								for j, response := range responses {
									rows += max(len(response.TimeDelta), len(response.Aggregate))
									if mode != "cold" {
										if err = compare(expected[j], response); err != nil {
											t.Fatalf("%s cache changed response: %v", mode, err)
										}
									}
								}
								if mode == "cold" {
									expected = responses
								}
								cardinalityRow(t, queries, count, mode, condition.label, target, rep, latency, store.dataGets.Load()-dataGets, store.indexGets.Load()-indexGets, store.bytes.Load()-bytes, rows)
							}()
						}
					}
				}
			}
			logical := int64(count) * int64(points) * 16
			mean := func(n, d int64) float64 {
				if d == 0 {
					return 0
				}
				return float64(n) / float64(d)
			}
			cardinalityRow(t, summary, count, points, chunkSize, objectTarget, builderHard, walTarget, memoryLimit, logical, inputBytes, elapsed, float64(count)*float64(points)/elapsed, putCalls, dataWritten, indexWritten, manifestWritten, mean(dataWritten+indexWritten+manifestWritten, logical), layout.liveDataBytes, dataWritten-layout.liveDataBytes, layout.liveIndexBytes, indexWritten-layout.liveIndexBytes, layout.rawBlocks, mean(layout.rawRecords, layout.rawBlocks), layout.aggregateBlocks, mean(layout.aggregateRecords, layout.aggregateBlocks), layout.fragmentedAggregates, peak.Load())
			t.Logf("metrics=%d ingestion=%.1fs rate=%.0f/s raw records/block=%.1f obsolete data=%.1f%% PUT/input=%.2f", count, elapsed, float64(count)*float64(points)/elapsed, mean(layout.rawRecords, layout.rawBlocks), 100*mean(dataWritten-layout.liveDataBytes, dataWritten), mean(dataWritten+indexWritten+manifestWritten, logical))
		})
	}
}
