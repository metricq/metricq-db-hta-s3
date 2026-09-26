//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-go/engine"
	"github.com/metricq/metricq-db-hta-go/hta"
	"github.com/metricq/metricq-db-hta-go/storage"
	metricq "github.com/metricq/metricq-go"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

func benchmarkInteger(t *testing.T, key string, fallback int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("%s must be positive: %q", key, v)
	}
	return n
}

func benchmarkSpans(t *testing.T) []int64 {
	t.Helper()
	if spec := os.Getenv("METRICQ_BENCH_LOG_SPANS"); spec != "" {
		parts := strings.Split(spec, ",")
		if len(parts) != 3 {
			t.Fatalf("METRICQ_BENCH_LOG_SPANS must be min_seconds,max_seconds,count: %q", spec)
		}
		minSeconds, minErr := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		maxSeconds, maxErr := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		count, countErr := strconv.Atoi(strings.TrimSpace(parts[2]))
		if minErr != nil || maxErr != nil || countErr != nil || minSeconds <= 0 || maxSeconds <= minSeconds || count < 2 || maxSeconds > float64(math.MaxInt64)/float64(time.Second) {
			t.Fatalf("invalid METRICQ_BENCH_LOG_SPANS %q", spec)
		}
		spans := make([]int64, 0, count)
		for i := 0; i < count; i++ {
			seconds := minSeconds * math.Pow(maxSeconds/minSeconds, float64(i)/float64(count-1))
			span := int64(math.Round(seconds * float64(time.Second)))
			if span < 1 || (len(spans) > 0 && span <= spans[len(spans)-1]) {
				t.Fatalf("METRICQ_BENCH_LOG_SPANS produces duplicate or invalid spans at %d", i)
			}
			spans = append(spans, span)
		}
		return spans
	}
	value := os.Getenv("METRICQ_BENCH_SPANS")
	if value == "" {
		return []int64{int64(time.Second), 10 * int64(time.Second), 100 * int64(time.Second), 1000 * int64(time.Second)}
	}
	parts := strings.Split(value, ",")
	spans := make([]int64, 0, len(parts))
	for _, part := range parts {
		seconds, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || seconds <= 0 || seconds > float64(math.MaxInt64)/float64(time.Second) {
			t.Fatalf("invalid METRICQ_BENCH_SPANS entry %q", part)
		}
		span := int64(math.Round(seconds * float64(time.Second)))
		if span < 1 {
			t.Fatalf("METRICQ_BENCH_SPANS entry below one nanosecond: %q", part)
		}
		spans = append(spans, span)
	}
	return spans
}

func TestBenchmarkLogSpans(t *testing.T) {
	t.Setenv("METRICQ_BENCH_LOG_SPANS", "0.1,10000000,50")
	spans := benchmarkSpans(t)
	if len(spans) != 50 || spans[0] != int64(100*time.Millisecond) || spans[len(spans)-1] != int64(10000000*time.Second) {
		t.Fatalf("unexpected logarithmic spans: first=%d last=%d count=%d", spans[0], spans[len(spans)-1], len(spans))
	}
	for i := 1; i < len(spans); i++ {
		if spans[i] <= spans[i-1] {
			t.Fatalf("spans are not strictly increasing at %d", i)
		}
	}
}

func benchmarkOutputPath(t *testing.T, key string) string {
	t.Helper()
	path := os.Getenv(key)
	if path == "" {
		return ""
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join("..", path) // go test runs in the integration package directory.
	}
	probe, err := os.CreateTemp(filepath.Dir(path), ".metricq-benchmark-output-*")
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(probe.Name()); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBenchmarkOutputPath(t *testing.T) {
	t.Setenv("METRICQ_BENCH_OUTPUT", "docs/latency-scale.csv")
	got := benchmarkOutputPath(t, "METRICQ_BENCH_OUTPUT")
	if got != filepath.Join("..", "docs", "latency-scale.csv") {
		t.Fatalf("relative output path = %q", got)
	}
}

type latencyCell struct {
	Backend, Kind       string
	Metrics, Positions  int
	SpanSeconds         float64
	Samples             []float64
	Gets                []float64
	Bytes               []float64
	DataGets, IndexGets []float64
}

type ingestMeasurement struct {
	Metric       int
	Points       int
	PublishTime  time.Duration
	OldReadyTime time.Duration
	NewReadyTime time.Duration
}

type measuredStore struct {
	storage.Store
	ranges              storage.RangeGetter
	gets, bytes         atomic.Int64
	dataGets, indexGets atomic.Int64
}

func (s *measuredStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.gets.Add(1)
	if strings.HasPrefix(key, "data/") {
		s.dataGets.Add(1)
	}
	if strings.HasPrefix(key, "index/") {
		s.indexGets.Add(1)
	}
	b, err := s.ranges.GetRange(ctx, key, offset, length)
	s.bytes.Add(int64(len(b)))
	return b, err
}

func (c *latencyCell) report(t *testing.T) string {
	v := append([]float64(nil), c.Samples...)
	sort.Float64s(v)
	var sum, square float64
	for _, x := range v {
		sum += x
		square += x * x
	}
	mean := sum / float64(len(v))
	variance := math.Max(0, square/float64(len(v))-mean*mean)
	ci := 1.96 * math.Sqrt(variance/float64(len(v)))
	p95 := v[int(math.Ceil(.95*float64(len(v))))-1]
	var gets, bytes float64
	for _, n := range c.Gets {
		gets += n
	}
	for _, n := range c.Bytes {
		bytes += n
	}
	var dataGets, indexGets float64
	for _, n := range c.DataGets {
		dataGets += n
	}
	for _, n := range c.IndexGets {
		indexGets += n
	}
	return fmt.Sprintf("%s,%s,%d,%d,%.9g,%d,%.3f,%.3f,%.3f,%.3f,%.1f,%.0f,%.2f,%.2f", c.Backend, c.Kind, c.Metrics, c.Positions, c.SpanSeconds, len(v), mean, ci, v[len(v)/2], p95, gets/float64(len(v)), bytes/float64(len(v)), dataGets/float64(len(v)), indexGets/float64(len(v)))
}

// TestRequestLatency is a small, reproducible version of Ilsche §4.3.5:
// random windows, log-spaced spans, timeline/aggregate, one/six metrics.
// It measures client end-to-end latency; the two server-internal timings from
// the dissertation need equivalent instrumentation in both implementations.
func TestRequestLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	const metricCount = 6
	const second = int64(time.Second)
	points := benchmarkInteger(t, "METRICQ_BENCH_POINTS", 20000)
	rate := benchmarkInteger(t, "METRICQ_BENCH_RATE_HZ", 10)
	repetitions := benchmarkInteger(t, "METRICQ_BENCH_REPETITIONS", 20)
	objectTarget := int64(benchmarkInteger(t, "METRICQ_BENCH_OBJECT_TARGET_BYTES", 1<<20))
	walTarget := int64(benchmarkInteger(t, "METRICQ_BENCH_WAL_TARGET_BYTES", 32<<20))
	htaMaxSeconds := benchmarkInteger(t, "METRICQ_BENCH_HTA_MAX_SECONDS", 1000)
	durations := benchmarkSpans(t)
	latencyOutput := benchmarkOutputPath(t, "METRICQ_BENCH_OUTPUT")
	ingestOutput := benchmarkOutputPath(t, "METRICQ_BENCH_INGEST_OUTPUT")
	if points < 1000 || int64(rate) > second || second%int64(rate) != 0 {
		t.Fatal("at least 1000 points and an integral nanosecond sample period required")
	}
	if int64(htaMaxSeconds) > math.MaxInt64/second {
		t.Fatal("METRICQ_BENCH_HTA_MAX_SECONDS is too large")
	}
	id := fmt.Sprintf("hta-latency-%d", time.Now().UnixNano())
	server := env("METRICQ_AMQP", "amqp://admin:admin@localhost/")
	oldToken, newToken := "db-"+id+"-old", "db-"+id+"-new"
	t.Cleanup(func() {
		conn, err := amqp.Dial(server)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Error(err)
			return
		}
		defer ch.Close()
		for _, token := range []string{oldToken, newToken} {
			for _, suffix := range []string{"-data", "-hreq"} {
				_, _ = ch.QueueDelete(token+suffix, false, false, false)
			}
		}
	})
	rawBackend, _ := newS3(t, ctx, id)
	backend := &measuredStore{Store: rawBackend, ranges: rawBackend.(storage.RangeGetter)}
	base := int64(1700000000) * second
	step := second / int64(rate)
	last := base + int64(points-1)*step
	oldMetrics := map[string]any{}
	newMetrics := map[string]hta.Config{}
	bindings := make([]metricq.DBBinding, 0, metricCount)
	inputs := map[string]string{}
	for i := 0; i < metricCount; i++ {
		input := fmt.Sprintf("%s.input.%d", id, i)
		oldName := fmt.Sprintf("%s.old.%d", id, i)
		newName := fmt.Sprintf("%s.new.%d", id, i)
		cfg := hta.Config{Input: input, IntervalMin: step, IntervalMax: int64(htaMaxSeconds) * second, IntervalFactor: 10}
		oldMetrics[oldName] = map[string]any{"input": input, "mode": "RW", "interval_min": cfg.IntervalMin, "interval_max": cfg.IntervalMax, "interval_factor": cfg.IntervalFactor}
		newMetrics[newName] = cfg
		bindings = append(bindings, metricq.DBBinding{Name: newName, Input: input})
		inputs[input] = newName
		for _, name := range []string{input, oldName, newName} {
			seed(t, "metadata", name, map[string]any{"description": "isolated latency benchmark"})
		}
	}
	seed(t, "config", oldToken, map[string]any{"threads": metricCount, "type": "file", "path": "/tmp", "metrics": oldMetrics})
	seed(t, "config", newToken, map[string]any{"metrics": newMetrics})
	docker(t, "run", "-d", "--name", id, "--network", env("METRICQ_DOCKER_NETWORK", "metricq_metricq-network"), "--entrypoint", "/usr/bin/metricq-db-hta", env("METRICQ_LEGACY_IMAGE", "metricq-db-hta"), "--server", env("METRICQ_DOCKER_AMQP", "amqp://admin:admin@rabbitmq-server/"), "--token", oldToken)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(docker(t, "logs", id))
		}
		docker(t, "rm", "-f", id)
	})
	e, err := engine.Open(ctx, backend, engine.Options{WALDirectory: t.TempDir(), ObjectTarget: objectTarget, BuilderHard: max(64<<20, objectTarget*2), WALTarget: walTarget, WALHigh: walTarget * 2, WALHard: walTarget * 3}, newMetrics, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	db, err := metricq.NewDB(newToken, server)
	if err != nil {
		t.Fatal(err)
	}
	dbCtx, stopDB := context.WithCancel(ctx)
	dbDone := make(chan error, 1)
	go func() {
		dbDone <- db.Run(dbCtx, metricq.DBHandlers{
			Configure: func(context.Context, json.RawMessage) ([]metricq.DBBinding, error) { return bindings, nil },
			Data: func(ctx context.Context, input string, chunk *metricq.DataChunk) error {
				for {
					err := e.Ingest(ctx, inputs[input], chunk)
					if errors.Is(err, engine.ErrPressure) {
						if err := e.Flush(ctx); err != nil {
							return err
						}
						continue
					}
					if err != nil {
						return err
					}
					if e.NeedsFlush() {
						return e.Flush(ctx)
					}
					return nil
				}
			},
			History: e.Query,
		})
	}()
	t.Cleanup(func() {
		stopDB()
		select {
		case err := <-dbDone:
			if err != nil && err != context.Canceled {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("new DB did not stop")
		}
	})
	agent, err := metricq.NewAgent("history-"+id, server)
	if err != nil {
		t.Fatal(err)
	}
	if err = agent.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	history, err := metricq.NewHistoryClient(ctx, agent)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	request := func(name string, start, end, interval int64, kind metricq.HistoryRequest_RequestType) (*metricq.HistoryResponse, error) {
		callCtx, done := context.WithTimeout(ctx, 60*time.Second)
		defer done()
		r, _, err := history.Request(callCtx, name, time.Unix(0, start), time.Unix(0, end), time.Duration(interval), kind)
		return r, err
	}
	waitReady := func(name string, want int64) time.Time {
		deadline := time.Now().Add(15 * time.Minute)
		nextProgress := time.Now().Add(30 * time.Second)
		for {
			r, err := request(name, 0, 0, 0, metricq.HistoryRequest_LAST_VALUE)
			if err == nil && len(r.TimeDelta) == 1 && r.TimeDelta[0] == want {
				return time.Now()
			}
			if time.Now().After(nextProgress) {
				seen := int64(0)
				if r != nil && len(r.TimeDelta) == 1 {
					seen = r.TimeDelta[0]
				}
				t.Logf("ingest catch-up %s: last timestamp=%d target=%d error=%v", name, seen, want, err)
				nextProgress = time.Now().Add(30 * time.Second)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not ready: %v %v", name, r, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	// Empty LAST_VALUE responses show that both history bindings are live.
	for _, name := range []string{id + ".old.0", id + ".new.0"} {
		deadline := time.Now().Add(90 * time.Second)
		for {
			_, err := request(name, 0, 0, 0, metricq.HistoryRequest_LAST_VALUE)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s binding not ready: %v", name, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	conn, err := amqp.Dial(server)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	measurements := make([]ingestMeasurement, 0, metricCount)
	for metric := 0; metric < metricCount; metric++ {
		started := time.Now()
		for begin := 0; begin < points; begin += 500 {
			end := min(begin+500, points)
			chunk := &metricq.DataChunk{}
			var previous int64
			for j := begin; j < end; j++ {
				timestamp := base + int64(j)*step
				chunk.TimeDelta = append(chunk.TimeDelta, timestamp-previous)
				chunk.Value = append(chunk.Value, float64(metric)+math.Sin(float64(j)/31))
				previous = timestamp
			}
			body, err := proto.Marshal(chunk)
			if err != nil {
				t.Fatal(err)
			}
			if err := ch.PublishWithContext(ctx, "metricq.data", fmt.Sprintf("%s.input.%d", id, metric), true, false, amqp.Publishing{Body: body, DeliveryMode: amqp.Persistent}); err != nil {
				t.Fatal(err)
			}
			select {
			case confirmation := <-confirms:
				if !confirmation.Ack {
					t.Fatal("publish not confirmed")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if end%10000000 == 0 {
				t.Logf("ingest progress metric=%d points=%d/%d elapsed=%.1fs", metric, end, points, time.Since(started).Seconds())
			}
		}
		published := time.Now()
		oldReady := waitReady(fmt.Sprintf("%s.old.%d", id, metric), last)
		newReady := waitReady(fmt.Sprintf("%s.new.%d", id, metric), last)
		measurements = append(measurements, ingestMeasurement{Metric: metric, Points: points, PublishTime: published.Sub(started), OldReadyTime: oldReady.Sub(started), NewReadyTime: newReady.Sub(started)})
		t.Logf("ingest metric=%d points=%d publish=%.3fs (%.0f points/s) file_visible=%.3fs (%.0f points/s) s3_visible=%.3fs (%.0f points/s)", metric, points, published.Sub(started).Seconds(), float64(points)/published.Sub(started).Seconds(), oldReady.Sub(started).Seconds(), float64(points)/oldReady.Sub(started).Seconds(), newReady.Sub(started).Seconds(), float64(points)/newReady.Sub(started).Seconds())
	}
	if ingestOutput != "" {
		f, err := os.Create(ingestOutput)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(f, "metric,points,publish_s,publish_points_per_s,file_visible_s,file_visible_points_per_s,s3_visible_s,s3_visible_points_per_s")
		for _, m := range measurements {
			fmt.Fprintf(f, "%d,%d,%.6f,%.1f,%.6f,%.1f,%.6f,%.1f\n", m.Metric, m.Points, m.PublishTime.Seconds(), float64(m.Points)/m.PublishTime.Seconds(), m.OldReadyTime.Seconds(), float64(m.Points)/m.OldReadyTime.Seconds(), m.NewReadyTime.Seconds(), float64(m.Points)/m.NewReadyTime.Seconds())
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	const header = "backend,kind,metrics,positions,span_s,n,mean_ms,ci95_ms,median_ms,p95_ms,mean_range_gets,mean_range_bytes,mean_data_range_gets,mean_index_range_gets"
	var latencyFile *os.File
	if latencyOutput != "" {
		latencyFile, err = os.Create(latencyOutput)
		if err != nil {
			t.Fatal(err)
		}
		defer latencyFile.Close()
		if _, err := fmt.Fprintln(latencyFile, header); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(42))
	positions := []int{100, 1000}
	var cells []latencyCell
	for _, span := range durations {
		if span >= int64(points)*step {
			continue
		}
		spanSeconds := float64(span) / float64(second)
		for _, target := range []int{1, metricCount} {
			for _, kind := range []struct {
				name  string
				code  metricq.HistoryRequest_RequestType
				pixel int
			}{{"aggregate", metricq.HistoryRequest_AGGREGATE, 0}, {"timeline", metricq.HistoryRequest_AGGREGATE_TIMELINE, positions[0]}, {"timeline", metricq.HistoryRequest_AGGREGATE_TIMELINE, positions[1]}} {
				pair := []latencyCell{{Backend: "file", Kind: kind.name, Metrics: target, Positions: kind.pixel, SpanSeconds: spanSeconds}, {Backend: "s3", Kind: kind.name, Metrics: target, Positions: kind.pixel, SpanSeconds: spanSeconds}}
				for rep := 0; rep < repetitions; rep++ {
					start := base + int64(rng.Int63n(int64(points)*step-span))
					end := start + span
					interval := int64(0)
					if kind.pixel > 0 {
						interval = span / int64(kind.pixel)
					}
					firstMetric := rng.Intn(metricCount)
					order := []int{0, 1}
					if rep%2 == 1 {
						order[0], order[1] = 1, 0
					}
					var responses [2][]*metricq.HistoryResponse
					for _, backendIndex := range order {
						label := pair[backendIndex].Backend
						oldGets, oldBytes := backend.gets.Load(), backend.bytes.Load()
						oldDataGets, oldIndexGets := backend.dataGets.Load(), backend.indexGets.Load()
						results := make([]*metricq.HistoryResponse, target)
						errors := make([]error, target)
						var wg sync.WaitGroup
						began := time.Now()
						for j := 0; j < target; j++ {
							wg.Add(1)
							go func(j int) {
								defer wg.Done()
								name := fmt.Sprintf("%s.%s.%d", id, map[string]string{"file": "old", "s3": "new"}[label], (firstMetric+j)%metricCount)
								results[j], errors[j] = request(name, start, end, interval, kind.code)
							}(j)
						}
						wg.Wait()
						for _, err := range errors {
							if err != nil {
								t.Fatalf("%s %s %.9gs: %v", label, kind.name, spanSeconds, err)
							}
						}
						pair[backendIndex].Samples = append(pair[backendIndex].Samples, float64(time.Since(began))/float64(time.Millisecond))
						pair[backendIndex].Gets = append(pair[backendIndex].Gets, float64(backend.gets.Load()-oldGets))
						pair[backendIndex].Bytes = append(pair[backendIndex].Bytes, float64(backend.bytes.Load()-oldBytes))
						pair[backendIndex].DataGets = append(pair[backendIndex].DataGets, float64(backend.dataGets.Load()-oldDataGets))
						pair[backendIndex].IndexGets = append(pair[backendIndex].IndexGets, float64(backend.indexGets.Load()-oldIndexGets))
						responses[backendIndex] = results
					}
					for j := 0; j < target; j++ {
						if err := compare(responses[0][j], responses[1][j]); err != nil {
							t.Fatalf("response differs for %s %.9gs: %v", kind.name, spanSeconds, err)
						}
					}
				}
				cells = append(cells, pair...)
				if latencyFile != nil {
					for i := range pair {
						if _, err := fmt.Fprintln(latencyFile, pair[i].report(t)); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
		}
	}
	t.Log(header)
	for i := range cells {
		t.Log(cells[i].report(t))
	}
	if latencyFile != nil {
		if err := latencyFile.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("dataset: %d metrics × %d samples at %d Sa/s; spans=%v ns; repetitions=%d; file backend=/tmp in Docker; S3 backend=%s", metricCount, points, rate, durations, repetitions, env("METRICQ_TEST_S3", "http://localhost:19000"))
}
