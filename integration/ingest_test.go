//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/engine"
	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// TestIngestThroughput measures end-to-end ingestion of the Go/S3 database
// alone: RabbitMQ delivery, WAL durability, aggregation and background S3
// checkpoints, until LAST_VALUE shows the final point of every metric. It
// compares per-delivery and batched (group commit) handlers across AMQP
// prefetch values. Place the WAL on the intended device: tmpfs hides fsync.
func TestIngestThroughput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	metricCount := benchmarkInteger(t, "METRICQ_INGEST_METRICS", 6)
	points := benchmarkInteger(t, "METRICQ_INGEST_POINTS", 1000000)
	chunkSize := benchmarkInteger(t, "METRICQ_INGEST_CHUNK", 500)
	objectTarget := int64(benchmarkInteger(t, "METRICQ_INGEST_OBJECT_TARGET_BYTES", 4<<20))
	walTarget := int64(benchmarkInteger(t, "METRICQ_INGEST_WAL_TARGET_BYTES", 32<<20))
	walParent := os.Getenv("METRICQ_INGEST_WAL_DIR")
	var prefetches []int
	for _, v := range strings.Split(env("METRICQ_INGEST_PREFETCH", "50,400,2000"), ",") {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n < 1 {
			t.Fatalf("invalid prefetch %q", v)
		}
		prefetches = append(prefetches, n)
	}
	modes := strings.Split(env("METRICQ_INGEST_MODES", "single,batch"), ",")
	output := benchmarkOutputPath(t, "METRICQ_INGEST_OUTPUT")
	var rows []string
	for _, mode := range modes {
		for _, prefetch := range prefetches {
			row := ingestRun(t, ctx, mode, prefetch, metricCount, points, chunkSize, objectTarget, walTarget, walParent)
			rows = append(rows, row)
		}
	}
	if output != "" {
		body := "mode,prefetch,metrics,points_per_metric,chunk,seconds,points_per_s,wal_syncs,chunks_per_sync,mean_sync_ms,flushes,mean_flush_ms\n" + strings.Join(rows, "\n") + "\n"
		if err := os.WriteFile(output, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func histogram(t *testing.T, r *prometheus.Registry, name string) (uint64, float64) {
	t.Helper()
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == name {
			h := f.GetMetric()[0].GetHistogram()
			return h.GetSampleCount(), h.GetSampleSum()
		}
	}
	return 0, 0
}

func ingestRun(t *testing.T, ctx context.Context, mode string, prefetch, metricCount, points, chunkSize int, objectTarget, walTarget int64, walParent string) string {
	t.Helper()
	const second = int64(time.Second)
	id := fmt.Sprintf("hta-ingest-%s-%d-%d", mode, prefetch, time.Now().UnixNano())
	server := env("METRICQ_AMQP", "amqp://admin:admin@localhost/")
	token := "db-" + id
	backend, _ := newS3(t, ctx, id)
	walDir, err := os.MkdirTemp(walParent, "wal-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(walDir)
	step := second / 10
	base := int64(1700000000) * second
	last := base + int64(points-1)*step
	configs := map[string]hta.Config{}
	bindings := make([]metricq.DBBinding, 0, metricCount)
	inputs := map[string]string{}
	for i := 0; i < metricCount; i++ {
		input := fmt.Sprintf("%s.input.%d", id, i)
		name := fmt.Sprintf("%s.new.%d", id, i)
		configs[name] = hta.Config{Input: input, IntervalMin: step, IntervalMax: 1000000 * second, IntervalFactor: 10}
		bindings = append(bindings, metricq.DBBinding{Name: name, Input: input})
		inputs[input] = name
		for _, n := range []string{input, name} {
			seed(t, "metadata", n, map[string]any{"description": "isolated ingest benchmark"})
		}
	}
	seed(t, "config", token, map[string]any{"metrics": configs})
	registry := prometheus.NewRegistry()
	e, err := engine.Open(ctx, backend, engine.Options{WALDirectory: walDir, ObjectTarget: objectTarget, BuilderHard: max(64<<20, objectTarget*2), WALTarget: walTarget, WALHigh: walTarget * 2, WALHard: walTarget * 3}, configs, engine.NewMetrics(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go e.RunFlush(runCtx)
	db, err := metricq.NewDB(token, server)
	if err != nil {
		t.Fatal(err)
	}
	db.Prefetch = prefetch
	// Same retry policy as the executable: flush on pressure, keep the delivery.
	var pressure atomic.Int64
	ingest := func(ctx context.Context, deliveries []engine.Delivery) error {
		for len(deliveries) > 0 {
			n, err := e.IngestBatch(ctx, deliveries)
			deliveries = deliveries[n:]
			if err == nil || len(deliveries) == 0 {
				continue
			}
			if !errors.Is(err, engine.ErrPressure) {
				return err
			}
			pressure.Add(1)
			if err = e.Flush(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	handlers := metricq.DBHandlers{
		Configure: func(context.Context, json.RawMessage) ([]metricq.DBBinding, error) { return bindings, nil },
		History:   e.Query,
	}
	switch mode {
	case "single":
		handlers.Data = func(ctx context.Context, input string, c *metricq.DataChunk) error {
			return ingest(ctx, []engine.Delivery{{Metric: inputs[input], Chunk: c}})
		}
	case "batch":
		handlers.DataBatch = func(ctx context.Context, messages []metricq.DataMessage) error {
			deliveries := make([]engine.Delivery, len(messages))
			for i, m := range messages {
				deliveries[i] = engine.Delivery{Metric: inputs[m.Input], Chunk: m.Chunk}
			}
			return ingest(ctx, deliveries)
		}
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	dbDone := make(chan error, 1)
	go func() { dbDone <- db.Run(runCtx, handlers) }()
	defer func() {
		stop()
		select {
		case <-dbDone:
		case <-time.After(10 * time.Second):
			t.Error("DB did not stop")
		}
	}()
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
	lastValue := func(name string) (int64, error) {
		callCtx, done := context.WithTimeout(ctx, 30*time.Second)
		defer done()
		r, _, err := history.Request(callCtx, name, time.Unix(0, 0), time.Unix(0, 0), 0, metricq.HistoryRequest_LAST_VALUE)
		if err != nil || len(r.TimeDelta) != 1 {
			return 0, err
		}
		return r.TimeDelta[0], nil
	}
	for deadline := time.Now().Add(90 * time.Second); ; {
		if _, err = lastValue(inputs[bindings[0].Input]); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("binding not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
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
	if err = ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	// Pre-encode, then publish round-robin across metrics with a confirm window,
	// so the publisher is not the bottleneck.
	var bodies [][]byte
	var keys []string
	for begin := 0; begin < points; begin += chunkSize {
		for metric := 0; metric < metricCount; metric++ {
			c := &metricq.DataChunk{}
			var previous int64
			for j := begin; j < min(begin+chunkSize, points); j++ {
				timestamp := base + int64(j)*step
				c.TimeDelta = append(c.TimeDelta, timestamp-previous)
				c.Value = append(c.Value, float64(metric)+math.Sin(float64(j)/31))
				previous = timestamp
			}
			b, err := proto.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			bodies = append(bodies, b)
			keys = append(keys, fmt.Sprintf("%s.input.%d", id, metric))
		}
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, len(bodies)))
	syncsBefore, syncSumBefore := histogram(t, registry, "metricq_db_wal_sync_seconds")
	flushesBefore, flushSumBefore := histogram(t, registry, "metricq_db_flush_seconds")
	putsBefore, putSumBefore := histogram(t, registry, "metricq_db_store_put_seconds")
	started := time.Now()
	outstanding := 0
	for i, body := range bodies {
		if err = ch.PublishWithContext(ctx, "metricq.data", keys[i], true, false, amqp.Publishing{Body: body, DeliveryMode: amqp.Persistent}); err != nil {
			t.Fatal(err)
		}
		if outstanding++; outstanding >= 1024 {
			if c := <-confirms; !c.Ack {
				t.Fatal("publish not confirmed")
			}
			outstanding--
		}
	}
	for ; outstanding > 0; outstanding-- {
		if c := <-confirms; !c.Ack {
			t.Fatal("publish not confirmed")
		}
	}
	published := time.Since(started)
	for _, b := range bindings {
		for deadline := time.Now().Add(20 * time.Minute); ; {
			seen, err := lastValue(b.Name)
			if err == nil && seen == last {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not visible: %d %v", b.Name, seen, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	elapsed := time.Since(started)
	syncs, syncSum := histogram(t, registry, "metricq_db_wal_sync_seconds")
	flushes, flushSum := histogram(t, registry, "metricq_db_flush_seconds")
	puts, putSum := histogram(t, registry, "metricq_db_store_put_seconds")
	t.Logf("backpressure waits=%d, S3 PUTs=%d taking %.1fs", pressure.Load(), puts-putsBefore, putSum-putSumBefore)
	syncs -= syncsBefore
	syncSum -= syncSumBefore
	flushes -= flushesBefore
	flushSum -= flushSumBefore
	total := float64(points * metricCount)
	rate := total / elapsed.Seconds()
	t.Logf("mode=%s prefetch=%d: %.0f points/s (publish %.0f points/s), %.1fs, %d syncs (%.1f chunks/sync, %.2f ms), %d flushes (%.0f ms)", mode, prefetch, rate, total/published.Seconds(), elapsed.Seconds(), syncs, float64(len(bodies))/float64(max(syncs, 1)), 1000*syncSum/float64(max(syncs, 1)), flushes, 1000*flushSum/float64(max(flushes, 1)))
	return fmt.Sprintf("%s,%d,%d,%d,%d,%.3f,%.1f,%d,%.2f,%.3f,%d,%.1f", mode, prefetch, metricCount, points, chunkSize, elapsed.Seconds(), rate, syncs, float64(len(bodies))/float64(max(syncs, 1)), 1000*syncSum/float64(max(syncs, 1)), flushes, 1000*flushSum/float64(max(flushes, 1)))
}
