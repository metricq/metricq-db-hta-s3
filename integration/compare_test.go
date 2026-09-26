//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/metricq/metricq-db-hta-go/engine"
	"github.com/metricq/metricq-db-hta-go/hta"
	"github.com/metricq/metricq-db-hta-go/storage"
	metricq "github.com/metricq/metricq-go"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func docker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	b, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %s: %v", args, b, err)
	}
	return strings.TrimSpace(string(b))
}
func couch(t *testing.T, method, path string, body any) map[string]any {
	t.Helper()
	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		payload = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, env("METRICQ_COUCHDB", "http://admin:admin@localhost:5984")+"/"+path, payload)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("couch %s %s: %s", method, path, b)
	}
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
func seed(t *testing.T, db, id string, body any) {
	t.Helper()
	path := db + "/" + url.PathEscape(id)
	couch(t, "PUT", path, body)
	t.Cleanup(func() {
		doc := couch(t, "GET", path, nil)
		couch(t, "DELETE", path+"?rev="+url.QueryEscape(doc["_rev"].(string)), nil)
	})
}
func newS3(t *testing.T, ctx context.Context, id string) (storage.Store, *s3.Client) {
	t.Helper()
	endpoint := env("METRICQ_TEST_S3", "http://localhost:19000")
	access := env("AWS_ACCESS_KEY_ID", "metricqtest")
	secret := env("AWS_SECRET_ACCESS_KEY", "metricqtestsecret")
	t.Setenv("AWS_ACCESS_KEY_ID", access)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")))
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = &endpoint; o.UsePathStyle = true })
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &id}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: &id})
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			for _, item := range page.Contents {
				if _, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &id, Key: item.Key}); err != nil {
					t.Error(err)
				}
			}
		}
		if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &id}); err != nil {
			t.Error(err)
		}
	})
	store, err := storage.NewS3(ctx, storage.S3Config{Bucket: id, Endpoint: endpoint, PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	// Fail early if a nominally S3-compatible backend ignores conditional writes.
	empty := ""
	version, err := store.Put(ctx, "cas-probe", []byte("one"), &empty)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Put(ctx, "cas-probe", []byte("bad"), &empty); err == nil {
		t.Fatal("S3 ignores If-None-Match")
	}
	if _, err = store.Put(ctx, "cas-probe", []byte("two"), &version); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Put(ctx, "cas-probe", []byte("bad"), &version); err == nil {
		t.Fatal("S3 ignores If-Match")
	}
	return store, client
}
func closeEnough(a, b float64) bool {
	return a == b || math.IsNaN(a) && math.IsNaN(b) || math.Abs(a-b) <= 1e-10*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}
func compare(a, b *metricq.HistoryResponse) error {
	if a.Error != b.Error || len(a.TimeDelta) != len(b.TimeDelta) || len(a.Value) != len(b.Value) || len(a.Aggregate) != len(b.Aggregate) {
		return fmt.Errorf("response shape differs")
	}
	for i := range a.TimeDelta {
		if a.TimeDelta[i] != b.TimeDelta[i] {
			return fmt.Errorf("timestamp %d differs", i)
		}
	}
	for i := range a.Value {
		if !closeEnough(a.Value[i], b.Value[i]) {
			return fmt.Errorf("value %d differs", i)
		}
	}
	for i, x := range a.Aggregate {
		y := b.Aggregate[i]
		if x.Count != y.Count || x.ActiveTime != y.ActiveTime {
			return fmt.Errorf("aggregate %d count/time differs", i)
		}
		for j, pair := range [][2]float64{{x.Minimum, y.Minimum}, {x.Maximum, y.Maximum}, {x.Sum, y.Sum}, {x.Integral, y.Integral}} {
			if !closeEnough(pair[0], pair[1]) {
				return fmt.Errorf("aggregate %d field %d differs", i, j)
			}
		}
	}
	return nil
}
func TestLegacyRequestParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	id := fmt.Sprintf("hta-go-test-%d", time.Now().UnixNano())
	server := env("METRICQ_AMQP", "amqp://admin:admin@localhost/")
	oldToken := "db-" + id + "-old"
	newToken := "db-" + id + "-new"
	// Remove only the dedicated queues created by this test.
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
		names := []string{oldToken + "-data", oldToken + "-hreq", newToken + "-data", newToken + "-hreq"}
		for _, name := range names {
			_, _ = ch.QueueDelete(name, false, false, false)
		}
	})
	realBackend, _ := newS3(t, ctx, id)
	backend := &outageStore{Store: realBackend}
	type dataset struct {
		name   string
		cfg    hta.Config
		points []hta.Point
	}
	const sec = int64(time.Second)
	base := int64(1700000000) * sec
	sets := []dataset{
		{"dense", hta.Config{IntervalMin: sec / 10, IntervalMax: 100 * sec, IntervalFactor: 10}, nil},
		{"irregular", hta.Config{IntervalMin: sec, IntervalMax: 100 * sec, IntervalFactor: 10}, []hta.Point{{Time: base + sec, Value: 2}, {Time: base + 3*sec, Value: -4}, {Time: base + 11*sec, Value: 8}, {Time: base + 11*sec, Value: 99}, {Time: base + 10*sec, Value: 99}, {Time: base + 12*sec, Value: math.NaN()}, {Time: base + 13*sec, Value: math.Inf(1)}, {Time: base + 44*sec, Value: 1}, {Time: base + 100*sec, Value: 3}}},
		{"daily", hta.Config{IntervalMin: 60 * sec, IntervalMax: 60000 * sec, IntervalFactor: 10}, []hta.Point{{Time: base + sec, Value: 4}, {Time: base + 86400*sec, Value: 2}, {Time: base + 3*86400*sec, Value: -1}}},
		{"single", hta.Config{IntervalMin: sec, IntervalMax: 100 * sec, IntervalFactor: 10}, []hta.Point{{Time: base + sec, Value: 2}}},
		{"empty", hta.Config{IntervalMin: sec, IntervalMax: 100 * sec, IntervalFactor: 10}, nil},
	}
	for i := int64(0); i < 1000; i++ {
		sets[0].points = append(sets[0].points, hta.Point{Time: base + i*sec/100 + sec/10, Value: math.Sin(float64(i)/7) * 20})
	}
	oldMetrics := map[string]any{}
	newMetrics := map[string]hta.Config{}
	inputs := map[string]string{}
	for _, d := range sets {
		input := id + ".input." + d.name
		oldName := id + ".old." + d.name
		newName := id + ".new." + d.name
		c := d.cfg
		c.Input = input
		newMetrics[newName] = c
		inputs[input] = newName
		oldMetrics[oldName] = map[string]any{"input": input, "mode": "RW", "interval_min": c.IntervalMin, "interval_max": c.IntervalMax, "interval_factor": c.IntervalFactor}
		for _, name := range []string{input, oldName, newName} {
			seed(t, "metadata", name, map[string]any{"description": "isolated parity test"})
		}
	}
	seed(t, "config", oldToken, map[string]any{"threads": 2, "type": "file", "path": "/tmp", "metrics": oldMetrics})
	seed(t, "config", newToken, map[string]any{"metrics": newMetrics})
	docker(t, "run", "-d", "--name", id, "--network", env("METRICQ_DOCKER_NETWORK", "metricq_metricq-network"), "--entrypoint", "/usr/bin/metricq-db-hta", env("METRICQ_LEGACY_IMAGE", "metricq-db-hta"), "--server", env("METRICQ_DOCKER_AMQP", "amqp://admin:admin@rabbitmq-server/"), "--token", oldToken)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(docker(t, "logs", id))
		}
		docker(t, "rm", "-f", id)
	})
	opts := engine.Options{WALDirectory: t.TempDir(), ObjectTarget: 4096, BuilderHard: 8 << 20, BackgroundMaintenance: true, AppendOnlyAggregates: true, Compaction: engine.CompactionOptions{Enabled: true, DeadFraction: .05, BytesPerSecond: 64 << 20, MaxBlocks: 512, MergeSmallBlocks: true}}
	var current *engine.Engine
	blocked := make(chan struct{})
	var blockedOnce sync.Once
	var stopDB context.CancelFunc
	var dbDone chan error
	startDB := func() {
		var err error
		current, err = engine.Open(ctx, backend, opts, newMetrics, nil)
		if err != nil {
			t.Fatal(err)
		}
		db, err := metricq.NewDB(newToken, server)
		if err != nil {
			t.Fatal(err)
		}
		dbCtx, stop := context.WithCancel(ctx)
		stopDB = stop
		dbDone = make(chan error, 1)
		done := dbDone
		activeEngine := current
		go func() {
			done <- db.Run(dbCtx, metricq.DBHandlers{Configure: func(context.Context, json.RawMessage) ([]metricq.DBBinding, error) {
				bindings := []metricq.DBBinding{}
				for input, name := range inputs {
					bindings = append(bindings, metricq.DBBinding{Name: name, Input: input})
				}
				return bindings, nil
			}, Data: func(ctx context.Context, input string, c *metricq.DataChunk) error {
				for {
					err := activeEngine.Ingest(ctx, inputs[input], c)
					if !errors.Is(err, engine.ErrPressure) {
						return err
					}
					blockedOnce.Do(func() { close(blocked) })
					if err = activeEngine.Flush(ctx); err != nil {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(100 * time.Millisecond):
						}
					}
				}
			}, History: activeEngine.Query})
		}()
	}
	stop := func() {
		stopDB()
		select {
		case <-dbDone:
		case <-time.After(10 * time.Second):
			t.Fatal("DB did not stop")
		}
		current.Close()
		// RabbitMQ deletes the exclusive management RPC queue after closing
		// the connection. Give that deletion time before reusing this token.
		time.Sleep(time.Second)
	}
	startDB()
	t.Cleanup(func() { stop() })
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
	// Wait for history bindings, then use empty LAST_VALUE replies as readiness.
	request := func(metric string, r *metricq.HistoryRequest) (*metricq.HistoryResponse, error) {
		callCtx, done := context.WithTimeout(ctx, 2*time.Second)
		defer done()
		resp, _, err := history.Request(callCtx, metric, time.Unix(0, r.StartTime), time.Unix(0, r.EndTime), time.Duration(r.IntervalMax), r.Type)
		return resp, err
	}
	waitReady := func(name string, wantTime int64) {
		deadline := time.Now().Add(30 * time.Second)
		for {
			r, err := request(name, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
			if err == nil && (wantTime == 0 || len(r.TimeDelta) == 1 && r.TimeDelta[0] == wantTime) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not ready: %v %v", name, r, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitReady(id+".old.empty", 0)
	waitReady(id+".new.empty", 0)
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
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	for _, d := range sets {
		for begin := 0; begin < len(d.points); begin += 73 {
			end := min(begin+73, len(d.points))
			c := &metricq.DataChunk{}
			var prev int64
			for _, p := range d.points[begin:end] {
				c.TimeDelta = append(c.TimeDelta, p.Time-prev)
				c.Value = append(c.Value, p.Value)
				prev = p.Time
			}
			b, _ := proto.Marshal(c)
			if err = ch.PublishWithContext(ctx, "metricq.data", id+".input."+d.name, true, false, amqp.Publishing{Body: b, DeliveryMode: amqp.Persistent}); err != nil {
				t.Fatal(err)
			}
			select {
			case c := <-confirms:
				if !c.Ack {
					t.Fatal("publish not confirmed")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		if len(d.points) > 0 {
			last := d.points[len(d.points)-1].Time
			waitReady(id+".old."+d.name, last)
			waitReady(id+".new."+d.name, last)
		}
	}
	requests := map[string][]*metricq.HistoryRequest{}
	for _, d := range sets {
		duration := int64(120) * sec
		if d.name == "daily" {
			duration = 4 * 86400 * sec
		}
		spans := [][2]int64{{base, base + duration}, {base + sec, base + 11*sec}, {base + sec + 1, base + 11*sec - 1}, {base + duration, base + 2*duration}, {base - 10*sec, base}, {base + 11*sec, base + 11*sec + 1}}
		resolutions := []int64{0, sec / 100, sec / 2, sec, 3 * sec, 10 * sec, 100 * sec, 6000 * sec, -1}
		requests[d.name] = append(requests[d.name], &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
		for _, span := range spans {
			requests[d.name] = append(requests[d.name], &metricq.HistoryRequest{Type: metricq.HistoryRequest_AGGREGATE, StartTime: span[0], EndTime: span[1]})
			for _, resolution := range resolutions {
				for _, typ := range []metricq.HistoryRequest_RequestType{metricq.HistoryRequest_FLEX_TIMELINE, metricq.HistoryRequest_AGGREGATE_TIMELINE} {
					requests[d.name] = append(requests[d.name], &metricq.HistoryRequest{Type: typ, StartTime: span[0], EndTime: span[1], IntervalMax: resolution})
				}
			}
		}
	}
	comparisons := 0
	for _, phase := range []string{"hot", "WAL-restart", "S3-restart", "compacted", "compacted-S3-restart"} {
		if phase == "WAL-restart" {
			stop()
			startDB()
			waitReady(id+".new.dense", sets[0].points[len(sets[0].points)-1].Time)
		}
		if phase == "compacted" {
			if err = current.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				if err = current.CompactOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}
		if phase == "S3-restart" || phase == "compacted-S3-restart" {
			if err = current.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			stop()
			opts.WALDirectory = t.TempDir()
			startDB()
			waitReady(id+".new.dense", sets[0].points[len(sets[0].points)-1].Time)
		}
		t.Run(phase, func(t *testing.T) {
			for _, d := range sets {
				for _, r := range requests[d.name] {
					a, err := request(id+".old."+d.name, r)
					if err != nil {
						t.Fatalf("legacy request %s %v: %v", d.name, r, err)
					}
					b, err := request(id+".new."+d.name, r)
					if err != nil {
						t.Fatalf("Go request %s %v: %v", d.name, r, err)
					}
					if err = compare(a, b); err != nil {
						t.Fatalf("%s %v: %v\nlegacy: %v\nGo: %v", d.name, r, err, a, b)
					}
					comparisons++
				}
			}
		})
		if t.Failed() {
			break
		}
	}

	if !t.Failed() {
		t.Run("S3-outage-backpressure", func(t *testing.T) {
			stop()
			opts.WALTarget = 2000
			opts.WALHigh = 4000
			opts.WALHard = 6000
			startDB()
			waitReady(id+".new.dense", sets[0].points[len(sets[0].points)-1].Time)
			backend.fail.Store(true)
			defer backend.fail.Store(false)
			last := sets[0].points[len(sets[0].points)-1].Time
			// QueueInspect reports ready messages only, excluding unacked
			// deliveries. Exceed the data consumer's prefetch of 400 so that
			// durable backpressure also leaves a visible ready backlog.
			const outageDeliveries = int64(1000)
			for i := int64(1); i <= outageDeliveries; i++ {
				b, _ := proto.Marshal(&metricq.DataChunk{TimeDelta: []int64{last + i*sec/outageDeliveries}, Value: []float64{float64(i)}})
				if err = ch.PublishWithContext(ctx, "metricq.data", id+".input.dense", true, false, amqp.Publishing{Body: b, DeliveryMode: amqp.Persistent}); err != nil {
					t.Fatal(err)
				}
				select {
				case c := <-confirms:
					if !c.Ack {
						t.Fatal("publish not confirmed")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case <-blocked:
			case <-time.After(10 * time.Second):
				t.Fatal("WAL did not apply pressure")
			}
			queue, err := ch.QueueInspect(newToken + "-data")
			if err != nil {
				t.Fatal(err)
			}
			if queue.Messages == 0 {
				t.Fatal("RabbitMQ did not retain the storage outage backlog")
			}
			r, err := request(id+".new.dense", &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
			if err != nil {
				t.Fatalf("history blocked by ingest pressure: %v", err)
			}
			if r.TimeDelta[0] >= last+sec {
				t.Fatal("ingestion exceeded WAL limit during outage")
			}
			backend.fail.Store(false)
			waitReady(id+".new.dense", last+sec)
			waitReady(id+".old.dense", last+sec)
			if err = current.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			stop()
			opts.WALDirectory = t.TempDir()
			startDB()
			waitReady(id+".new.dense", last+sec)
			req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_AGGREGATE, StartTime: last - sec, EndTime: last + sec}
			a, err := request(id+".old.dense", req)
			if err != nil {
				t.Fatal(err)
			}
			b, err := request(id+".new.dense", req)
			if err != nil {
				t.Fatal(err)
			}
			if err = compare(a, b); err != nil {
				t.Fatalf("post-outage parity: %v\nold %v\nnew %v", err, a, b)
			}
		})
	}
	t.Logf("Compared %d legacy/Go response pairs, including all four request types, smoothing, boundaries, gaps and recovery", comparisons)

}

type outageStore struct {
	storage.Store
	fail atomic.Bool
}

func (s *outageStore) Put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	if s.fail.Load() {
		return "", fmt.Errorf("injected S3 outage")
	}
	return s.Store.Put(ctx, key, b, v)
}
