package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

type memoryStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	versions  map[string]string
	next      int
	fail      string
	loseReply bool
}

func newStore() *memoryStore {
	return &memoryStore{objects: map[string][]byte{}, versions: map[string]string{}}
}
func (s *memoryStore) Get(_ context.Context, k string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.objects[k]
	if !ok {
		return nil, "", storage.ErrNotFound
	}
	return append([]byte(nil), v...), s.versions[k], nil
}
func (s *memoryStore) Put(_ context.Context, k string, b []byte, expected *string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail == "all" || s.fail == k {
		return "", fmt.Errorf("injected outage")
	}
	if expected != nil && *expected != s.versions[k] {
		return "", storage.ErrConflict
	}
	s.next++
	v := fmt.Sprint(s.next)
	s.objects[k] = append([]byte(nil), b...)
	s.versions[k] = v
	if s.loseReply && k == "manifest" {
		return "", fmt.Errorf("lost response")
	}
	return v, nil
}

var testConfig = map[string]hta.Config{"x": {IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10}}

func openTest(t *testing.T, s storage.Store, dir string) *Engine {
	t.Helper()
	e, err := Open(context.Background(), s, Options{WALDirectory: dir, CheckpointUnsavedBytes: 300, IngestMemoryLimitBytes: 100000}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}
func chunk(points ...hta.Point) *metricq.DataChunk {
	c := &metricq.DataChunk{}
	var last int64
	for _, p := range points {
		c.TimeDelta = append(c.TimeDelta, p.Time-last)
		c.Value = append(c.Value, p.Value)
		last = p.Time
	}
	return c
}
func ingest(t *testing.T, e *Engine, points ...hta.Point) {
	t.Helper()
	if err := e.Ingest(context.Background(), "x", chunk(points...)); err != nil {
		t.Fatal(err)
	}
}
func query(t *testing.T, e *Engine, req *metricq.HistoryRequest) *metricq.HistoryResponse {
	t.Helper()
	r, err := e.Query(context.Background(), "x", req)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestDurabilityAndQueries(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	ingest(t, e, hta.Point{Time: 110, Value: 2}, hta.Point{Time: 120, Value: 4}, hta.Point{Time: 200, Value: 8}, hta.Point{Time: 230, Value: -2}, hta.Point{Time: 400, Value: 3})
	requests := []*metricq.HistoryRequest{
		{Type: metricq.HistoryRequest_LAST_VALUE},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 115, EndTime: 400},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 400, IntervalMax: 100},
		{Type: metricq.HistoryRequest_AGGREGATE_TIMELINE, StartTime: 115, EndTime: 400, IntervalMax: 0},
		{Type: metricq.HistoryRequest_AGGREGATE, StartTime: 110, EndTime: 400},
	}
	before := make([]*metricq.HistoryResponse, len(requests))
	for i, r := range requests {
		before[i] = query(t, e, r)
	}
	agg := before[4].Aggregate[0]
	if agg.Count != 4 || agg.ActiveTime != 290 || agg.Sum != 12 || agg.Integral != 1130 {
		t.Fatalf("unexpected aggregate: %v", agg)
	}
	if before[1].TimeDelta[0] != 110 {
		t.Fatal("extended left boundary lost")
	}
	e.Close()
	e = openTest(t, s, dir)
	for i, r := range requests {
		if got := query(t, e, r); !proto.Equal(got, before[i]) {
			t.Fatalf("WAL replay changed response: %v vs %v", got, before[i])
		}
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if e.wal.total() != 0 {
		t.Fatal("WAL not reclaimed")
	}
	e.Close()
	e = openTest(t, s, t.TempDir())
	for i, r := range requests {
		if got := query(t, e, r); !proto.Equal(got, before[i]) {
			t.Fatalf("S3 recovery changed response: %v vs %v", got, before[i])
		}
	}
	ingest(t, e, hta.Point{Time: 400, Value: 99}, hta.Point{Time: 390, Value: 99}, hta.Point{Time: 450, Value: math.NaN()}, hta.Point{Time: 460, Value: math.Inf(1)}, hta.Point{Time: 500, Value: 5})
	if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE}); got.Value[0] != 5 {
		t.Fatal(got)
	}
}
func TestManifestFailureRetainsWAL(t *testing.T) {
	for _, failure := range []string{"all", "manifest", "lost"} {
		t.Run(failure, func(t *testing.T) {
			s := newStore()
			dir := t.TempDir()
			e := openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 110, Value: 2}, hta.Point{Time: 230, Value: 4})
			s.fail = failure
			s.loseReply = failure == "lost"
			err := e.Flush(context.Background())
			if failure == "lost" {
				if err != nil || e.wal.total() != 0 {
					t.Fatalf("lost successful reply not reconciled: %v", err)
				}
			} else {
				if err == nil || e.wal.total() == 0 {
					t.Fatal("failed commit discarded WAL")
				}
			}
			e.Close()
			s.fail = ""
			s.loseReply = false
			e = openTest(t, s, dir)
			r := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
			if r.Value[0] != 4 {
				t.Fatal(r)
			}
			if err = e.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPressureAndRecovery(t *testing.T) {
	s := newStore()
	e, err := Open(context.Background(), s, Options{WALDirectory: t.TempDir(), WALTarget: 500, WALHigh: 1000, WALHard: 1500, CheckpointUnsavedBytes: 100, IngestMemoryLimitBytes: 10000}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := int64(1); ; i++ {
		err = e.Ingest(context.Background(), "x", chunk(hta.Point{Time: i, Value: 2}))
		if errors.Is(err, ErrPressure) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if i > 100 {
			t.Fatal("no backpressure")
		}
	}
	if e.wal.total() > e.options.WALHard {
		t.Fatal("hard limit exceeded")
	}
	seq := e.sequence
	s.fail = "all"
	if err = e.Flush(context.Background()); err == nil {
		t.Fatal("expected outage")
	}
	if e.sequence != seq {
		t.Fatal("unacknowledged sample applied")
	}
	s.fail = ""
	if err = e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	ingest(t, e, hta.Point{Time: 999, Value: 4})
}
func TestWALTailAndCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			s := newStore()
			dir := t.TempDir()
			e := openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 100, Value: 1})
			e.Close()
			path := filepath.Join(dir, "ingest.wal")
			f, err := os.OpenFile(path, os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if corrupt {
				_, err = f.WriteAt([]byte{255}, frameHeader+5)
			} else {
				st, _ := f.Stat()
				_, err = f.WriteAt([]byte{1, 2, 3}, st.Size())
			}
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil)
			if err == nil {
				recovered.Close()
				t.Fatal("accepted incomplete or corrupted WAL")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("startup removed WAL evidence")
			}
		})
	}
}
func TestAcknowledgedWALContainsOnlyAcceptedPoints(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	input := chunk(
		hta.Point{Time: 100, Value: 1},
		hta.Point{Time: 100, Value: 99},
		hta.Point{Time: 90, Value: 99},
		hta.Point{Time: 200, Value: math.NaN()},
		hta.Point{Time: 300, Value: 3},
	)
	if err := e.Ingest(context.Background(), "x", input); err != nil {
		t.Fatal(err)
	}
	frames := 0
	if err := e.wal.replay(func(seq uint64, data []byte) error {
		frames++
		var stored batch
		if err := decode(data, &stored); err != nil {
			return err
		}
		if len(stored.Points) != 2 || stored.Points[0].Time != 100 || stored.Points[1].Time != 300 {
			t.Fatalf("WAL includes deliberately discarded points: %+v", stored.Points)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if frames != 1 {
		t.Fatalf("got %d WAL frames", frames)
	}
	e.Close()
	e = openTest(t, s, dir)
	response := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 400})
	if len(response.Value) != 2 || response.Value[0] != 1 || response.Value[1] != 3 {
		t.Fatalf("replay did not preserve accepted points: %v", response)
	}
}
func TestWALWriteFailureCannotBeAcknowledged(t *testing.T) {
	e := openTest(t, newStore(), t.TempDir())
	before := *e.state.Series["x"]
	before.Levels = make(map[int64]hta.Level)
	for k, v := range e.state.Series["x"].Levels {
		before.Levels[k] = v
	}
	if err := e.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Ingest(context.Background(), "x", chunk(hta.Point{Time: 100, Value: 1})); err == nil {
		t.Fatal("ingestion succeeded without durable WAL write")
	}
	if e.sequence != 0 || e.pending.len() != 0 || !reflect.DeepEqual(before, *e.state.Series["x"]) {
		t.Fatal("failed WAL write advanced database state")
	}
}

func TestRejectedIngestPlanDoesNotChangeState(t *testing.T) {
	e := openTest(t, newStore(), t.TempDir())
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	before := *e.state.Series["x"]
	before.Levels = make(map[int64]hta.Level)
	for k, v := range e.state.Series["x"].Levels {
		before.Levels[k] = v
	}
	seq, size, pending, pendingBytes := e.sequence, e.wal.total(), e.pending.len(), e.pendingBytes
	e.options.IngestMemoryLimitBytes = pendingBytes + 1
	if err := e.Ingest(context.Background(), "x", chunk(hta.Point{Time: 200, Value: 2})); err == nil {
		t.Fatal("accepted over-capacity delivery")
	}
	if e.sequence != seq || e.wal.total() != size || e.pending.len() != pending || e.pendingBytes != pendingBytes || !reflect.DeepEqual(before, *e.state.Series["x"]) {
		t.Fatal("rejected preparation changed live state")
	}
	e.options.IngestMemoryLimitBytes = 100000
	ingest(t, e, hta.Point{Time: 200, Value: 2})
	if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 300}); len(got.Value) != 2 {
		t.Fatal("rejected point could not be ingested later")
	}
}

func TestReplayRejectsUnprocessableDurablePoint(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	payload, err := encode(batch{Config: testConfig["x"], Metric: "x", Points: []hta.Point{{Time: 100, Value: 99}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.wal.append(2, payload); err != nil {
		t.Fatal(err)
	}
	e.Close()
	path := filepath.Join(dir, "ingest.wal")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil)
	if err == nil {
		recovered.Close()
		t.Fatal("silently discarded a durable, unprocessable point")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed recovery changed durable WAL bytes")
	}
}
func TestManifestCommittedBeforeWALTruncate(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	ingest(t, e, hta.Point{Time: 100, Value: 1}, hta.Point{Time: 200, Value: 2})
	old, err := os.ReadFile(filepath.Join(dir, "ingest.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.Close()
	if err = os.WriteFile(filepath.Join(dir, "ingest.wal"), old, 0600); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, s, dir)
	if e.pendingBytes != 0 || e.sequence != 1 {
		t.Fatal("replayed committed data")
	}
	ingest(t, e, hta.Point{Time: 300, Value: 3})
	if err = e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestLocksAndConcurrentWriter(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	a := openTest(t, s, dir)
	if b, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil); err == nil {
		b.Close()
		t.Fatal("second local writer accepted")
	}
	b := openTest(t, s, t.TempDir())
	ingest(t, a, hta.Point{Time: 100, Value: 1})
	ingest(t, b, hta.Point{Time: 100, Value: 2})
	if err := a.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(context.Background()); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected writer conflict: %v", err)
	}
	if b.wal.total() == 0 {
		t.Fatal("lost conflicting writer WAL")
	}
}
func TestPrometheusAndQueryLimits(t *testing.T) {
	r := prometheus.NewRegistry()
	m := NewMetrics(r)
	e, err := Open(context.Background(), newStore(), Options{WALDirectory: t.TempDir(), QueryMaxRows: 2}, testConfig, m)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ingest(t, e, hta.Point{Time: 100, Value: 1}, hta.Point{Time: 200, Value: 2}, hta.Point{Time: 300, Value: 3})
	_, err = e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 400})
	if err == nil {
		t.Fatal("query limit ignored")
	}
	fs, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range fs {
		if f.GetName() == "metricq_db_wal_bytes" {
			found = true
			if f.Metric[0].Gauge.GetValue() <= 0 {
				t.Fatal("WAL gauge empty")
			}
		}
	}
	if !found {
		t.Fatal("missing WAL gauge")
	}
}

func (*memoryStore) Identity() string { return "memory:test" }

func TestInputBindingDoesNotChangeStoredMetricOrWALReplay(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	oldInput := map[string]hta.Config{"x": {Input: "x.downsampled.old", IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10}}
	newInput := map[string]hta.Config{"x": {Input: "x.downsampled.new", IntervalMin: 100, IntervalMax: 10000, IntervalFactor: 10}}
	e, err := Open(context.Background(), s, Options{WALDirectory: dir}, oldInput, nil)
	if err != nil {
		t.Fatal(err)
	}
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(context.Background(), s, Options{WALDirectory: dir}, newInput, nil)
	if err != nil {
		t.Fatalf("input binding change blocked WAL replay: %v", err)
	}
	if e.state.Series["x"].Config.Input != "" {
		t.Fatal("input binding persisted in aggregation config")
	}
	ingest(t, e, hta.Point{Time: 200, Value: 2})
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(context.Background(), s, Options{WALDirectory: dir}, newInput, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	response := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 300})
	if len(response.Value) != 2 || response.Value[0] != 1 || response.Value[1] != 2 {
		t.Fatalf("renamed input changed history under x: %v", response.Value)
	}
}

func TestWALConfigurationAndCheckpointGuard(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	e.Close()
	changed := map[string]hta.Config{"x": {IntervalMin: 200, IntervalMax: 10000, IntervalFactor: 10}}
	if other, err := Open(context.Background(), s, Options{WALDirectory: dir}, changed, nil); err == nil {
		other.Close()
		t.Fatal("replayed WAL with different HTA config")
	}
	e = openTest(t, s, dir)
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.Close()
	delete(s.objects, "manifest")
	delete(s.versions, "manifest")
	if other, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil); err == nil {
		other.Close()
		t.Fatal("accepted missing manifest after WAL GC")
	}
}
func TestHeaderCorruptionAndNamespaceGuard(t *testing.T) {
	for _, namespace := range []bool{false, true} {
		t.Run(fmt.Sprint(namespace), func(t *testing.T) {
			s := newStore()
			dir := t.TempDir()
			e := openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 100, Value: 1})
			e.Close()
			if namespace {
				if err := os.WriteFile(filepath.Join(dir, "identity"), []byte("other"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				f, err := os.OpenFile(filepath.Join(dir, "ingest.wal"), os.O_RDWR, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte{255}, 9)
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if other, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil); err == nil {
				other.Close()
				t.Fatal("accepted wrong namespace or header corruption")
			}
		})
	}
}
func TestCorruptObjectRejected(t *testing.T) {
	s := newStore()
	e := openTest(t, s, t.TempDir())
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for key := range s.objects {
		if key != "manifest" {
			s.objects[key] = []byte("corruption")
		}
	}
	_, err := e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000})
	if err == nil {
		t.Fatal("accepted corrupted segment")
	}
}

type slowStore struct {
	*memoryStore
	entered, release chan struct{}
}

func (s *slowStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	if key != "manifest" {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return s.memoryStore.Get(ctx, key)
}
func TestHistoricalGETDoesNotBlockIngestion(t *testing.T) {
	s := &slowStore{newStore(), make(chan struct{}, 1), make(chan struct{})}
	e := openTest(t, s, t.TempDir())
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan *metricq.HistoryResponse, 1)
	fail := make(chan error, 1)
	go func() {
		r, err := e.Query(context.Background(), "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000})
		result <- r
		fail <- err
	}()
	<-s.entered
	done := make(chan error, 1)
	go func() { done <- e.Ingest(context.Background(), "x", chunk(hta.Point{Time: 200, Value: 2})) }()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("S3 GET blocked ingestion")
	}
	close(s.release)
	r := <-result
	if err := <-fail; err != nil {
		t.Fatal(err)
	}
	if len(r.Value) != 1 || r.Value[0] != 1 {
		t.Fatalf("query snapshot changed under ingestion: %v", r)
	}
}
