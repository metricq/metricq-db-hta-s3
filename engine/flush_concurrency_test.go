package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
)

// putGateStore blocks the first PUT with the given prefix until released.
type putGateStore struct {
	*memoryStore
	prefix  string
	entered chan struct{}
	release chan struct{}
}

func (s *putGateStore) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	if s.prefix != "" && strings.HasPrefix(key, s.prefix) {
		s.prefix = ""
		close(s.entered)
		<-s.release
	}
	return s.memoryStore.Put(ctx, key, b, expected)
}

func walFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "ingest.wal*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestIngestAndQueriesContinueDuringFlushUpload(t *testing.T) {
	for _, prefix := range []string{"data/", "state/", "roots/", "manifest"} {
		t.Run(prefix, func(t *testing.T) {
			ctx := context.Background()
			s := &putGateStore{memoryStore: newStore(), prefix: prefix, entered: make(chan struct{}), release: make(chan struct{})}
			dir := t.TempDir()
			e := openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 100, Value: 1}, hta.Point{Time: 150, Value: 2})
			flushed := make(chan error, 1)
			go func() { flushed <- e.Flush(ctx) }()
			<-s.entered
			// The upload is blocked: new deliveries and queries must not wait for it.
			done := make(chan error, 1)
			go func() { done <- e.Ingest(ctx, "x", chunk(hta.Point{Time: 250, Value: 3})) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("flush upload blocked ingestion")
			}
			answered := make(chan *metricq.HistoryResponse, 1)
			go func() {
				r, err := e.Query(ctx, "x", &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000})
				if err != nil {
					t.Error(err)
				}
				answered <- r
			}()
			select {
			case r := <-answered:
				// Frozen and newer records are both visible during the upload.
				if len(r.Value) != 3 {
					t.Fatalf("query during flush saw %v", r)
				}
			case <-time.After(time.Second):
				t.Fatal("flush upload blocked queries")
			}
			if len(walFiles(t, dir)) != 2 {
				t.Fatalf("expected frozen and active WAL segments: %v", walFiles(t, dir))
			}
			close(s.release)
			if err := <-flushed; err != nil {
				t.Fatal(err)
			}
			if e.state.Sequence != 1 || e.sequence != 2 || e.pending.len() == 0 || e.flushing.len() != 0 {
				t.Fatalf("checkpoint %d head %d pending %d flushing %d", e.state.Sequence, e.sequence, e.pending.len(), e.flushing.len())
			}
			if files := walFiles(t, dir); len(files) != 1 || e.wal.total() == 0 {
				t.Fatalf("committed segment not released or newer frame lost: %v", files)
			}
			if err := e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if e.wal.total() != 0 {
				t.Fatal("second checkpoint left WAL bytes")
			}
			r := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000})
			if len(r.Value) != 3 {
				t.Fatalf("stored history %v", r)
			}
		})
	}
}

func TestFailedFlushKeepsSegmentsForRetryAndReplay(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	s.fail = "manifest"
	if err := e.Flush(ctx); err == nil {
		t.Fatal("flush succeeded during outage")
	}
	ingest(t, e, hta.Point{Time: 200, Value: 2})
	if err := e.Flush(ctx); err == nil {
		t.Fatal("flush succeeded during outage")
	}
	ingest(t, e, hta.Point{Time: 300, Value: 3})
	// Two frozen segments and one active segment, none published.
	if files := walFiles(t, dir); len(files) != 3 {
		t.Fatalf("WAL segments %v", files)
	}
	want := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000})
	if len(want.Value) != 3 {
		t.Fatalf("failed flushes hid records: %v", want)
	}
	// Crash, then replay all segments in order.
	e.Close()
	s.fail = ""
	if err := os.WriteFile(filepath.Join(dir, "ingest.wal.new"), nil, 0o600); err != nil {
		t.Fatal(err) // leftover of an interrupted rotation
	}
	e = openTest(t, s, dir)
	if e.sequence != 3 || e.state.Sequence != 0 {
		t.Fatalf("replayed head %d checkpoint %d", e.sequence, e.state.Sequence)
	}
	if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000}); len(got.Value) != 3 {
		t.Fatalf("replay lost records: %v", got)
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if files := walFiles(t, dir); len(files) != 1 || e.wal.total() != 0 {
		t.Fatalf("published segments not released: %v", files)
	}
	e.Close()
	e = openTest(t, s, dir)
	if got := query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 1000}); len(got.Value) != 3 || e.sequence != 3 {
		t.Fatalf("restart after checkpoint: %v sequence %d", got, e.sequence)
	}
}
