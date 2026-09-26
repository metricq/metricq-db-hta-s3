package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

func TestAggregateTailsAcrossCheckpoints(t *testing.T) {
	for _, step := range []int64{100, 1000000} {
		t.Run(fmt.Sprint(step), func(t *testing.T) {
			ctx := context.Background()
			s := newStore()
			e := openTest(t, s, t.TempDir())
			var firstRoot blob
			for batch := 0; batch < 35; batch++ {
				points := make([]hta.Point, 100)
				for i := range points {
					n := batch*100 + i + 1
					points[i] = hta.Point{Time: int64(n) * step, Value: float64(n % 17)}
				}
				ingest(t, e, points...)
				requests := []*metricq.HistoryRequest{
					{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: step, EndTime: points[99].Time},
					{Type: metricq.HistoryRequest_AGGREGATE_TIMELINE, StartTime: points[99].Time - 10*step + 10, EndTime: points[99].Time - 10, IntervalMax: 10000},
					{Type: metricq.HistoryRequest_AGGREGATE, StartTime: step + 10, EndTime: points[99].Time - 10},
				}
				before := make([]*metricq.HistoryResponse, len(requests))
				for i, req := range requests {
					before[i] = query(t, e, req)
				}
				if err := e.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if batch == 0 {
					firstRoot = e.state.Roots["x"][100]
				}
				for i, req := range requests {
					if got := query(t, e, req); !proto.Equal(got, before[i]) {
						t.Fatalf("checkpoint %d changed response %d", batch, i)
					}
				}
				// Recover using S3 alone after every flush, including partial tails.
				e.Close()
				e = openTest(t, s, t.TempDir())
				for i, req := range requests {
					if got := query(t, e, req); !proto.Equal(got, before[i]) {
						t.Fatalf("recovery %d changed response %d", batch, i)
					}
				}
			}
			for level, root := range e.state.Roots["x"] {
				if level == 0 {
					continue
				}
				var refs []blob
				if err := e.indexRange(ctx, root, 0, math.MaxInt64, &refs); err != nil {
					t.Fatal(err)
				}
				for i, ref := range refs {
					b, err := e.readBlob(ctx, ref)
					if err != nil {
						t.Fatal(err)
					}
					var rows []hta.Record
					if err := decode(b, &rows); err != nil {
						t.Fatal(err)
					}
					if len(rows) > maxDataBlockRecords || (i < len(refs)-1 && len(rows) != maxDataBlockRecords) {
						t.Fatalf("level %d block %d still fragmented: %d rows", level, i, len(rows))
					}
				}
				if step == 100 && level == 100 && len(refs) != 4 {
					t.Fatalf("got %d blocks, want 4", len(refs))
				}
			}
			// An older reader's root still addresses its original records.
			var old []blob
			if err := e.indexRange(ctx, firstRoot, 0, math.MaxInt64, &old); err != nil {
				t.Fatal(err)
			}
			if len(old) != 1 {
				t.Fatalf("old snapshot changed: %d blocks", len(old))
			}
			b, err := e.readBlob(ctx, old[0])
			if err != nil {
				t.Fatal(err)
			}
			var rows []hta.Record
			if err := decode(b, &rows); err != nil {
				t.Fatal(err)
			}
			if rows[len(rows)-1].LastTime() > 100*step {
				t.Fatal("old snapshot contains newer records")
			}
		})
	}
}

type tailFailureStore struct {
	*memoryStore
	getPrefix, putPrefix string
}

func (s *tailFailureStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	if s.getPrefix != "" && strings.HasPrefix(key, s.getPrefix) {
		return nil, "", fmt.Errorf("injected tail read failure")
	}
	return s.memoryStore.Get(ctx, key)
}

func (s *tailFailureStore) Put(ctx context.Context, key string, b []byte, v *string) (string, error) {
	if s.putPrefix != "" && strings.HasPrefix(key, s.putPrefix) {
		return "", fmt.Errorf("injected tail write failure")
	}
	return s.memoryStore.Put(ctx, key, b, v)
}

func TestFailedTailReplacementRetainsWAL(t *testing.T) {
	for _, failure := range []string{"read-data", "read-index", "data/", "index/", "manifest"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			s := &tailFailureStore{memoryStore: newStore()}
			dir := t.TempDir()
			e := openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 110, Value: 2}, hta.Point{Time: 230, Value: 4})
			if err := e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			e.Close()
			e = openTest(t, s, dir)
			ingest(t, e, hta.Point{Time: 440, Value: 8}, hta.Point{Time: 560, Value: 3})
			oldRoot, seq, walSize := e.state.Roots["x"][100], e.state.Sequence, e.wal.size
			if failure == "read-data" {
				s.getPrefix = "data/"
			} else if failure == "read-index" {
				s.getPrefix = "index/"
			} else {
				s.putPrefix = failure
			}
			if err := e.Flush(ctx); err == nil {
				t.Fatal("flush succeeded during outage")
			}
			if e.wal.size != walSize || e.state.Sequence != seq || e.state.Roots["x"][100] != oldRoot {
				t.Fatal("failed replacement published state or discarded WAL")
			}
			e.Close()
			s.getPrefix, s.putPrefix = "", ""
			e = openTest(t, s, dir)
			req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 600}
			before := query(t, e, req)
			if len(before.Value) != 4 || before.Value[3] != 3 {
				t.Fatal("ACKed samples lost on replay", before)
			}
			if err := e.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			e.Close()
			e = openTest(t, s, t.TempDir())
			if got := query(t, e, req); !proto.Equal(got, before) {
				t.Fatal("tail replacement changed recovered response")
			}
		})
	}
}
