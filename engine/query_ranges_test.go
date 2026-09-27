package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

func TestColdFLEXCoalescesBlocksAndSkipsAggregateNeighbors(t *testing.T) {
	ctx := context.Background()
	s := &countedRangeStore{memoryStore: newStore()}
	e, err := Open(ctx, s, Options{WALDirectory: t.TempDir(), BuilderHard: 64 << 20}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	c := &metricq.DataChunk{}
	for i := 0; i < 24*maxDataBlockRecords; i++ {
		c.TimeDelta = append(c.TimeDelta, 100)
		c.Value = append(c.Value, float64(i%7))
	}
	if err = e.Ingest(ctx, "x", c); err != nil {
		t.Fatal(err)
	}
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 100, EndTime: 2300000, IntervalMax: 100}
	want := query(t, e, req) // In-memory reference, before any index exists.
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	e.sharedBlocks = nil
	e.sharedNodes = nil
	s.dataRanges.Store(0)
	got := query(t, e, req)
	if !proto.Equal(want, got) {
		t.Fatal("coalescing changed FLEX response")
	}
	if reads := s.dataRanges.Load(); reads != 1 {
		t.Fatalf("cold 23-block FLEX made %d data GETs, want 1", reads)
	}
	entries, err := e.indexEntriesAfter(ctx, e.state.Roots["x"][100], 0, 30)
	if err != nil || len(entries) < 3 {
		t.Fatalf("fixture: %v", err)
	}
	// Query starts inside the LAST bucket of a block. Rounding down is needed
	// to include it; adjacent blocks must neither be fetched nor returned.
	target := entries[1]
	s.dataRanges.Store(0)
	s.dataBytes.Store(0)
	got = query(t, e, &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: target.Last + 1, EndTime: target.Last + 99, IntervalMax: 100})
	if len(got.Aggregate) != 1 || got.TimeDelta[0] != target.Last {
		t.Fatalf("lost bucket containing begin: %v", got)
	}
	if s.dataRanges.Load() != 1 || s.dataBytes.Load() != target.Blob.Length {
		t.Fatalf("unnecessary neighbor bytes: reads=%d bytes=%d want=%d", s.dataRanges.Load(), s.dataBytes.Load(), target.Blob.Length)
	}
}

func TestCoalescedRangesPreserveOrderBoundariesAndChecksums(t *testing.T) {
	for _, tc := range []struct {
		name      string
		gap       int
		corrupt   bool
		wantReads int64
	}{
		{"adjacent", 0, false, 1},
		{"small-gap", maxCoalescedGap, false, 1},
		{"large-gap", maxCoalescedGap + 1, false, 2},
		{"checksum", 8, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &countedRangeStore{memoryStore: newStore()}
			e := openTest(t, s, t.TempDir())
			e.sharedBlocks = nil
			p, err := newPack("data")
			if err != nil {
				t.Fatal(err)
			}
			var refs []blob
			for i := 0; i < 2; i++ {
				b, err := encode([]hta.Record{{Time: int64(i + 1), Value: float64(i + 1)}})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, p.add(b))
				if i == 0 {
					p.buf.Write(make([]byte, tc.gap))
				}
			}
			if tc.corrupt {
				p.buf.Bytes()[refs[1].Offset] ^= 1
			}
			if _, err = s.Put(context.Background(), p.key, p.buf.Bytes(), nil); err != nil {
				t.Fatal(err)
			}
			q := reader{e: e, ctx: context.Background(), cache: map[blob][]hta.Record{}}
			blocks, err := q.fetchBlocks([]blob{refs[1], refs[0]})
			if tc.corrupt {
				if err == nil || !strings.Contains(err.Error(), "checksum") {
					t.Fatalf("corrupt block accepted: %v", err)
				}
				if len(q.cache) != 0 {
					t.Fatal("partial corrupt result cached")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if blocks[0][0].Value != 2 || blocks[1][0].Value != 1 {
					t.Fatal("physical sorting changed logical order")
				}
			}
			if s.dataRanges.Load() != tc.wantReads {
				t.Fatalf("GETs=%d want=%d", s.dataRanges.Load(), tc.wantReads)
			}
		})
	}
}
