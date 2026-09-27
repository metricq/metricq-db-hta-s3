package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

func defaultCompressedFixture(t *testing.T, value any) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out) // The original default compression level.
	if err := gob.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestDefaultCompressionRecoveryAndBestSpeedCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	points := []hta.Point{{Time: 110, Value: 2}, {Time: 230, Value: 4}, {Time: 440, Value: 8}}
	ingest(t, e, points...)
	// Construct a complete checkpoint with the previous compressor, including
	// independently compressed data blocks, index pages and the manifest.
	groups := make(map[int64][]hta.Record)
	for _, stream := range e.pending.sorted() {
		groups[stream.level] = stream.records
	}
	data, _ := newPack("data")
	index, _ := newPack("index")
	for level, rows := range groups {
		ref := data.add(defaultCompressedFixture(t, rows))
		node := indexNode{Leaf: true, Entries: []indexEntry{{First: rows[0].Time, Last: rows[len(rows)-1].LastTime(), Blob: ref, Records: len(rows)}}}
		e.state.Roots["x"][level] = index.add(defaultCompressedFixture(t, node))
	}
	for _, p := range []*pack{data, index} {
		if _, err := s.Put(ctx, p.key, p.buf.Bytes(), nil); err != nil {
			t.Fatal(err)
		}
	}
	e.state.Sequence = e.sequence
	if _, err := s.Put(ctx, "manifest", defaultCompressedFixture(t, e.state), nil); err != nil {
		t.Fatal(err)
	}
	last := hta.Point{Time: 560, Value: 3}
	legacyBatch := batch{Metric: "x", Config: e.state.Series["x"].Config, Points: []hta.Point{last}, ReceivedAt: 1}
	if err := e.wal.append(e.sequence+1, defaultCompressedFixture(t, legacyBatch)); err != nil {
		t.Fatal(err)
	}
	e.Close()
	// This engine must read old manifest/index/data streams and replay an old
	// WAL frame, then extend those aggregate tails with BestSpeed streams.
	e = openTest(t, s, dir)
	reference := openTest(t, newStore(), t.TempDir())
	ingest(t, reference, append(points, last)...)
	requests := []*metricq.HistoryRequest{
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, EndTime: 600},
		{Type: metricq.HistoryRequest_AGGREGATE_TIMELINE, EndTime: 600, IntervalMax: 100},
		{Type: metricq.HistoryRequest_AGGREGATE, StartTime: 110, EndTime: 560},
	}
	for _, req := range requests {
		if !proto.Equal(query(t, e, req), query(t, reference, req)) {
			t.Fatal("old compressed checkpoint/WAL changed response")
		}
	}
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e = openTest(t, s, t.TempDir())
	for _, req := range requests {
		if !proto.Equal(query(t, e, req), query(t, reference, req)) {
			t.Fatal("mixed compression levels changed response")
		}
	}
}
