package engine

import (
	"bytes"
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

// The direct encoding equals proto.Marshal of the response message, byte
// for byte, including zero fields, negative zero, infinities and NaN.
func TestHistoryEncodingMatchesProtobuf(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	special := []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.NaN(), 1e-300, -2.5}
	value := func() float64 {
		if r.IntN(3) == 0 {
			return special[r.IntN(len(special))]
		}
		return r.NormFloat64() * 1e6
	}
	for i := 0; i < 500; i++ {
		out := &historyResult{}
		if r.IntN(4) > 0 {
			out.metric = "metric.name"
		}
		for n := r.IntN(50); n > 0; n-- {
			out.timeDelta = append(out.timeDelta, r.Int64N(1<<50)-1<<40)
			if i%2 == 0 {
				out.values = append(out.values, value())
			} else {
				a := hta.Aggregate{Minimum: value(), Maximum: value(), Sum: value(), Integral: value()}
				if r.IntN(3) > 0 {
					a.Count = r.Uint64N(1 << 40)
				}
				if r.IntN(3) > 0 {
					a.ActiveTime = r.Int64N(1<<50) - 1<<40
				}
				out.aggs = append(out.aggs, a)
			}
		}
		want, err := proto.Marshal(out.response())
		if err != nil {
			t.Fatal(err)
		}
		if got := out.encode(); !bytes.Equal(got, want) || out.size() != len(want) {
			t.Fatalf("case %d: encoded %d bytes (size %d), protobuf %d bytes", i, len(got), out.size(), len(want))
		}
	}
}

// QueryEncoded returns exactly the marshaled response of Query for every
// request type.
func TestQueryEncodedMatchesQuery(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 5000)
	f.flush()
	f.ingest("x", 300) // also unflushed values
	end := f.next["x"] + 1000
	for _, req := range []*metricq.HistoryRequest{
		{Type: metricq.HistoryRequest_LAST_VALUE},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 150, EndTime: end},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 150, EndTime: end, IntervalMax: 50},
		{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 150, EndTime: end, IntervalMax: 1000},
		{Type: metricq.HistoryRequest_AGGREGATE_TIMELINE, StartTime: 150, EndTime: end},
		{Type: metricq.HistoryRequest_AGGREGATE_TIMELINE, StartTime: 150, EndTime: end, IntervalMax: 1000},
		{Type: metricq.HistoryRequest_AGGREGATE, StartTime: 150, EndTime: end},
	} {
		resp, err := f.e.Query(context.Background(), "x", req)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := proto.Marshal(resp)
		got, err := f.e.QueryEncoded(context.Background(), "x", req)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%v: %v, %d bytes against %d", req.Type, err, len(got), len(want))
		}
	}
}

func BenchmarkHistoryResponse(b *testing.B) {
	out := &historyResult{metric: "diss.hta-s3.c0"}
	for i := 0; i < 23_700; i++ {
		out.timeDelta = append(out.timeDelta, 1_000_000)
		v := 200 + float64(i%97)/7
		out.aggs = append(out.aggs, hta.Aggregate{Minimum: v, Maximum: v, Sum: v, Count: 1, Integral: v * 1e6, ActiveTime: 1_000_000})
	}
	b.Run("message+marshal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			resp := out.response()
			_ = proto.Size(resp)
			if _, err := proto.Marshal(resp); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = out.size()
			_ = out.encode()
		}
	})
}
