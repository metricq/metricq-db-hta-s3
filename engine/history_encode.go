package engine

import (
	"encoding/binary"
	"math"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/encoding/protowire"
)

// historyResult is a history response before encoding: flat slices instead
// of a protobuf message per point, which dominated the CPU and allocations
// of large timelines.
type historyResult struct {
	metric    string
	timeDelta []int64
	values    []float64
	aggs      []hta.Aggregate
}

func (r *historyResult) response() *metricq.HistoryResponse {
	resp := &metricq.HistoryResponse{Metric: r.metric, TimeDelta: r.timeDelta, Value: r.values}
	if len(r.aggs) > 0 {
		resp.Aggregate = make([]*metricq.HistoryResponse_Aggregate, len(r.aggs))
		for i, a := range r.aggs {
			resp.Aggregate[i] = &metricq.HistoryResponse_Aggregate{Minimum: a.Minimum, Maximum: a.Maximum, Sum: a.Sum, Count: a.Count, Integral: a.Integral, ActiveTime: a.ActiveTime}
		}
	}
	return resp
}

// HistoryResponse field numbers (history.proto); 3-5 are deprecated.
const (
	responseMetric    = 1
	responseTimeDelta = 2
	responseAggregate = 6
	responseValue     = 7
)

// aggregateSize is the encoded size of one Aggregate message; like
// protobuf-go, proto3 scalars equal to zero (bit pattern) are omitted.
func aggregateSize(a hta.Aggregate) int {
	n := 0
	for _, f := range [...]float64{a.Minimum, a.Maximum, a.Sum, a.Integral} {
		if math.Float64bits(f) != 0 {
			n += 9
		}
	}
	if a.Count != 0 {
		n += 1 + protowire.SizeVarint(a.Count)
	}
	if a.ActiveTime != 0 {
		n += 1 + protowire.SizeVarint(uint64(a.ActiveTime))
	}
	return n
}

func (r *historyResult) timeDeltaBytes() int {
	n := 0
	for _, d := range r.timeDelta {
		n += protowire.SizeVarint(uint64(d))
	}
	return n
}

// size is the exact length of encode().
func (r *historyResult) size() int {
	n := 0
	if r.metric != "" {
		n += 1 + protowire.SizeBytes(len(r.metric))
	}
	if len(r.timeDelta) > 0 {
		n += 1 + protowire.SizeBytes(r.timeDeltaBytes())
	}
	for _, a := range r.aggs {
		n += 1 + protowire.SizeBytes(aggregateSize(a))
	}
	if len(r.values) > 0 {
		n += 1 + protowire.SizeBytes(8*len(r.values))
	}
	return n
}

// encode writes the HistoryResponse protobuf bytes, identical to
// proto.Marshal of response() (fields in number order, packed repeated
// scalars).
func (r *historyResult) encode() []byte {
	b := make([]byte, 0, r.size())
	if r.metric != "" {
		b = protowire.AppendTag(b, responseMetric, protowire.BytesType)
		b = protowire.AppendString(b, r.metric)
	}
	if len(r.timeDelta) > 0 {
		b = protowire.AppendTag(b, responseTimeDelta, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(r.timeDeltaBytes()))
		for _, d := range r.timeDelta {
			b = protowire.AppendVarint(b, uint64(d))
		}
	}
	for _, a := range r.aggs {
		b = protowire.AppendTag(b, responseAggregate, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(aggregateSize(a)))
		for i, f := range [...]float64{a.Minimum, a.Maximum, a.Sum} {
			if bits := math.Float64bits(f); bits != 0 {
				b = protowire.AppendTag(b, protowire.Number(i+1), protowire.Fixed64Type)
				b = binary.LittleEndian.AppendUint64(b, bits)
			}
		}
		if a.Count != 0 {
			b = protowire.AppendTag(b, 4, protowire.VarintType)
			b = protowire.AppendVarint(b, a.Count)
		}
		if bits := math.Float64bits(a.Integral); bits != 0 {
			b = protowire.AppendTag(b, 5, protowire.Fixed64Type)
			b = binary.LittleEndian.AppendUint64(b, bits)
		}
		if a.ActiveTime != 0 {
			b = protowire.AppendTag(b, 6, protowire.VarintType)
			b = protowire.AppendVarint(b, uint64(a.ActiveTime))
		}
	}
	if len(r.values) > 0 {
		b = protowire.AppendTag(b, responseValue, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(8*len(r.values)))
		for _, v := range r.values {
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
		}
	}
	return b
}
