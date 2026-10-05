package engine

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

func sampleStatePage(n int) map[string]*hta.Series {
	m := map[string]*hta.Series{}
	for i := 0; i < n; i++ {
		s := hta.New(hta.Config{IntervalMin: 40e9, IntervalMax: 4e14, IntervalFactor: 10})
		s.Config.Input = fmt.Sprintf("input.%d", i)
		s.First = hta.Point{Time: 1790000000e9, Value: 1.5}
		s.Last = hta.Point{Time: 1790003600e9 + int64(i), Value: math.Sin(float64(i))}
		for level := int64(40e9); level <= 4e14; level *= 10 {
			s.Levels[level] = hta.Level{Time: 1790003600e9 - level, Aggregate: hta.Aggregate{Minimum: -1, Maximum: 2, Sum: 3.25, Count: uint64(i), Integral: 1e11, ActiveTime: level}}
		}
		m[fmt.Sprintf("load.hta-s3.m%04d", i)] = s
	}
	return m
}

func TestStatePageBinaryRoundTrip(t *testing.T) {
	want := sampleStatePage(20)
	want["empty"] = hta.New(hta.Config{IntervalMin: 1, IntervalMax: 10, IntervalFactor: 10})
	b, err := encode(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte(blockMagic)) {
		t.Fatal("state page not in the binary block format")
	}
	var got map[string]*hta.Series
	if err := decode(b, &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("state page changed by round trip: %v", err)
	}
	if got["empty"].Levels == nil {
		t.Fatal("empty level map decoded as nil")
	}
	legacy, err := encodeGob(want)
	if err != nil {
		t.Fatal(err)
	}
	var old map[string]*hta.Series
	if err := decode(legacy, &old); err == nil {
		t.Fatal("gob-encoded state page accepted")
	}
	if _, err := encode(map[string]*hta.Series{"nil": nil}); err == nil {
		t.Fatal("nil series encoded")
	}
}

func TestStatePageBinaryRejectsCorruption(t *testing.T) {
	payload, err := encodeStatePayload(sampleStatePage(3))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]*hta.Series
	for cut := 0; cut < len(payload); cut++ {
		if err := decodeStatePayload(payload[:cut], &got); err == nil {
			t.Fatalf("truncated state page (%d of %d bytes) accepted", cut, len(payload))
		}
	}
	if err := decodeStatePayload(append(append([]byte(nil), payload...), 0), &got); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	r := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 20000; i++ {
		buf := append([]byte(nil), payload...)
		buf[r.IntN(len(buf))] = byte(r.Uint32())
		_ = decodeStatePayload(buf, &got)
	}
}
