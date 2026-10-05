package engine

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

func sampleHeldDelta(streams, records int) heldDelta {
	var d heldDelta
	for s := 0; s < streams; s++ {
		st := deltaStream{Metric: "load.hta-s3.m" + string(rune('a'+s%26)), Level: int64(s%3) * 40e9}
		for i := 0; i < records; i++ {
			r := hta.Record{Time: 1790000000e9 + int64(i)*1e9, Level: st.Level, Repeat: 1, Value: math.Round(math.Sin(float64(i))*1e4) / 100}
			if st.Level > 0 {
				r.Value = 0
				r.Aggregate = hta.Aggregate{Minimum: -1, Maximum: 1, Sum: 3.5, Count: 40, Integral: 1.5e11, ActiveTime: 40e9}
			}
			st.Records = append(st.Records, r)
		}
		d.Streams = append(d.Streams, st)
	}
	return d
}

func TestHeldDeltaBinaryRoundTrip(t *testing.T) {
	for _, want := range []heldDelta{{Streams: []deltaStream{}}, sampleHeldDelta(1, 1), sampleHeldDelta(30, 200)} {
		b, err := encode(want)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(b, []byte(blockMagic)) {
			t.Fatal("held delta not in the binary block format")
		}
		var got heldDelta
		if err := decode(b, &got); err != nil {
			t.Fatal(err)
		}
		if len(want.Streams) == 0 {
			if len(got.Streams) != 0 {
				t.Fatalf("empty delta decoded as %+v", got)
			}
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("held delta changed by round trip")
		}
		legacy, err := encodeGob(want)
		if err != nil {
			t.Fatal(err)
		}
		var old heldDelta
		if err := decode(legacy, &old); err == nil {
			t.Fatal("gob-encoded held delta accepted")
		}
	}
}

func TestHeldDeltaBinaryRejectsCorruption(t *testing.T) {
	d := sampleHeldDelta(3, 5)
	payload, err := encodeHeldPayload(d)
	if err != nil {
		t.Fatal(err)
	}
	var got heldDelta
	if err := decodeBinaryPayload(payload, &got); err != nil || !reflect.DeepEqual(got, d) {
		t.Fatalf("reference payload: %v", err)
	}
	for cut := 0; cut < len(payload); cut++ {
		if err := decodeBinaryPayload(payload[:cut], &got); err == nil {
			t.Fatalf("truncated payload (%d of %d bytes) accepted", cut, len(payload))
		}
	}
	if err := decodeBinaryPayload(append(append([]byte(nil), payload...), 0), &got); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	huge := append([]byte{0xff, 0xff, 0xff, 0xff, 0x0f}, payload[1:]...)
	if err := decodeBinaryPayload(huge, &got); err == nil {
		t.Fatal("implausible stream count accepted")
	}
	b, _ := encode(d)
	var records []hta.Record
	if err := decode(b, &records); err == nil {
		t.Fatal("held delta decoded as data block")
	}
}

func TestHeldDeltaRejectsMismatchedRecords(t *testing.T) {
	d := sampleHeldDelta(1, 2)
	d.Streams[0].Records[1].Level = 400e9
	if _, err := encode(d); err == nil {
		t.Fatal("record of another level encoded")
	}
}

func BenchmarkHeldDeltaEncode(b *testing.B) {
	d := sampleHeldDelta(30, 200)
	b.Run("binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := encode(d); err != nil {
				b.Fatal(err)
			}
		}
	})
}
