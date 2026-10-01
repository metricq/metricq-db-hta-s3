package engine

import (
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

func sampleBatch(points int) batch {
	b := batch{ReceivedAt: 1790000000123456789, Config: hta.Config{IntervalMin: 40e9, IntervalMax: 4e14, IntervalFactor: 10}, Metric: "load.hta-s3.m0042"}
	for i := 0; i < points; i++ {
		b.Points = append(b.Points, hta.Point{Time: 1790000000000000000 + int64(i)*1e9 + int64(i%7), Value: math.Sin(float64(i)) * 100})
	}
	return b
}

func TestWALFrameRoundTripAndLegacyFrames(t *testing.T) {
	for _, n := range []int{0, 1, 500} {
		want := sampleBatch(n)
		if n == 0 {
			want.Points = []hta.Point{}
		}
		frame, err := encode(want)
		if err != nil {
			t.Fatal(err)
		}
		var got batch
		if err := decode(frame, &got); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%d points: %v\n got %+v\nwant %+v", n, err, got, want)
		}
		// Frames written by earlier versions stay readable.
		legacy, err := encodeGob(want)
		if err != nil {
			t.Fatal(err)
		}
		var old batch
		if err := decode(legacy, &old); err != nil || old.Metric != want.Metric || len(old.Points) != n {
			t.Fatalf("legacy frame: %v %+v", err, old)
		}
	}
	single, _ := encode(sampleBatch(1))
	legacy, _ := encodeGob(sampleBatch(1))
	t.Logf("one-sample frame: %d bytes, legacy %d bytes", len(single), len(legacy))
}

func TestWALFrameRejectsCorruption(t *testing.T) {
	frame, _ := encode(sampleBatch(3))
	var b batch
	for cut := len(walMagic) + 1; cut < len(frame); cut++ {
		if err := decode(frame[:cut], &b); err == nil {
			t.Fatalf("truncated frame (%d of %d bytes) accepted", cut, len(frame))
		}
	}
	if err := decode(append(append([]byte(nil), frame...), 0), &b); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	bad := append([]byte(nil), frame...)
	bad[len(walMagic)] = walVersion + 1
	if err := decode(bad, &b); err == nil {
		t.Fatal("unknown version accepted")
	}
	var records []hta.Record
	if err := decode(frame, &records); err == nil {
		t.Fatal("WAL frame decoded as data block")
	}
}

func BenchmarkWALFrameEncode(b *testing.B) {
	single := sampleBatch(1)
	b.Run("binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := encode(single); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("gob-gzip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := encodeGob(single); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestWALFrameRandomInputNeverPanics(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	frame, _ := encode(sampleBatch(5))
	var b batch
	for i := 0; i < 20000; i++ {
		buf := append([]byte(nil), frame...)
		for j := 0; j < 1+r.IntN(4); j++ {
			buf[len(walMagic)+1+r.IntN(len(buf)-len(walMagic)-1)] = byte(r.Uint32())
		}
		_ = decode(buf[:len(walMagic)+1+r.IntN(len(buf)-len(walMagic))], &b)
	}
}
