package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func codecRootPage() map[string]map[int64]blob {
	roots := map[string]map[int64]blob{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("canonical.%03d", i)
		roots[name] = map[int64]blob{}
		for j := 0; j < 7; j++ {
			roots[name][int64(j)*100] = blob{Key: fmt.Sprintf("index/checkpoint/%d", j%2), Offset: int64(i*100 + j), Length: 1000, Hash: [32]byte{byte(i), byte(j)}}
		}
	}
	return roots
}

func TestBinaryRootsStableAndLegacyReadable(t *testing.T) {
	want := codecRootPage()
	want[""] = map[int64]blob{math.MinInt64: {Key: "", Offset: math.MinInt64, Length: math.MaxInt64, Hash: [32]byte{255}}}
	want["empty"] = map[int64]blob{}
	binaryBytes, err := encode(want)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		again, err := encode(want)
		if err != nil || !bytes.Equal(binaryBytes, again) {
			t.Fatalf("unstable encoding: %v", err)
		}
	}
	legacy, err := encodeGob(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, encoded := range [][]byte{binaryBytes, legacy} {
		var got map[string]map[int64]blob
		if err := decode(encoded, &got); err != nil || !reflect.DeepEqual(want, got) {
			t.Fatalf("root round trip: %v", err)
		}
	}
}

func TestBinaryRootsRejectDamageWithoutChangingDestination(t *testing.T) {
	want := codecRootPage()
	valid, err := encode(want)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(valid); cut++ {
		dst := map[string]map[int64]blob{"sentinel": {}}
		if err := decode(valid[:cut], &dst); err == nil {
			t.Fatalf("accepted root truncation %d", cut)
		}
		if len(dst) != 1 || dst["sentinel"] == nil {
			t.Fatal("failed root decode modified destination")
		}
	}
	payload, err := encodeRootPayload(want)
	if err != nil {
		t.Fatal(err)
	}
	badID := append([]byte(nil), payload...)
	// All initial dictionary keys are consumed before metric/level records.
	pos := 8
	for i := uint32(0); i < binary.LittleEndian.Uint32(payload[4:]); i++ {
		n := int(binary.LittleEndian.Uint16(payload[pos:]))
		pos += 2 + n
	}
	pos += 2 + int(binary.LittleEndian.Uint16(payload[pos:])) + 2
	binary.LittleEndian.PutUint32(badID[pos+8:], math.MaxUint32)
	badCount := append([]byte(nil), payload...)
	binary.LittleEndian.PutUint32(badCount, math.MaxUint32)
	badKeys := append([]byte(nil), payload...)
	binary.LittleEndian.PutUint32(badKeys[4:], math.MaxUint32)
	for _, p := range [][]byte{badID, badCount, badKeys, append(append([]byte(nil), payload...), 0)} {
		dst := map[string]map[int64]blob{"sentinel": {}}
		if err := decode(binaryTestEnvelope(rootKind, p), &dst); err == nil {
			t.Fatal("accepted invalid root page")
		}
		if len(dst) != 1 || dst["sentinel"] == nil {
			t.Fatal("failed root decode modified destination")
		}
	}
	for _, invalid := range []map[string]map[int64]blob{{"nil": nil}, {string(make([]byte, 65536)): {}}} {
		if _, err := encode(invalid); err == nil {
			t.Fatal("accepted invalid root map")
		}
	}
}

func BenchmarkRootPageCodec(b *testing.B) {
	value := codecRootPage()
	for _, binaryCodec := range []bool{false, true} {
		name := "gob"
		encoder := encodeGob
		if binaryCodec {
			name = "binary"
			encoder = encode
		}
		data, err := encoder(value)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name+"/encode", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(data)), "wire_bytes")
			for i := 0; i < b.N; i++ {
				if _, err := encoder(value); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/decode", func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(data)), "wire_bytes")
			for i := 0; i < b.N; i++ {
				var dst map[string]map[int64]blob
				if err := decode(data, &dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzBinaryRoots(f *testing.F) {
	b, err := encode(codecRootPage())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	payload, err := encodeRootPayload(codecRootPage())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(payload)
	f.Fuzz(func(t *testing.T, b []byte) {
		var roots map[string]map[int64]blob
		_ = decode(b, &roots)
		_ = decodeRootPayload(b, &roots)
		if len(roots) > maxRootMetrics {
			t.Fatal("unbounded root map")
		}
	})
}
