package engine

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

func codecRecords(n int, aggregate bool) []hta.Record {
	records := make([]hta.Record, n)
	for i := range records {
		records[i] = hta.Record{Time: 1700000000000000000 + int64(i)*1000000000, Repeat: 1, Value: math.Sin(float64(i) / 17)}
		if aggregate {
			records[i].Level = 1000000000
			records[i].Repeat = 1
			records[i].Aggregate = hta.Value(records[i].Value, 1000000000, uint64(i%7+1))
			records[i].Value = 0
		}
	}
	return records
}

func codecIndex() indexNode {
	n := indexNode{Leaf: true}
	for i := 0; i < indexFanout; i++ {
		n.Entries = append(n.Entries, indexEntry{First: int64(i) * 1024, Last: int64(i+1)*1024 - 1, Records: 1024, Blob: blob{Key: fmt.Sprintf("data/compact-test/%d", i%4), Offset: int64(i) * 1000, Length: 1000, Hash: sha256.Sum256([]byte(fmt.Sprint(i)))}})
	}
	return n
}

func TestBinaryBlockRoundTrip(t *testing.T) {
	for _, v := range []any{codecRecords(1024, false), codecRecords(1024, true), []hta.Record(nil), codecIndex(), indexNode{}} {
		encoded, err := encode(v)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(encoded, []byte(blockMagic)) {
			t.Fatal("missing versioned envelope")
		}
		switch want := v.(type) {
		case []hta.Record:
			var got []hta.Record
			if err = decode(encoded, &got); err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("records round trip: %v", err)
			}
		case indexNode:
			var got indexNode
			if err = decode(encoded, &got); err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("index round trip: %v", err)
			}
		}
	}
	// Fixed integers and IEEE-754 bits survive even when Gob's zero-field
	// handling would discard the sign of zero. No math is performed by the codec.
	for _, special := range [][]hta.Record{
		{{Time: math.MinInt64, Repeat: 1, Value: math.Float64frombits(1 << 63)}, {Time: math.MaxInt64, Repeat: 1, Value: math.Float64frombits(0x7ff8000000000042)}},
		{{Time: math.MinInt64, Level: math.MaxInt64, Repeat: -1, Aggregate: hta.Aggregate{Minimum: math.Inf(-1), Maximum: math.Inf(1), Sum: math.Float64frombits(0x7ff8000000000042), Count: math.MaxUint64, Integral: math.SmallestNonzeroFloat64, ActiveTime: math.MinInt64}}},
	} {
		b, err := encode(special)
		if err != nil {
			t.Fatal(err)
		}
		var got []hta.Record
		if err = decode(b, &got); err != nil {
			t.Fatal(err)
		}
		again, err := encode(got)
		if err != nil || !bytes.Equal(b, again) {
			t.Fatalf("bit representation changed: %v", err)
		}
	}
}

func binaryTestEnvelope(kind byte, payload []byte) []byte {
	var out bytes.Buffer
	out.WriteString(blockMagic)
	out.WriteByte(blockVersion)
	out.WriteByte(kind)
	if kind == recordKind || kind == settledRecordKind {
		return zstdFast().EncodeAll(payload, out.Bytes())
	}
	z := gzip.NewWriter(&out)
	z.Write(payload)
	z.Close()
	return out.Bytes()
}

func TestBinaryBlockRejectsDamageWithoutChangingDestination(t *testing.T) {
	valid, err := encode(codecRecords(3, false))
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(valid); cut++ {
		dst := []hta.Record{{Time: 42}}
		if err := decode(valid[:cut], &dst); err == nil {
			t.Fatalf("accepted truncation at %d", cut)
		}
		if len(dst) != 1 || dst[0].Time != 42 {
			t.Fatalf("modified destination at %d", cut)
		}
	}
	version := append([]byte(nil), valid...)
	version[4]++
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-1] ^= 1
	// Appended garbage is rejected. An appended empty zstd frame decodes to
	// nothing and is caught only by the index entry's SHA-256 and length.
	excessive := binary.AppendVarint(binary.AppendUvarint(nil, math.MaxUint32), 0)
	trailing := []byte{0, 0, 1}
	for i, bad := range [][]byte{version, corrupt, append(append([]byte(nil), valid...), 0), append(append([]byte(nil), valid...), binaryTestEnvelope(stateKind, nil)[6:]...), binaryTestEnvelope(recordKind, excessive), binaryTestEnvelope(recordKind, trailing), binaryTestEnvelope(recordKind, make([]byte, 21+maxDataBlockRecords*maxRecordWireBytes))} {
		dst := []hta.Record{{Time: 42}}
		if err := decode(bad, &dst); err == nil {
			t.Fatalf("accepted corrupt data block %d", i)
		}
		if len(dst) != 1 || dst[0].Time != 42 {
			t.Fatal("failed decode modified data destination")
		}
	}
	n := codecIndex()
	encoded, err := encode(n)
	if err != nil {
		t.Fatal(err)
	}
	var records []hta.Record
	if decode(encoded, &records) == nil {
		t.Fatal("index decoded as records")
	}
	var node indexNode
	if decode(valid, &node) == nil {
		t.Fatal("records decoded as index")
	}
	// Invalid key IDs, flags, counts and trailing bytes are all rejected.
	for _, payload := range [][]byte{{2, 0, 0, 0, 0}, {0, 65, 0, 0, 0}, {0, 1, 0, 0, 0}, {0, 0, 0, 0, 0, 0}, append([]byte{0, 1, 0, 0, 0}, make([]byte, 70)...)} {
		dst := indexNode{Leaf: true}
		if decode(binaryTestEnvelope(indexKind, payload), &dst) == nil {
			t.Fatal("accepted invalid index page")
		}
		if !dst.Leaf || len(dst.Entries) != 0 {
			t.Fatal("failed decode modified index destination")
		}
	}
	if _, err = encode(make([]hta.Record, maxDataBlockRecords+1)); err == nil {
		t.Fatal("unbounded data block")
	}
	n.Entries[0].Blob.Key = string(make([]byte, maxIndexKeyBytes+1))
	if _, err = encode(n); err == nil {
		t.Fatal("unbounded index key")
	}
}

// Compaction output is strongly compressed (kind 9); gzip data blocks
// (kind 7) are no longer read.
func TestDataBlockCompressionKinds(t *testing.T) {
	for _, aggregate := range []bool{false, true} {
		records := codecRecords(1024, aggregate)
		fast, err := encode(records)
		if err != nil {
			t.Fatal(err)
		}
		settled, err := encode(settledRecords(records))
		if err != nil {
			t.Fatal(err)
		}
		payload, err := appendRecordsPayload(nil, records)
		if err != nil {
			t.Fatal(err)
		}
		if settledBlock(fast) || !settledBlock(settled) {
			t.Fatal("wrong compression kind")
		}
		var dst []hta.Record
		if decode(binaryTestEnvelope(7, payload), &dst) == nil {
			t.Fatal("gzip data block accepted")
		}
		for _, b := range [][]byte{fast, settled} {
			var got []hta.Record
			if err := decode(b, &got); err != nil || !reflect.DeepEqual(records, got) {
				t.Fatalf("kind %d: %v", b[5], err)
			}
		}
	}
}

func TestBinaryBlockTypesRejectGob(t *testing.T) {
	for _, v := range []any{codecRecords(17, false), codecIndex()} {
		old, err := encodeGob(v)
		if err != nil {
			t.Fatal(err)
		}
		var records []hta.Record
		var node indexNode
		if decode(old, &records) == nil || decode(old, &node) == nil {
			t.Fatalf("gob-encoded %T accepted", v)
		}
	}
}

func BenchmarkBlockCodec(b *testing.B) {
	values := []struct {
		name string
		v    any
	}{{"raw1024", codecRecords(1024, false)}, {"aggregate1024", codecRecords(1024, true)}, {"index64", codecIndex()}}
	for _, value := range values {
		b.Run(value.name, func(b *testing.B) {
			for _, name := range []string{"binary"} {
				encoder := encode
				encoded, err := encoder(value.v)
				if err != nil {
					b.Fatal(err)
				}
				b.Run(name+"/encode", func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(len(encoded)), "wire_bytes")
					for i := 0; i < b.N; i++ {
						if _, err := encoder(value.v); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run(name+"/decode", func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(len(encoded)), "wire_bytes")
					for i := 0; i < b.N; i++ {
						switch value.v.(type) {
						case []hta.Record:
							var dst []hta.Record
							if err := decode(encoded, &dst); err != nil {
								b.Fatal(err)
							}
						case indexNode:
							var dst indexNode
							if err := decode(encoded, &dst); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		})
	}
}

func FuzzBinaryBlock(f *testing.F) {
	for _, value := range []any{codecRecords(2, false), codecRecords(2, true), codecIndex()} {
		b, err := encode(value)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		var payload []byte
		if b[5] == recordKind {
			payload, err = zstdDecoder().DecodeAll(b[6:], nil)
		} else {
			var z *gzip.Reader
			if z, err = gzip.NewReader(bytes.NewReader(b[6:])); err == nil {
				payload, err = io.ReadAll(z)
				z.Close()
			}
		}
		if err != nil {
			f.Fatal(err)
		}
		f.Add(payload)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var records []hta.Record
		var node indexNode
		_ = decode(b, &records)
		_ = decode(b, &node)
		_ = decodeBinaryPayload(b, &records)
		_ = decodeBinaryPayload(b, &node)
		if len(records) > maxDataBlockRecords || len(node.Entries) > indexFanout {
			t.Fatal("unbounded decoder allocation")
		}
	})
}

// Data blocks hold one level; raw records carry only time and value and
// aggregate records no value, so nothing outside that shape is dropped.
func TestDataBlockRejectsRecordsOutsideItsShape(t *testing.T) {
	for name, records := range map[string][]hta.Record{
		"mixed levels":       {{Time: 1, Repeat: 1, Value: 2}, {Time: 2, Level: 10, Repeat: 1}},
		"raw aggregate":      {{Time: 1, Repeat: 1, Value: 2, Aggregate: hta.Aggregate{Count: 1}}},
		"raw repeat":         {{Time: 1, Repeat: 2, Value: 2}},
		"aggregate value":    {{Time: 1, Level: 10, Repeat: 1, Value: 3}},
		"aggregate -0 value": {{Time: 1, Level: 10, Repeat: 1, Value: math.Copysign(0, -1)}},
	} {
		if _, err := encode(records); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
}
