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
		records[i] = hta.Record{Time: 1700000000000000000 + int64(i)*1000000000, Value: math.Sin(float64(i) / 17)}
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
	special := []hta.Record{{Time: math.MinInt64, Level: math.MaxInt64, Repeat: -1, Value: math.Float64frombits(1 << 63), Aggregate: hta.Aggregate{Minimum: math.Inf(-1), Maximum: math.Inf(1), Sum: math.Float64frombits(0x7ff8000000000042), Count: math.MaxUint64, Integral: math.SmallestNonzeroFloat64, ActiveTime: math.MinInt64}}}
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

func binaryTestEnvelope(kind byte, payload []byte) []byte {
	var out bytes.Buffer
	out.WriteString(blockMagic)
	out.WriteByte(blockVersion)
	out.WriteByte(kind)
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
	excessive := make([]byte, 4)
	binary.LittleEndian.PutUint32(excessive, math.MaxUint32)
	trailing := make([]byte, 5)
	for _, bad := range [][]byte{version, corrupt, append(append([]byte(nil), valid...), 0), append(append([]byte(nil), valid...), binaryTestEnvelope(recordKind, nil)[6:]...), binaryTestEnvelope(recordKind, excessive), binaryTestEnvelope(recordKind, trailing), binaryTestEnvelope(recordKind, make([]byte, 5+maxDataBlockRecords*recordWireBytes))} {
		dst := []hta.Record{{Time: 42}}
		if err := decode(bad, &dst); err == nil {
			t.Fatal("accepted corrupt data block")
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
		z, err := gzip.NewReader(bytes.NewReader(b[6:]))
		if err != nil {
			f.Fatal(err)
		}
		payload, err := io.ReadAll(z)
		z.Close()
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
