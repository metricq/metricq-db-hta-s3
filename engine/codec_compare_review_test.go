//go:build review

package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// --- Gorilla-style bit stream (Pelkonen et al. 2015; buckets of the
// Prometheus XOR chunk, which also stores millisecond-or-finer times).

type bitWriter struct {
	b     []byte
	count uint8 // free bits in the last byte
}

func (w *bitWriter) writeBit(bit bool) {
	if w.count == 0 {
		w.b = append(w.b, 0)
		w.count = 8
	}
	w.count--
	if bit {
		w.b[len(w.b)-1] |= 1 << w.count
	}
}

func (w *bitWriter) writeBits(v uint64, n int) {
	for n > 0 {
		if w.count == 0 {
			w.b = append(w.b, 0)
			w.count = 8
		}
		take := min(n, int(w.count))
		w.count -= uint8(take)
		w.b[len(w.b)-1] |= byte((v>>(n-take))&(1<<take-1)) << w.count
		n -= take
	}
}

type bitReader struct {
	b   []byte
	pos int // bit position
}

func (r *bitReader) readBit() (bool, error) {
	if r.pos >= 8*len(r.b) {
		return false, fmt.Errorf("end of stream")
	}
	bit := r.b[r.pos/8]&(1<<(7-r.pos%8)) != 0
	r.pos++
	return bit, nil
}

func (r *bitReader) readBits(n int) (uint64, error) {
	if r.pos+n > 8*len(r.b) {
		return 0, fmt.Errorf("end of stream")
	}
	var v uint64
	for n > 0 {
		off := r.pos % 8
		take := min(n, 8-off)
		chunk := uint64(r.b[r.pos/8]>>(8-off-take)) & (1<<take - 1)
		v = v<<take | chunk
		r.pos += take
		n -= take
	}
	return v, nil
}

// int stream: first value raw, then delta-of-delta in buckets.
type dodState struct {
	n           int
	prev, delta int64
}

func (s *dodState) put(w *bitWriter, v int64) {
	switch s.n {
	case 0:
		w.writeBits(uint64(v), 64)
	case 1:
		s.delta = v - s.prev
		w.writeBits(uint64(s.delta), 64)
	default:
		d := v - s.prev
		dod := d - s.delta
		s.delta = d
		switch {
		case dod == 0:
			w.writeBit(false)
		case bitRange(dod, 14):
			w.writeBits(0b10, 2)
			w.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			w.writeBits(0b110, 3)
			w.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			w.writeBits(0b1110, 4)
			w.writeBits(uint64(dod), 20)
		default:
			w.writeBits(0b1111, 4)
			w.writeBits(uint64(dod), 64)
		}
	}
	s.prev = v
	s.n++
}

func bitRange(x int64, n uint) bool { return -((1<<(n-1))-1) <= x && x <= 1<<(n-1) }

func (s *dodState) get(r *bitReader) (int64, error) {
	var v int64
	switch s.n {
	case 0:
		u, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		v = int64(u)
	case 1:
		u, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		s.delta = int64(u)
		v = s.prev + s.delta
	default:
		var prefix int
		for prefix < 4 {
			bit, err := r.readBit()
			if err != nil {
				return 0, err
			}
			if !bit {
				break
			}
			prefix++
		}
		var dod int64
		if prefix > 0 {
			width := [...]int{0, 14, 17, 20, 64}[prefix]
			u, err := r.readBits(width)
			if err != nil {
				return 0, err
			}
			if width == 64 {
				dod = int64(u)
			} else if u > 1<<(width-1) {
				dod = int64(u) - 1<<width
			} else {
				dod = int64(u)
			}
		}
		s.delta += dod
		v = s.prev + s.delta
	}
	s.prev = v
	s.n++
	return v, nil
}

// float stream: XOR with the previous value, reusing the previous window
// of meaningful bits when it covers the new one.
type xorState struct {
	n                 int
	prev              uint64
	leading, trailing uint8
}

func (s *xorState) put(w *bitWriter, f float64) {
	v := math.Float64bits(f)
	if s.n == 0 {
		w.writeBits(v, 64)
		s.prev, s.leading = v, 0xff
		s.n++
		return
	}
	x := v ^ s.prev
	s.prev = v
	s.n++
	if x == 0 {
		w.writeBit(false)
		return
	}
	w.writeBit(true)
	leading, trailing := uint8(bits.LeadingZeros64(x)), uint8(bits.TrailingZeros64(x))
	if leading >= 32 {
		leading = 31
	}
	if s.leading != 0xff && leading >= s.leading && trailing >= s.trailing {
		w.writeBit(false)
		w.writeBits(x>>s.trailing, 64-int(s.leading)-int(s.trailing))
		return
	}
	s.leading, s.trailing = leading, trailing
	w.writeBit(true)
	w.writeBits(uint64(leading), 5)
	sig := 64 - leading - trailing
	w.writeBits(uint64(sig&63), 6) // 64 is stored as 0
	w.writeBits(x>>trailing, int(sig))
}

func (s *xorState) get(r *bitReader) (float64, error) {
	if s.n == 0 {
		v, err := r.readBits(64)
		if err != nil {
			return 0, err
		}
		s.prev, s.n = v, 1
		return math.Float64frombits(v), nil
	}
	s.n++
	bit, err := r.readBit()
	if err != nil {
		return 0, err
	}
	if !bit {
		return math.Float64frombits(s.prev), nil
	}
	if bit, err = r.readBit(); err != nil {
		return 0, err
	}
	if bit {
		l, err := r.readBits(5)
		if err != nil {
			return 0, err
		}
		sig, err := r.readBits(6)
		if err != nil {
			return 0, err
		}
		if sig == 0 {
			sig = 64
		}
		s.leading, s.trailing = uint8(l), uint8(64-l-sig)
	}
	sig := 64 - int(s.leading) - int(s.trailing)
	x, err := r.readBits(sig)
	if err != nil {
		return 0, err
	}
	s.prev ^= x << s.trailing
	return math.Float64frombits(s.prev), nil
}

func gorillaEncode(records []hta.Record) []byte {
	w := &bitWriter{b: binary.AppendUvarint(nil, uint64(len(records)))}
	if len(records) == 0 {
		return w.b
	}
	level := records[0].Level
	w.b = binary.AppendVarint(w.b, level)
	var t, repeat, count, active dodState
	var value, mn, mx, sum, integral xorState
	for _, r := range records {
		t.put(w, r.Time)
		if level == 0 {
			value.put(w, r.Value)
			continue
		}
		repeat.put(w, r.Repeat)
		mn.put(w, r.Aggregate.Minimum)
		mx.put(w, r.Aggregate.Maximum)
		sum.put(w, r.Aggregate.Sum)
		count.put(w, int64(r.Aggregate.Count))
		integral.put(w, r.Aggregate.Integral)
		active.put(w, r.Aggregate.ActiveTime)
	}
	return w.b
}

func gorillaDecode(b []byte) ([]hta.Record, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, fmt.Errorf("count")
	}
	b = b[k:]
	if n == 0 {
		return nil, nil
	}
	level, k := binary.Varint(b)
	if k <= 0 {
		return nil, fmt.Errorf("level")
	}
	r := &bitReader{b: b[k:]}
	records := make([]hta.Record, n)
	var t, repeat, count, active dodState
	var value, mn, mx, sum, integral xorState
	var err error
	for i := range records {
		rec := &records[i]
		rec.Level, rec.Repeat = level, 1
		if rec.Time, err = t.get(r); err != nil {
			return nil, err
		}
		if level == 0 {
			if rec.Value, err = value.get(r); err != nil {
				return nil, err
			}
			continue
		}
		var c int64
		if rec.Repeat, err = repeat.get(r); err == nil {
			if rec.Aggregate.Minimum, err = mn.get(r); err == nil {
				if rec.Aggregate.Maximum, err = mx.get(r); err == nil {
					if rec.Aggregate.Sum, err = sum.get(r); err == nil {
						if c, err = count.get(r); err == nil {
							if rec.Aggregate.Integral, err = integral.get(r); err == nil {
								rec.Aggregate.ActiveTime, err = active.get(r)
							}
						}
					}
				}
			}
		}
		if err != nil {
			return nil, err
		}
		rec.Aggregate.Count = uint64(c)
	}
	return records, nil
}

// --- columnar layout: each field as its own column, time as varint deltas,
// floats byte-shuffled (byte 0 of all values, then byte 1, ...) so that
// exponents and high mantissa bytes sit next to each other.

func appendShuffled(b []byte, fs []float64) []byte {
	for k := 0; k < 8; k++ {
		for _, f := range fs {
			b = append(b, byte(math.Float64bits(f)>>(8*k)))
		}
	}
	return b
}

func readShuffled(b []byte, n int) ([]float64, []byte, error) {
	if len(b) < 8*n {
		return nil, nil, fmt.Errorf("short float column")
	}
	u := make([]uint64, n)
	for k := 0; k < 8; k++ {
		for i := range u {
			u[i] |= uint64(b[k*n+i]) << (8 * k)
		}
	}
	fs := make([]float64, n)
	for i := range u {
		fs[i] = math.Float64frombits(u[i])
	}
	return fs, b[8*n:], nil
}

func columnarEncode(records []hta.Record) []byte {
	b := binary.AppendUvarint(nil, uint64(len(records)))
	if len(records) == 0 {
		return b
	}
	level := records[0].Level
	b = binary.AppendVarint(b, level)
	prev := int64(0)
	for _, r := range records {
		b = binary.AppendVarint(b, r.Time-prev)
		prev = r.Time
	}
	column := func(get func(hta.Record) float64) {
		fs := make([]float64, len(records))
		for i, r := range records {
			fs[i] = get(r)
		}
		b = appendShuffled(b, fs)
	}
	if level == 0 {
		column(func(r hta.Record) float64 { return r.Value })
		return b
	}
	for _, r := range records {
		b = binary.AppendVarint(b, r.Repeat)
	}
	for _, r := range records {
		b = binary.AppendUvarint(b, r.Aggregate.Count)
	}
	for _, r := range records {
		b = binary.AppendVarint(b, r.Aggregate.ActiveTime)
	}
	column(func(r hta.Record) float64 { return r.Aggregate.Minimum })
	column(func(r hta.Record) float64 { return r.Aggregate.Maximum })
	column(func(r hta.Record) float64 { return r.Aggregate.Sum })
	column(func(r hta.Record) float64 { return r.Aggregate.Integral })
	return b
}

func columnarDecode(b []byte) ([]hta.Record, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, fmt.Errorf("count")
	}
	b = b[k:]
	if n == 0 {
		return nil, nil
	}
	level, k := binary.Varint(b)
	if k <= 0 {
		return nil, fmt.Errorf("level")
	}
	b = b[k:]
	records := make([]hta.Record, n)
	varints := func(set func(*hta.Record, int64)) error {
		for i := range records {
			v, k := binary.Varint(b)
			if k <= 0 {
				return fmt.Errorf("varint column")
			}
			b = b[k:]
			set(&records[i], v)
		}
		return nil
	}
	prev := int64(0)
	if err := varints(func(r *hta.Record, v int64) { prev += v; r.Time, r.Level, r.Repeat = prev, level, 1 }); err != nil {
		return nil, err
	}
	column := func(set func(*hta.Record, float64)) error {
		fs, rest, err := readShuffled(b, len(records))
		if err != nil {
			return err
		}
		b = rest
		for i, f := range fs {
			set(&records[i], f)
		}
		return nil
	}
	if level == 0 {
		return records, column(func(r *hta.Record, f float64) { r.Value = f })
	}
	if err := varints(func(r *hta.Record, v int64) { r.Repeat = v }); err != nil {
		return nil, err
	}
	for i := range records {
		v, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, fmt.Errorf("count column")
		}
		b = b[k:]
		records[i].Aggregate.Count = v
	}
	if err := varints(func(r *hta.Record, v int64) { r.Aggregate.ActiveTime = v }); err != nil {
		return nil, err
	}
	for _, set := range []func(*hta.Record, float64){
		func(r *hta.Record, f float64) { r.Aggregate.Minimum = f },
		func(r *hta.Record, f float64) { r.Aggregate.Maximum = f },
		func(r *hta.Record, f float64) { r.Aggregate.Sum = f },
		func(r *hta.Record, f float64) { r.Aggregate.Integral = f },
	} {
		if err := column(set); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// --- comparison

type blockCodec struct {
	name   string
	encode func([]hta.Record) ([]byte, error)
	decode func([]byte) ([]hta.Record, error)
}

func sameRecords(a, b []hta.Record) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Time != y.Time || x.Level != y.Level || x.Repeat != y.Repeat || math.Float64bits(x.Value) != math.Float64bits(y.Value) ||
			math.Float64bits(x.Aggregate.Minimum) != math.Float64bits(y.Aggregate.Minimum) || math.Float64bits(x.Aggregate.Maximum) != math.Float64bits(y.Aggregate.Maximum) ||
			math.Float64bits(x.Aggregate.Sum) != math.Float64bits(y.Aggregate.Sum) || x.Aggregate.Count != y.Aggregate.Count ||
			math.Float64bits(x.Aggregate.Integral) != math.Float64bits(y.Aggregate.Integral) || x.Aggregate.ActiveTime != y.Aggregate.ActiveTime {
			return false
		}
	}
	return true
}

// Compares block codecs on blocks sampled from a live database (GETs only):
//
//	METRICQ_LIVE_S3_ENDPOINT=http://127.0.0.1:19001 METRICQ_LIVE_S3_BUCKET=metricq \
//	METRICQ_LIVE_S3_PREFIX=db-hta-s3-dummy AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//	go test -tags review ./engine -run TestReviewBlockCodecs -v
func TestReviewBlockCodecs(t *testing.T) {
	endpoint := os.Getenv("METRICQ_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set METRICQ_LIVE_S3_ENDPOINT")
	}
	ctx := context.Background()
	s, err := storage.NewS3(ctx, storage.S3Config{Bucket: os.Getenv("METRICQ_LIVE_S3_BUCKET"), Prefix: os.Getenv("METRICQ_LIVE_S3_PREFIX"), Endpoint: endpoint, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{store: s, metrics: NewMetrics(prometheus.NewRegistry()), nodeCache: make(map[blob]indexNode)}
	b, _, err := e.get(ctx, "manifest")
	if err != nil {
		t.Fatal(err)
	}
	if err = decode(b, &e.state); err != nil {
		t.Fatal(err)
	}
	if err = e.loadManifestState(ctx, &e.state); err != nil {
		t.Fatal(err)
	}
	// Data sets: metric name prefix and how many streams of it to sample.
	sets := []struct {
		name, prefix string
		streams      int
	}{{"1 Sa/s load", "load.hta-s3.", 12}, {"100 Sa/s dummy", "dummy.source", 1}, {"1 kSa/s diss", "diss.hta-s3.", 6}, {"rabbitmq rates", "metricq.rabbitmq", 2}}
	r := rand.New(rand.NewPCG(1, 1))
	blocks := map[string][][]hta.Record{}
	for _, set := range sets {
		var names []string
		for name := range e.state.Roots {
			if strings.HasPrefix(name, set.prefix) {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
		for _, name := range names[:min(set.streams, len(names))] {
			for level, root := range e.state.Roots[name] {
				kind := set.name + ", raw"
				if level != 0 {
					kind = set.name + ", aggregates"
				}
				var entries []indexEntry
				if err := e.indexRangeEntries(ctx, root, math.MinInt64, math.MaxInt64, &entries); err != nil {
					t.Fatal(err)
				}
				r.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
				for _, entry := range entries[:min(len(entries), 40/set.streams+4)] {
					raw, err := e.readBlob(ctx, entry.Blob)
					if err != nil {
						t.Fatal(err)
					}
					var records []hta.Record
					if err := decode(raw, &records); err != nil {
						t.Fatal(err)
					}
					blocks[kind] = append(blocks[kind], records)
				}
			}
		}
	}
	zdec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	payload := func(records []hta.Record) []byte { p, _ := appendRecordsPayload(nil, records); return p }
	zstdCodec := func(name string, level zstd.EncoderLevel, layout func([]hta.Record) []byte, parse func([]byte) ([]hta.Record, error)) blockCodec {
		z, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false))
		return blockCodec{name, func(rs []hta.Record) ([]byte, error) { return z.EncodeAll(layout(rs), nil), nil }, func(b []byte) ([]hta.Record, error) {
			p, err := zdec.DecodeAll(b, nil)
			if err != nil {
				return nil, err
			}
			return parse(p)
		}}
	}
	gzipCodec := func(name string, level int, layout func([]hta.Record) []byte, parse func([]byte) ([]hta.Record, error)) blockCodec {
		return blockCodec{name, func(rs []hta.Record) ([]byte, error) {
			var out bytes.Buffer
			z, _ := gzip.NewWriterLevel(&out, level)
			z.Write(layout(rs))
			z.Close()
			return out.Bytes(), nil
		}, func(b []byte) ([]hta.Record, error) {
			z, err := gzip.NewReader(bytes.NewReader(b))
			if err != nil {
				return nil, err
			}
			p, err := io.ReadAll(z)
			if err != nil {
				return nil, err
			}
			return parse(p)
		}}
	}
	codecs := []blockCodec{
		gzipCodec("gzip 1", gzip.BestSpeed, payload, decodeRecordsPayload),
		gzipCodec("gzip 6", gzip.DefaultCompression, payload, decodeRecordsPayload),
		gzipCodec("gzip 9", gzip.BestCompression, payload, decodeRecordsPayload),
		zstdCodec("zstd fastest", zstd.SpeedFastest, payload, decodeRecordsPayload),
		zstdCodec("zstd default", zstd.SpeedDefault, payload, decodeRecordsPayload),
		zstdCodec("zstd better", zstd.SpeedBetterCompression, payload, decodeRecordsPayload),
		zstdCodec("zstd best", zstd.SpeedBestCompression, payload, decodeRecordsPayload),
		zstdCodec("columnar + zstd fastest", zstd.SpeedFastest, columnarEncode, columnarDecode),
		zstdCodec("columnar + zstd best", zstd.SpeedBestCompression, columnarEncode, columnarDecode),
		gzipCodec("columnar + gzip 9", gzip.BestCompression, columnarEncode, columnarDecode),
		{"gorilla", func(rs []hta.Record) ([]byte, error) { return gorillaEncode(rs), nil }, gorillaDecode},
	}
	var kinds []string
	for kind := range blocks {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	fmt.Printf("data set,codec,blocks,records,bytes_per_record,encode_us_per_block,decode_us_per_block,decode_ns_per_record\n")
	for _, kind := range kinds {
		set := blocks[kind]
		records := 0
		for _, rs := range set {
			records += len(rs)
		}
		for _, c := range codecs {
			var size int
			encoded := make([][]byte, len(set))
			start := time.Now()
			const rounds = 5
			for round := 0; round < rounds; round++ {
				for i, rs := range set {
					if encoded[i], err = c.encode(rs); err != nil {
						t.Fatal(err)
					}
				}
			}
			enc := time.Since(start)
			for i, rs := range set {
				size += len(encoded[i])
				got, err := c.decode(encoded[i])
				if err != nil || !sameRecords(rs, got) {
					t.Fatalf("%s %s block %d: round trip failed: %v", kind, c.name, i, err)
				}
			}
			start = time.Now()
			for round := 0; round < rounds; round++ {
				for _, b := range encoded {
					if _, err := c.decode(b); err != nil {
						t.Fatal(err)
					}
				}
			}
			dec := time.Since(start)
			n := float64(rounds * len(set))
			fmt.Printf("%s,%s,%d,%d,%.2f,%.1f,%.1f,%.1f\n", kind, c.name, len(set), records, float64(size)/float64(records), float64(enc.Microseconds())/n, float64(dec.Microseconds())/n, float64(dec.Nanoseconds())/float64(rounds*records))
		}
	}
}
