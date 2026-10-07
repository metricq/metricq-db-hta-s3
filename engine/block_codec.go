package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/gzip"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// The envelope identifies a version and payload kind before decompression.
// Each range still has its own gzip stream and SHA-256 in the owning index.
const blockMagic = "MQHB"
const blockVersion byte = 1
const recordKind byte = 7 // kind 1 were data blocks of fixed 80-byte records

const indexKind byte = 6 // kind 2 were index pages without entry aggregates
const rootKind byte = 3
const heldKind byte = 4
const stateKind byte = 5

// maxHeldPayloadBytes bounds a decoded held delta, as the Gob decoder did.
const maxHeldPayloadBytes = 512 << 20

// maxRecordWireBytes bounds one encoded record: three varints of at most
// ten bytes and five fixed 64-bit fields.
const maxRecordWireBytes = 70
const maxIndexKeyBytes = 65535

// Index entry fields: times, key id, offset, length, hash, records and the
// six aggregate fields.
const indexEntryBytes = 118

func encodeBinaryBlock(v any) ([]byte, bool, error) {
	var payload []byte
	var kind byte
	switch value := v.(type) {
	case []hta.Record:
		kind = recordKind
		if len(value) > maxDataBlockRecords {
			return nil, true, fmt.Errorf("too many data records")
		}
		var err error
		if payload, err = appendRecordsPayload(make([]byte, 0, 16+len(value)*16), value); err != nil {
			return nil, true, err
		}
	case indexNode:
		kind = indexKind
		if len(value.Entries) > indexFanout {
			return nil, true, fmt.Errorf("too many index entries")
		}
		keys := make([]string, 0, len(value.Entries))
		ids := make(map[string]uint16, len(value.Entries))
		for _, entry := range value.Entries {
			if len(entry.Blob.Key) > maxIndexKeyBytes || entry.Records < 0 || uint64(entry.Records) > math.MaxUint32 {
				return nil, true, fmt.Errorf("invalid index field size")
			}
			if _, ok := ids[entry.Blob.Key]; !ok {
				ids[entry.Blob.Key] = uint16(len(keys))
				keys = append(keys, entry.Blob.Key)
			}
		}
		size := 5 + len(value.Entries)*indexEntryBytes
		for _, key := range keys {
			size += 2 + len(key)
		}
		payload = make([]byte, 0, size)
		leaf := byte(0)
		if value.Leaf {
			leaf = 1
		}
		payload = append(payload, leaf)
		payload = binary.LittleEndian.AppendUint16(payload, uint16(len(value.Entries)))
		payload = binary.LittleEndian.AppendUint16(payload, uint16(len(keys)))
		for _, key := range keys {
			payload = binary.LittleEndian.AppendUint16(payload, uint16(len(key)))
			payload = append(payload, key...)
		}
		for _, entry := range value.Entries {
			payload = binary.LittleEndian.AppendUint64(payload, uint64(entry.First))
			payload = binary.LittleEndian.AppendUint64(payload, uint64(entry.Last))
			payload = binary.LittleEndian.AppendUint16(payload, ids[entry.Blob.Key])
			payload = binary.LittleEndian.AppendUint64(payload, uint64(entry.Blob.Offset))
			payload = binary.LittleEndian.AppendUint64(payload, uint64(entry.Blob.Length))
			payload = append(payload, entry.Blob.Hash[:]...)
			payload = binary.LittleEndian.AppendUint32(payload, uint32(entry.Records))
			a := entry.agg
			for _, f := range [...]uint64{math.Float64bits(a.Minimum), math.Float64bits(a.Maximum), math.Float64bits(a.Sum), a.Count, math.Float64bits(a.Integral), uint64(a.ActiveTime)} {
				payload = binary.LittleEndian.AppendUint64(payload, f)
			}
		}
	case heldDelta:
		kind = heldKind
		var err error
		if payload, err = encodeHeldPayload(value); err != nil {
			return nil, true, err
		}
	case map[string]*hta.Series:
		kind = stateKind
		var err error
		if payload, err = encodeStatePayload(value); err != nil {
			return nil, true, err
		}
	case map[string]map[int64]blob:
		kind = rootKind
		var err error
		payload, err = encodeRootPayload(value)
		if err != nil {
			return nil, true, err
		}
	default:
		return nil, false, nil
	}
	var out bytes.Buffer
	out.WriteString(blockMagic)
	out.WriteByte(blockVersion)
	out.WriteByte(kind)
	z := gzipWriters.Get().(*gzip.Writer)
	z.Reset(&out)
	_, err := z.Write(payload)
	if closeErr := z.Close(); err == nil {
		err = closeErr
	}
	z.Reset(io.Discard)
	gzipWriters.Put(z)
	if err != nil {
		return nil, true, err
	}
	return out.Bytes(), true, nil
}

var gzipReaders sync.Pool
var payloadBuffers = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, 32<<10)) }}

func decodeBinaryBlock(b []byte, v any) error {
	if len(b) < 6 || b[4] != blockVersion {
		return fmt.Errorf("unknown or truncated block version")
	}
	kind := b[5]
	limit := int64(0)
	switch v.(type) {
	case *[]hta.Record:
		if kind != recordKind {
			return fmt.Errorf("block payload type mismatch")
		}
		limit = 20 + maxDataBlockRecords*maxRecordWireBytes
	case *indexNode:
		if kind != indexKind {
			return fmt.Errorf("block payload type mismatch")
		}
		limit = 5 + indexFanout*(maxIndexKeyBytes+2+indexEntryBytes)
	case *map[string]map[int64]blob:
		if kind != rootKind {
			return fmt.Errorf("block payload type mismatch")
		}
		limit = maxRootPayloadBytes
	case *heldDelta:
		if kind != heldKind {
			return fmt.Errorf("block payload type mismatch")
		}
		limit = maxHeldPayloadBytes
	case *map[string]*hta.Series:
		if kind != stateKind {
			return fmt.Errorf("block payload type mismatch")
		}
		limit = maxHeldPayloadBytes
	default:
		return fmt.Errorf("block payload type mismatch")
	}
	compressed := bytes.NewReader(b[6:])
	z, _ := gzipReaders.Get().(*gzip.Reader)
	var err error
	if z == nil {
		z, err = gzip.NewReader(compressed)
	} else {
		err = z.Reset(compressed)
	}
	if err != nil {
		return err
	}
	defer gzipReaders.Put(z)
	z.Multistream(false)
	// Data blocks copy every field out of the payload, so its buffer is reused.
	buf := &bytes.Buffer{}
	if _, ok := v.(*[]hta.Record); ok {
		buf = payloadBuffers.Get().(*bytes.Buffer)
		buf.Reset()
		defer payloadBuffers.Put(buf)
	}
	if _, err = buf.ReadFrom(io.LimitReader(z, limit+1)); err != nil {
		return err
	}
	if int64(buf.Len()) > limit {
		return fmt.Errorf("block payload exceeds size limit")
	}
	if compressed.Len() != 0 {
		return fmt.Errorf("trailing compressed block bytes")
	}
	return decodeBinaryPayload(buf.Bytes(), v)
}

func decodeBinaryPayload(b []byte, v any) error {
	switch value := v.(type) {
	case *[]hta.Record:
		records, err := decodeRecordsPayload(b)
		if err != nil {
			return err
		}
		*value = records
	case *indexNode:
		return decodeIndexPayload(b, value)
	case *map[string]map[int64]blob:
		return decodeRootPayload(b, value)
	case *heldDelta:
		return decodeHeldPayload(b, value)
	case *map[string]*hta.Series:
		return decodeStatePayload(b, value)
	default:
		return fmt.Errorf("unsupported binary block destination")
	}
	return nil
}

func decodeIndexPayload(b []byte, value *indexNode) error {
	if len(b) < 5 {
		return io.ErrUnexpectedEOF
	}
	count := int(binary.LittleEndian.Uint16(b[1:]))
	keyCount := int(binary.LittleEndian.Uint16(b[3:]))
	if b[0] > 1 || count > indexFanout || keyCount > count {
		return fmt.Errorf("invalid index header")
	}
	n := indexNode{Leaf: b[0] == 1}
	keys := make([]string, keyCount)
	pos := 5
	for i := range keys {
		if len(b)-pos < 2 {
			return io.ErrUnexpectedEOF
		}
		size := int(binary.LittleEndian.Uint16(b[pos:]))
		pos += 2
		if size > len(b)-pos {
			return io.ErrUnexpectedEOF
		}
		keys[i] = string(b[pos : pos+size])
		pos += size
	}
	if len(b)-pos != count*indexEntryBytes {
		return fmt.Errorf("invalid index block size")
	}
	if count > 0 {
		n.Entries = make([]indexEntry, count)
	}
	for i := range n.Entries {
		p := b[pos+i*indexEntryBytes:]
		id := int(binary.LittleEndian.Uint16(p[16:]))
		if id >= len(keys) {
			return fmt.Errorf("invalid index key reference")
		}
		entry := indexEntry{First: int64(binary.LittleEndian.Uint64(p)), Last: int64(binary.LittleEndian.Uint64(p[8:])), Records: int(binary.LittleEndian.Uint32(p[66:]))}
		entry.Blob = blob{Key: keys[id], Offset: int64(binary.LittleEndian.Uint64(p[18:])), Length: int64(binary.LittleEndian.Uint64(p[26:]))}
		copy(entry.Blob.Hash[:], p[34:66])
		f := func(k int) uint64 { return binary.LittleEndian.Uint64(p[70+8*k:]) }
		entry.agg = hta.Aggregate{Minimum: math.Float64frombits(f(0)), Maximum: math.Float64frombits(f(1)), Sum: math.Float64frombits(f(2)), Count: f(3), Integral: math.Float64frombits(f(4)), ActiveTime: int64(f(5))}
		n.Entries[i] = entry
	}
	*value = n
	return nil
}

// Data payload: uvarint record count and varint level, shared by all
// records of a block (one stream). Each record starts with its time as a
// varint delta from the previous record (the first from zero). A raw record
// (level 0) adds its value; it always has repeat 1 and no aggregate. An
// aggregate record (value 0) adds uvarint repeat, minimum, maximum and sum,
// uvarint count, integral and varint active time. Floats are IEEE-754 bits,
// little-endian. A raw value thus needs 9-18 bytes instead of 80.
func appendRecordsPayload(b []byte, records []hta.Record) ([]byte, error) {
	if len(records) > maxDataBlockRecords {
		return nil, fmt.Errorf("too many data records")
	}
	level := int64(0)
	if len(records) > 0 {
		level = records[0].Level
	}
	b = binary.AppendUvarint(b, uint64(len(records)))
	b = binary.AppendVarint(b, level)
	var previous int64
	for _, r := range records {
		if r.Level != level {
			return nil, fmt.Errorf("data block mixes levels %d and %d", level, r.Level)
		}
		b = binary.AppendVarint(b, r.Time-previous)
		previous = r.Time
		if level == 0 {
			if r.Repeat != 1 || r.Aggregate != (hta.Aggregate{}) {
				return nil, fmt.Errorf("raw record at %d carries repeat or aggregate", r.Time)
			}
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(r.Value))
			continue
		}
		if math.Float64bits(r.Value) != 0 {
			return nil, fmt.Errorf("aggregate record at %d carries a value", r.Time)
		}
		a := r.Aggregate
		b = binary.AppendUvarint(b, uint64(r.Repeat))
		for _, f := range [...]float64{a.Minimum, a.Maximum, a.Sum} {
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
		}
		b = binary.AppendUvarint(b, a.Count)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(a.Integral))
		b = binary.AppendVarint(b, a.ActiveTime)
	}
	return b, nil
}

func decodeRecordsPayload(b []byte) ([]hta.Record, error) {
	uvarint := func() (uint64, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		b = b[n:]
		return v, nil
	}
	varint := func() (int64, error) {
		v, n := binary.Varint(b)
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		b = b[n:]
		return v, nil
	}
	fixed := func() (uint64, error) {
		if len(b) < 8 {
			return 0, io.ErrUnexpectedEOF
		}
		v := binary.LittleEndian.Uint64(b)
		b = b[8:]
		return v, nil
	}
	count, err := uvarint()
	if err != nil {
		return nil, err
	}
	if count > maxDataBlockRecords {
		return nil, fmt.Errorf("too many data records")
	}
	level, err := varint()
	if err != nil {
		return nil, err
	}
	var records []hta.Record
	if count > 0 {
		records = make([]hta.Record, count)
	}
	var previous int64
	for i := range records {
		delta, err := varint()
		if err != nil {
			return nil, err
		}
		previous += delta
		r := &records[i]
		r.Time, r.Level, r.Repeat = previous, level, 1
		if level == 0 {
			v, err := fixed()
			if err != nil {
				return nil, err
			}
			r.Value = math.Float64frombits(v)
			continue
		}
		var f [6]uint64
		repeat, err := uvarint()
		for k := 0; k < 3 && err == nil; k++ {
			f[k], err = fixed()
		}
		if err == nil {
			f[3], err = uvarint()
		}
		if err == nil {
			f[4], err = fixed()
		}
		var active int64
		if err == nil {
			active, err = varint()
		}
		if err != nil {
			return nil, err
		}
		r.Repeat = int64(repeat)
		r.Aggregate = hta.Aggregate{Minimum: math.Float64frombits(f[0]), Maximum: math.Float64frombits(f[1]), Sum: math.Float64frombits(f[2]), Count: f[3], Integral: math.Float64frombits(f[4]), ActiveTime: active}
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("trailing data block bytes")
	}
	return records, nil
}

// Held records persisted between checkpoints, per stream: name, level and
// records with varint time deltas. Raw records (level 0, repeat 1) carry only
// their value; aggregate records their repeat and six aggregate fields.
// Fixed 80-byte records were larger than Gob, which omits zero fields.
func encodeHeldPayload(d heldDelta) ([]byte, error) {
	size := binary.MaxVarintLen64
	for _, st := range d.Streams {
		size += 3*binary.MaxVarintLen64 + len(st.Metric) + len(st.Records)*(binary.MaxVarintLen64+8)
		if st.Level > 0 {
			size += len(st.Records) * (3*binary.MaxVarintLen64 + 32)
		}
	}
	out := make([]byte, 0, size)
	out = binary.AppendUvarint(out, uint64(len(d.Streams)))
	for _, st := range d.Streams {
		out = binary.AppendUvarint(out, uint64(len(st.Metric)))
		out = append(out, st.Metric...)
		out = binary.AppendVarint(out, st.Level)
		out = binary.AppendUvarint(out, uint64(len(st.Records)))
		var previous int64
		for _, r := range st.Records {
			if r.Level != st.Level || (st.Level == 0 && r.Repeat != 1) {
				return nil, fmt.Errorf("held record does not match its stream")
			}
			out = binary.AppendVarint(out, r.Time-previous)
			previous = r.Time
			if st.Level == 0 {
				out = binary.LittleEndian.AppendUint64(out, math.Float64bits(r.Value))
				continue
			}
			a := r.Aggregate
			out = binary.AppendVarint(out, r.Repeat)
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Minimum))
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Maximum))
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Sum))
			out = binary.AppendUvarint(out, a.Count)
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Integral))
			out = binary.AppendVarint(out, a.ActiveTime)
		}
	}
	return out, nil
}

func decodeHeldPayload(b []byte, dst *heldDelta) error {
	pos := 0
	uvarint := func() (uint64, error) {
		v, n := binary.Uvarint(b[pos:])
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		pos += n
		return v, nil
	}
	varint := func() (int64, error) {
		v, n := binary.Varint(b[pos:])
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		pos += n
		return v, nil
	}
	float := func() (float64, error) {
		if len(b)-pos < 8 {
			return 0, io.ErrUnexpectedEOF
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(b[pos:]))
		pos += 8
		return v, nil
	}
	streams, err := uvarint()
	if err != nil {
		return err
	}
	// A stream needs at least three header bytes, a record nine.
	if streams > uint64(len(b)-pos)/3 {
		return fmt.Errorf("invalid held delta stream count")
	}
	d := heldDelta{Streams: make([]deltaStream, streams)}
	for i := range d.Streams {
		n, err := uvarint()
		if err != nil {
			return err
		}
		if n > uint64(len(b)-pos) {
			return io.ErrUnexpectedEOF
		}
		st := deltaStream{Metric: string(b[pos : pos+int(n)])}
		pos += int(n)
		if st.Level, err = varint(); err != nil {
			return err
		}
		count, err := uvarint()
		if err != nil {
			return err
		}
		if st.Level < 0 || count > uint64(len(b)-pos)/9 {
			return fmt.Errorf("invalid held delta stream")
		}
		st.Records = make([]hta.Record, count)
		var previous int64
		for j := range st.Records {
			delta, err := varint()
			if err != nil {
				return err
			}
			previous += delta
			r := hta.Record{Time: previous, Level: st.Level, Repeat: 1}
			if st.Level == 0 {
				if r.Value, err = float(); err != nil {
					return err
				}
				st.Records[j] = r
				continue
			}
			if r.Repeat, err = varint(); err != nil {
				return err
			}
			a := &r.Aggregate
			for _, f := range []*float64{&a.Minimum, &a.Maximum, &a.Sum} {
				if *f, err = float(); err != nil {
					return err
				}
			}
			if a.Count, err = uvarint(); err != nil {
				return err
			}
			if a.Integral, err = float(); err != nil {
				return err
			}
			if a.ActiveTime, err = varint(); err != nil {
				return err
			}
			st.Records[j] = r
		}
		d.Streams[i] = st
	}
	if pos != len(b) {
		return fmt.Errorf("trailing bytes in held delta")
	}
	*dst = d
	return nil
}

// A checkpoint state page holds the aggregation state of its metrics: name,
// configuration, first and last sample and the open interval of every level,
// in name order. Times and counts are varints, values IEEE doubles.
func encodeStatePayload(m map[string]*hta.Series) ([]byte, error) {
	names := make([]string, 0, len(m))
	for name, s := range m {
		if s == nil {
			return nil, fmt.Errorf("nil series %q", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	out := binary.AppendUvarint(nil, uint64(len(names)))
	for _, name := range names {
		s := m[name]
		out = binary.AppendUvarint(out, uint64(len(name)))
		out = append(out, name...)
		out = binary.AppendUvarint(out, uint64(len(s.Config.Input)))
		out = append(out, s.Config.Input...)
		for _, v := range []int64{s.Config.IntervalMin, s.Config.IntervalMax, s.Config.IntervalFactor, s.First.Time} {
			out = binary.AppendVarint(out, v)
		}
		out = binary.LittleEndian.AppendUint64(out, math.Float64bits(s.First.Value))
		out = binary.AppendVarint(out, s.Last.Time)
		out = binary.LittleEndian.AppendUint64(out, math.Float64bits(s.Last.Value))
		levels := make([]int64, 0, len(s.Levels))
		for level := range s.Levels {
			levels = append(levels, level)
		}
		sort.Slice(levels, func(i, j int) bool { return levels[i] < levels[j] })
		out = binary.AppendUvarint(out, uint64(len(levels)))
		for _, level := range levels {
			l := s.Levels[level]
			out = binary.AppendVarint(out, level)
			out = binary.AppendVarint(out, l.Time)
			out = appendAggregate(out, l.Aggregate)
		}
	}
	return out, nil
}

func appendAggregate(out []byte, a hta.Aggregate) []byte {
	out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Minimum))
	out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Maximum))
	out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Sum))
	out = binary.AppendUvarint(out, a.Count)
	out = binary.LittleEndian.AppendUint64(out, math.Float64bits(a.Integral))
	return binary.AppendVarint(out, a.ActiveTime)
}

// payloadReader decodes varints and doubles with bounds checks.
type payloadReader struct {
	b   []byte
	pos int
	err error
}

func (r *payloadReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	r.pos += n
	return v
}

func (r *payloadReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b[r.pos:])
	if n <= 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	r.pos += n
	return v
}

func (r *payloadReader) float() float64 {
	if r.err != nil || len(r.b)-r.pos < 8 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.b[r.pos:]))
	r.pos += 8
	return v
}

// count reads a length of items needing at least unit bytes each.
func (r *payloadReader) count(unit int) int {
	n := r.uvarint()
	if r.err == nil && n > uint64(len(r.b)-r.pos)/uint64(unit) {
		r.err = fmt.Errorf("invalid payload length")
	}
	if r.err != nil {
		return 0
	}
	return int(n)
}

func (r *payloadReader) bytes(n int) string {
	if r.err != nil || n > len(r.b)-r.pos {
		r.err = io.ErrUnexpectedEOF
		return ""
	}
	v := string(r.b[r.pos : r.pos+n])
	r.pos += n
	return v
}

func (r *payloadReader) aggregate() hta.Aggregate {
	return hta.Aggregate{Minimum: r.float(), Maximum: r.float(), Sum: r.float(), Count: r.uvarint(), Integral: r.float(), ActiveTime: r.varint()}
}

func decodeStatePayload(b []byte, dst *map[string]*hta.Series) error {
	r := &payloadReader{b: b}
	// A series needs at least 24 bytes, a level 36.
	n := r.count(24)
	m := make(map[string]*hta.Series, n)
	for i := 0; i < n && r.err == nil; i++ {
		name := r.bytes(r.count(1))
		s := &hta.Series{}
		s.Config.Input = r.bytes(r.count(1))
		s.Config.IntervalMin, s.Config.IntervalMax, s.Config.IntervalFactor = r.varint(), r.varint(), r.varint()
		s.First = hta.Point{Time: r.varint(), Value: r.float()}
		s.Last = hta.Point{Time: r.varint(), Value: r.float()}
		levels := r.count(36)
		s.Levels = make(map[int64]hta.Level, levels)
		for j := 0; j < levels && r.err == nil; j++ {
			level := r.varint()
			s.Levels[level] = hta.Level{Time: r.varint(), Aggregate: r.aggregate()}
		}
		if _, dup := m[name]; dup && r.err == nil {
			r.err = fmt.Errorf("duplicate series %q", name)
		}
		m[name] = s
	}
	if r.err != nil {
		return r.err
	}
	if r.pos != len(b) {
		return fmt.Errorf("trailing bytes in state page")
	}
	*dst = m
	return nil
}
