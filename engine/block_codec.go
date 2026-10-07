package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/gzip"
	"io"
	"math"
	"sort"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// The envelope identifies a version and payload kind before decompression.
// Each range still has its own gzip stream and SHA-256 in the owning index.
const blockMagic = "MQHB"
const blockVersion byte = 1
const recordKind byte = 1

const indexKind byte = 6 // kind 2 were index pages without entry aggregates
const rootKind byte = 3
const heldKind byte = 4
const stateKind byte = 5

// maxHeldPayloadBytes bounds a decoded held delta, as the Gob decoder did.
const maxHeldPayloadBytes = 512 << 20
const recordWireBytes = 80
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
		payload = make([]byte, 0, 4+len(value)*recordWireBytes)
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(value)))
		for _, r := range value {
			payload = appendRecordWire(payload, r)
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
		limit = 4 + maxDataBlockRecords*recordWireBytes
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
	z, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}
	defer z.Close()
	z.Multistream(false)
	if dst, ok := v.(*[]hta.Record); ok {
		var records []hta.Record
		if err := decodeRecordStream(z, &records); err != nil {
			return err
		}
		if compressed.Len() != 0 {
			return fmt.Errorf("trailing compressed block bytes")
		}
		*dst = records
		return nil
	}
	payload, err := io.ReadAll(io.LimitReader(z, limit+1))
	if err != nil {
		return err
	}
	if int64(len(payload)) > limit {
		return fmt.Errorf("block payload exceeds size limit")
	}
	if compressed.Len() != 0 {
		return fmt.Errorf("trailing compressed block bytes")
	}
	return decodeBinaryPayload(payload, v)
}

func decodeBinaryPayload(b []byte, v any) error {
	switch value := v.(type) {
	case *[]hta.Record:
		if len(b) < 4 {
			return io.ErrUnexpectedEOF
		}
		count64 := binary.LittleEndian.Uint32(b)
		if count64 > maxDataBlockRecords {
			return fmt.Errorf("too many data records")
		}
		count := int(count64)
		if len(b) != 4+count*recordWireBytes {
			return fmt.Errorf("invalid data block size")
		}
		var records []hta.Record
		if count > 0 {
			records = make([]hta.Record, count)
		}
		for i := range records {
			records[i] = recordFromWire(b[4+i*recordWireBytes:])
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

// appendRecordWire appends the fixed little-endian fields of one record.
func appendRecordWire(payload []byte, r hta.Record) []byte {
	fields := [...]uint64{uint64(r.Time), uint64(r.Level), uint64(r.Repeat), math.Float64bits(r.Value), math.Float64bits(r.Aggregate.Minimum), math.Float64bits(r.Aggregate.Maximum), math.Float64bits(r.Aggregate.Sum), r.Aggregate.Count, math.Float64bits(r.Aggregate.Integral), uint64(r.Aggregate.ActiveTime)}
	for _, f := range fields {
		payload = binary.LittleEndian.AppendUint64(payload, f)
	}
	return payload
}

func recordFromWire(p []byte) hta.Record {
	var f [10]uint64
	for j := range f {
		f[j] = binary.LittleEndian.Uint64(p[j*8:])
	}
	return hta.Record{Time: int64(f[0]), Level: int64(f[1]), Repeat: int64(f[2]), Value: math.Float64frombits(f[3]), Aggregate: hta.Aggregate{Minimum: math.Float64frombits(f[4]), Maximum: math.Float64frombits(f[5]), Sum: math.Float64frombits(f[6]), Count: f[7], Integral: math.Float64frombits(f[8]), ActiveTime: int64(f[9])}}
}

// Decode bounded chunks directly into records, avoiding a second full-block
// payload allocation. Assign the destination only after gzip's trailer/CRC and
// the exact payload length have been checked.
func decodeRecordStream(z io.Reader, dst *[]hta.Record) error {
	var header [4]byte
	if _, err := io.ReadFull(z, header[:]); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(header[:])
	if count > maxDataBlockRecords {
		return fmt.Errorf("too many data records")
	}
	var records []hta.Record
	if count > 0 {
		records = make([]hta.Record, int(count))
	}
	var buf [16 * recordWireBytes]byte
	for start := 0; start < len(records); start += 16 {
		n := min(16, len(records)-start)
		if _, err := io.ReadFull(z, buf[:n*recordWireBytes]); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			records[start+i] = recordFromWire(buf[i*recordWireBytes:])
		}
	}
	var extra [1]byte
	if _, err := io.ReadFull(z, extra[:]); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("trailing data block payload")
	}
	*dst = records
	return nil
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
