package engine

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// The envelope identifies a version and payload kind before decompression.
// Each range still has its own gzip stream and SHA-256 in the owning index.
const blockMagic = "MQHB"
const blockVersion byte = 1
const recordKind byte = 1
const indexKind byte = 2
const rootKind byte = 3
const recordWireBytes = 80
const maxIndexKeyBytes = 65535

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
			fields := [...]uint64{uint64(r.Time), uint64(r.Level), uint64(r.Repeat), math.Float64bits(r.Value), math.Float64bits(r.Aggregate.Minimum), math.Float64bits(r.Aggregate.Maximum), math.Float64bits(r.Aggregate.Sum), r.Aggregate.Count, math.Float64bits(r.Aggregate.Integral), uint64(r.Aggregate.ActiveTime)}
			for _, f := range fields {
				payload = binary.LittleEndian.AppendUint64(payload, f)
			}
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
		size := 5 + len(value.Entries)*70
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
		limit = 5 + indexFanout*(maxIndexKeyBytes+2+70)
	case *map[string]map[int64]blob:
		if kind != rootKind {
			return fmt.Errorf("block payload type mismatch")
		}
		limit = maxRootPayloadBytes
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
		if len(b)-pos != count*70 {
			return fmt.Errorf("invalid index block size")
		}
		if count > 0 {
			n.Entries = make([]indexEntry, count)
		}
		for i := range n.Entries {
			p := b[pos+i*70:]
			id := int(binary.LittleEndian.Uint16(p[16:]))
			if id >= len(keys) {
				return fmt.Errorf("invalid index key reference")
			}
			entry := indexEntry{First: int64(binary.LittleEndian.Uint64(p)), Last: int64(binary.LittleEndian.Uint64(p[8:])), Records: int(binary.LittleEndian.Uint32(p[66:]))}
			entry.Blob = blob{Key: keys[id], Offset: int64(binary.LittleEndian.Uint64(p[18:])), Length: int64(binary.LittleEndian.Uint64(p[26:]))}
			copy(entry.Blob.Hash[:], p[34:66])
			n.Entries[i] = entry
		}
		*value = n
	case *map[string]map[int64]blob:
		return decodeRootPayload(b, value)
	default:
		return fmt.Errorf("unsupported binary block destination")
	}
	return nil
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
