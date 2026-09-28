package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const maxRootPayloadBytes = 32 << 20
const maxRootMetrics = 65535
const maxRootReferences = maxRootPayloadBytes / 60

// Root pages share object keys and store each metric/level exactly once. Sort
// maps explicitly so encoding is stable, independent of Go map iteration.
func encodeRootPayload(roots map[string]map[int64]blob) ([]byte, error) {
	if len(roots) > maxRootMetrics {
		return nil, fmt.Errorf("too many root metrics")
	}
	names := make([]string, 0, len(roots))
	for name := range roots {
		names = append(names, name)
	}
	sort.Strings(names)
	keys := []string{}
	ids := map[string]uint32{}
	size, refs := 8, 0
	for _, name := range names {
		levels := roots[name]
		if len(name) > 65535 || len(levels) > 65535 || levels == nil {
			return nil, fmt.Errorf("invalid root metric size")
		}
		size += 4 + len(name) + len(levels)*60
		refs += len(levels)
		if size > maxRootPayloadBytes || refs > maxRootReferences {
			return nil, fmt.Errorf("root page exceeds size limit")
		}
		for _, ref := range levels {
			if len(ref.Key) > 65535 {
				return nil, fmt.Errorf("root key too large")
			}
			if _, ok := ids[ref.Key]; !ok {
				ids[ref.Key] = 0
				keys = append(keys, ref.Key)
				size += 2 + len(ref.Key)
				if size > maxRootPayloadBytes {
					return nil, fmt.Errorf("root page exceeds size limit")
				}
			}
		}
	}
	if size > maxRootPayloadBytes || refs > maxRootReferences {
		return nil, fmt.Errorf("root page exceeds size limit")
	}
	sort.Strings(keys)
	payload := make([]byte, 0, size)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(names)))
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(keys)))
	for i, key := range keys {
		ids[key] = uint32(i)
		payload = binary.LittleEndian.AppendUint16(payload, uint16(len(key)))
		payload = append(payload, key...)
	}
	for _, name := range names {
		payload = binary.LittleEndian.AppendUint16(payload, uint16(len(name)))
		payload = append(payload, name...)
		levels := make([]int64, 0, len(roots[name]))
		for level := range roots[name] {
			levels = append(levels, level)
		}
		sort.Slice(levels, func(i, j int) bool { return levels[i] < levels[j] })
		payload = binary.LittleEndian.AppendUint16(payload, uint16(len(levels)))
		for _, level := range levels {
			ref := roots[name][level]
			payload = binary.LittleEndian.AppendUint64(payload, uint64(level))
			payload = binary.LittleEndian.AppendUint32(payload, ids[ref.Key])
			payload = binary.LittleEndian.AppendUint64(payload, uint64(ref.Offset))
			payload = binary.LittleEndian.AppendUint64(payload, uint64(ref.Length))
			payload = append(payload, ref.Hash[:]...)
		}
	}
	return payload, nil
}

func decodeRootPayload(payload []byte, dst *map[string]map[int64]blob) error {
	if len(payload) < 8 {
		return io.ErrUnexpectedEOF
	}
	if len(payload) > maxRootPayloadBytes {
		return fmt.Errorf("root page exceeds size limit")
	}
	count, keyCount := binary.LittleEndian.Uint32(payload), binary.LittleEndian.Uint32(payload[4:])
	if count > maxRootMetrics || keyCount > maxRootReferences || uint64(keyCount)*2 > uint64(len(payload)-8) {
		return fmt.Errorf("invalid root header")
	}
	pos := 8
	text := func() (string, error) {
		if len(payload)-pos < 2 {
			return "", io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint16(payload[pos:]))
		pos += 2
		if n > len(payload)-pos {
			return "", io.ErrUnexpectedEOF
		}
		s := string(payload[pos : pos+n])
		pos += n
		return s, nil
	}
	keys := make([]string, int(keyCount))
	for i := range keys {
		key, err := text()
		if err != nil {
			return err
		}
		if i > 0 && key <= keys[i-1] {
			return fmt.Errorf("unordered root dictionary")
		}
		keys[i] = key
	}
	if uint64(count)*4 > uint64(len(payload)-pos) {
		return fmt.Errorf("invalid root metric count")
	}
	var roots map[string]map[int64]blob
	if count > 0 {
		roots = make(map[string]map[int64]blob, int(count))
	}
	previous := ""
	references := 0
	for i := uint32(0); i < count; i++ {
		name, err := text()
		if err != nil {
			return err
		}
		if i > 0 && name <= previous {
			return fmt.Errorf("unordered root metrics")
		}
		previous = name
		if len(payload)-pos < 2 {
			return io.ErrUnexpectedEOF
		}
		levels := int(binary.LittleEndian.Uint16(payload[pos:]))
		pos += 2
		if levels > (len(payload)-pos)/60 || levels > maxRootReferences-references {
			return fmt.Errorf("invalid root level count")
		}
		references += levels
		roots[name] = make(map[int64]blob, levels)
		var prior int64
		for j := 0; j < levels; j++ {
			p := payload[pos : pos+60]
			pos += 60
			level := int64(binary.LittleEndian.Uint64(p))
			id := binary.LittleEndian.Uint32(p[8:])
			if id >= uint32(len(keys)) || (j > 0 && level <= prior) {
				return fmt.Errorf("invalid root level/key")
			}
			prior = level
			ref := blob{Key: keys[id], Offset: int64(binary.LittleEndian.Uint64(p[12:])), Length: int64(binary.LittleEndian.Uint64(p[20:]))}
			copy(ref.Hash[:], p[28:60])
			roots[name][level] = ref
		}
	}
	if pos != len(payload) {
		return fmt.Errorf("trailing root payload")
	}
	*dst = roots
	return nil
}
