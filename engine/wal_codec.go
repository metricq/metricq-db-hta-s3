package engine

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// WAL frames carry one accepted delivery each. Gob plus gzip cost tens of
// microseconds per frame (type descriptors and Huffman tables per frame),
// which dominated ingestion of single-sample deliveries under the engine
// mutex. Frames are CRC-protected already; this format is uncompressed with
// varint time deltas.
const walMagic = "MQHW"
const walVersion byte = 1

func encodeWALBatch(b batch) []byte {
	out := make([]byte, 0, len(walMagic)+1+5*binary.MaxVarintLen64+len(b.Metric)+len(b.Points)*(binary.MaxVarintLen64+8))
	out = append(out, walMagic...)
	out = append(out, walVersion)
	out = binary.AppendVarint(out, b.ReceivedAt)
	out = binary.AppendVarint(out, b.Config.IntervalMin)
	out = binary.AppendVarint(out, b.Config.IntervalMax)
	out = binary.AppendVarint(out, b.Config.IntervalFactor)
	out = binary.AppendUvarint(out, uint64(len(b.Metric)))
	out = append(out, b.Metric...)
	out = binary.AppendUvarint(out, uint64(len(b.Points)))
	var previous int64
	for _, p := range b.Points {
		out = binary.AppendVarint(out, p.Time-previous)
		out = binary.LittleEndian.AppendUint64(out, math.Float64bits(p.Value))
		previous = p.Time
	}
	return out
}

func decodeWALBatch(b []byte, v *batch) error {
	if len(b) < len(walMagic)+1 || b[len(walMagic)] != walVersion {
		return fmt.Errorf("unsupported WAL frame version")
	}
	b = b[len(walMagic)+1:]
	varint := func() (int64, error) {
		n, size := binary.Varint(b)
		if size <= 0 {
			return 0, fmt.Errorf("truncated WAL frame")
		}
		b = b[size:]
		return n, nil
	}
	// count reads a length; each counted item needs at least unit bytes of
	// the remaining frame.
	count := func(unit uint64) (uint64, error) {
		n, size := binary.Uvarint(b)
		if size <= 0 {
			return 0, fmt.Errorf("truncated WAL frame")
		}
		b = b[size:]
		if n > uint64(len(b))/unit {
			return 0, fmt.Errorf("invalid WAL frame length")
		}
		return n, nil
	}
	var out batch
	var err error
	fields := []*int64{&out.ReceivedAt, &out.Config.IntervalMin, &out.Config.IntervalMax, &out.Config.IntervalFactor}
	for _, f := range fields {
		if *f, err = varint(); err != nil {
			return err
		}
	}
	length, err := count(1)
	if err != nil {
		return err
	}
	out.Metric = string(b[:length])
	b = b[length:]
	// Each point needs at least one varint byte and eight value bytes.
	points, err := count(9)
	if err != nil {
		return err
	}
	out.Points = make([]hta.Point, points)
	var previous int64
	for i := range out.Points {
		delta, err := varint()
		if err != nil {
			return err
		}
		if len(b) < 8 {
			return fmt.Errorf("truncated WAL frame")
		}
		previous += delta
		out.Points[i] = hta.Point{Time: previous, Value: math.Float64frombits(binary.LittleEndian.Uint64(b))}
		b = b[8:]
	}
	if len(b) != 0 {
		return fmt.Errorf("trailing bytes in WAL frame")
	}
	*v = out
	return nil
}
