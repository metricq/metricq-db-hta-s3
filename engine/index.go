package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/metricq/metricq-db-hta-go/storage"
)

const indexFanout = 64
const maxDataBlockRecords = 1024

// A blob is one independently compressed block inside an immutable S3 pack.
// Its checksum covers exactly the addressed byte range.
type blob struct {
	Key    string
	Offset int64
	Length int64
	Hash   [32]byte
}

type indexEntry struct {
	First, Last int64
	Blob        blob
	Records     int // Physical records in a data block; zero for internal edges.
}

type indexNode struct {
	Leaf    bool
	Entries []indexEntry
}

type pack struct {
	key string
	buf bytes.Buffer
}

func newPack(prefix string) (*pack, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	return &pack{key: prefix + "/" + hex.EncodeToString(id[:])}, nil
}

func (p *pack) add(b []byte) blob {
	r := blob{Key: p.key, Offset: int64(p.buf.Len()), Length: int64(len(b)), Hash: sha256.Sum256(b)}
	p.buf.Write(b)
	return r
}

func (e *Engine) readBlob(ctx context.Context, r blob) ([]byte, error) {
	if r.Key == "" || r.Offset < 0 || r.Length <= 0 || r.Length > 512<<20 {
		return nil, fmt.Errorf("invalid object block reference")
	}
	start := time.Now()
	defer func() { e.metrics.StoreGet.Observe(time.Since(start).Seconds()) }()
	var b []byte
	var err error
	if s, ok := e.store.(storage.RangeGetter); ok {
		b, err = s.GetRange(ctx, r.Key, r.Offset, r.Length)
	} else {
		var whole []byte
		whole, _, err = e.store.Get(ctx, r.Key)
		if err == nil {
			if r.Offset > int64(len(whole)) || r.Length > int64(len(whole))-r.Offset {
				return nil, io.ErrUnexpectedEOF
			}
			b = whole[r.Offset : r.Offset+r.Length]
		}
	}
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != r.Length || sha256.Sum256(b) != r.Hash {
		return nil, fmt.Errorf("object block checksum mismatch: %s@%d", r.Key, r.Offset)
	}
	return b, nil
}

func (e *Engine) readNode(ctx context.Context, r blob) (indexNode, error) {
	if e.nodeCache != nil {
		if n, ok := e.nodeCache[r]; ok {
			return n, nil
		}
	}
	if e.sharedNodes != nil {
		if n, ok := e.sharedNodes.get(r); ok {
			if e.nodeCache != nil {
				e.nodeCache[r] = n
			}
			return n, nil
		}
	}
	b, err := e.readBlob(ctx, r)
	if err != nil {
		return indexNode{}, err
	}
	n, err := decodeNode(b)
	if err == nil {
		if e.nodeCache != nil {
			e.nodeCache[r] = n
		}
		if e.sharedNodes != nil {
			e.sharedNodes.add(r, n)
		}
	}
	return n, err
}

func decodeNode(b []byte) (indexNode, error) {
	var n indexNode
	if err := decode(b, &n); err != nil {
		return n, err
	}
	if len(n.Entries) == 0 || len(n.Entries) > indexFanout {
		return n, fmt.Errorf("invalid index node")
	}
	return n, nil
}

func writeNode(p *pack, n indexNode) (indexEntry, error) {
	b, err := encode(n)
	if err != nil {
		return indexEntry{}, err
	}
	return indexEntry{First: n.Entries[0].First, Last: n.Entries[len(n.Entries)-1].Last, Blob: p.add(b)}, nil
}

// appendIndex copies the published rightmost path once per batch. Only final
// pages enter the pack; snapshots still refer to their immutable old roots.
func (e *Engine) appendIndex(ctx context.Context, root blob, items []indexEntry, p *pack) (blob, error) {
	return e.updateIndex(ctx, root, items, p, false)
}

func (e *Engine) updateIndex(ctx context.Context, root blob, items []indexEntry, p *pack, replaceTail bool) (blob, error) {
	if len(items) == 0 {
		if replaceTail {
			return blob{}, fmt.Errorf("empty index tail replacement")
		}
		return root, nil
	}
	for i := 1; i < len(items); i++ {
		if items[i].First < items[i-1].First {
			return blob{}, fmt.Errorf("index time went backwards")
		}
	}
	edges, err := e.appendNode(ctx, root, items, p, replaceTail)
	if err != nil {
		return blob{}, err
	}
	for len(edges) > 1 {
		edges, err = writeNodes(p, false, edges)
		if err != nil {
			return blob{}, err
		}
	}
	return edges[0].Blob, nil
}

func writeNodes(p *pack, leaf bool, items []indexEntry) ([]indexEntry, error) {
	edges := make([]indexEntry, 0, (len(items)+indexFanout-1)/indexFanout)
	for start := 0; start < len(items); start += indexFanout {
		edge, err := writeNode(p, indexNode{Leaf: leaf, Entries: items[start:min(start+indexFanout, len(items))]})
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func (e *Engine) appendNode(ctx context.Context, ptr blob, items []indexEntry, p *pack, replaceTail bool) ([]indexEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ptr.Key == "" {
		if replaceTail {
			return nil, fmt.Errorf("missing index tail")
		}
		return writeNodes(p, true, items)
	}
	n, err := e.readNode(ctx, ptr)
	if err != nil {
		return nil, err
	}
	if n.Leaf {
		if replaceTail {
			last := n.Entries[len(n.Entries)-1]
			if items[0].First != last.First || items[len(items)-1].Last < last.Last {
				return nil, fmt.Errorf("index tail replacement loses time range")
			}
			combined := append(append([]indexEntry(nil), n.Entries[:len(n.Entries)-1]...), items...)
			return writeNodes(p, true, combined)
		}
		if items[0].First < n.Entries[len(n.Entries)-1].First {
			return nil, fmt.Errorf("index time went backwards")
		}
		if len(n.Entries) == indexFanout {
			edges, err := writeNodes(p, true, items)
			return append([]indexEntry{{First: n.Entries[0].First, Last: n.Entries[len(n.Entries)-1].Last, Blob: ptr}}, edges...), err
		}
		combined := append(append([]indexEntry(nil), n.Entries...), items...)
		return writeNodes(p, true, combined)
	}
	last := len(n.Entries) - 1
	children, err := e.appendNode(ctx, n.Entries[last].Blob, items, p, replaceTail)
	if err != nil {
		return nil, err
	}
	if len(n.Entries) == indexFanout && children[0] == n.Entries[last] {
		// The old full page is unchanged; only its new siblings need writing.
		edges, err := writeNodes(p, false, children[1:])
		return append([]indexEntry{{First: n.Entries[0].First, Last: n.Entries[last].Last, Blob: ptr}}, edges...), err
	}
	combined := append(append([]indexEntry(nil), n.Entries[:last]...), children...)
	return writeNodes(p, false, combined)
}

func (e *Engine) lastIndexEntry(ctx context.Context, ptr blob) (indexEntry, error) {
	for ptr.Key != "" {
		n, err := e.readNode(ctx, ptr)
		if err != nil {
			return indexEntry{}, err
		}
		last := n.Entries[len(n.Entries)-1]
		if n.Leaf {
			return last, nil
		}
		ptr = last.Blob
	}
	return indexEntry{}, nil
}

func (e *Engine) indexRange(ctx context.Context, ptr blob, begin, end int64, out *[]blob) error {
	if ptr.Key == "" {
		return nil
	}
	n, err := e.readNode(ctx, ptr)
	if err != nil {
		return err
	}
	for _, edge := range n.Entries {
		if edge.Last < begin || edge.First > end {
			continue
		}
		if n.Leaf {
			*out = append(*out, edge.Blob)
		} else if err := e.indexRange(ctx, edge.Blob, begin, end, out); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) indexNeighbor(ctx context.Context, ptr blob, timePoint int64, before bool) (blob, error) {
	if ptr.Key == "" {
		return blob{}, nil
	}
	n, err := e.readNode(ctx, ptr)
	if err != nil {
		return blob{}, err
	}
	if before {
		for i := len(n.Entries) - 1; i >= 0; i-- {
			if n.Entries[i].First >= timePoint {
				continue
			}
			if n.Leaf {
				return n.Entries[i].Blob, nil
			}
			if found, err := e.indexNeighbor(ctx, n.Entries[i].Blob, timePoint, before); err != nil || found.Key != "" {
				return found, err
			}
		}
	} else {
		for _, edge := range n.Entries {
			if edge.Last <= timePoint {
				continue
			}
			if n.Leaf {
				if edge.First > timePoint {
					return edge.Blob, nil
				}
				continue
			}
			if found, err := e.indexNeighbor(ctx, edge.Blob, timePoint, before); err != nil || found.Key != "" {
				return found, err
			}
		}
	}
	return blob{}, nil
}
