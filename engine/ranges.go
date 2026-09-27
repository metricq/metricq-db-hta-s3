package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"sync"
)

// fetchEncoded preserves logical order while fetching physical object ranges.
// Every contained block is independently checked before it can be published.
// Callers bound the number and total size of refs (query batch or maintenance job).
func (e *Engine) fetchEncoded(ctx context.Context, refs []blob) ([][]byte, error) {
	return e.fetchEncodedPaced(ctx, refs, nil)
}
func (e *Engine) fetchEncodedPaced(ctx context.Context, refs []blob, pace func(int64) error) ([][]byte, error) {
	order := make([]int, len(refs))
	for i, r := range refs {
		if r.Key == "" || r.Offset < 0 || r.Length <= 0 || r.Length > 512<<20 || r.Offset > math.MaxInt64-r.Length {
			return nil, fmt.Errorf("invalid object block reference")
		}
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := refs[order[i]], refs[order[j]]
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.Offset < b.Offset
	})
	type span struct {
		key        string
		begin, end int64
		indices    []int
	}
	var spans []span
	for _, i := range order {
		r := refs[i]
		if len(spans) > 0 {
			last := &spans[len(spans)-1]
			end := max(last.end, r.Offset+r.Length)
			if last.key == r.Key && r.Offset-last.end <= maxCoalescedGap && end-last.begin <= maxCoalescedRange {
				last.end = end
				last.indices = append(last.indices, i)
				continue
			}
		}
		spans = append(spans, span{r.Key, r.Offset, r.Offset + r.Length, []int{i}})
	}
	out := make([][]byte, len(refs))
	for start := 0; start < len(spans); start += maxParallelBlockFetches {
		batch := spans[start:min(start+maxParallelBlockFetches, len(spans))]
		errs := make([]error, len(batch))
		var wg sync.WaitGroup
		for k, s := range batch {
			wg.Add(1)
			go func(k int, s span) {
				defer wg.Done()
				if pace != nil {
					if errs[k] = pace(s.end - s.begin); errs[k] != nil {
						return
					}
				}
				b, err := e.readRange(ctx, s.key, s.begin, s.end-s.begin)
				if err != nil {
					errs[k] = err
					return
				}
				for _, i := range s.indices {
					r := refs[i]
					part := b[r.Offset-s.begin : r.Offset-s.begin+r.Length]
					if sha256.Sum256(part) != r.Hash {
						errs[k] = fmt.Errorf("object block checksum mismatch: %s@%d", r.Key, r.Offset)
						return
					}
					out[i] = part
				}
			}(k, s)
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
