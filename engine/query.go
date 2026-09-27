package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metricq/metricq-db-hta-go/hta"
	metricq "github.com/metricq/metricq-go"
)

type reader struct {
	e      *Engine
	ctx    context.Context
	metric string
	cache  map[blob][]hta.Record
	bytes  int64
	budget *atomic.Int64
}

// Bound request fan-out, compressed ranges and decoded records independently.
const maxParallelBlockFetches = 8
const maxQueryBlockBatch = 128
const maxCoalescedRange = 8 << 20
const maxCoalescedGap = 64 << 10

func recordsCost(n int) int64 { return int64(n) * 128 }

func (q *reader) records(level, begin, end int64) ([]hta.Record, error) {
	root := q.e.state.Roots[q.metric][level]
	var refs []blob
	lookupBegin, lookupEnd := begin, end
	if level > 0 {
		if begin >= end {
			return nil, nil
		}
		// FLEX includes the bucket containing begin, including empty runs.
		lookupBegin = begin - begin%level
		lookupEnd = end - 1
	} else {
		if prior, err := q.e.indexNeighbor(q.ctx, root, begin, true); err != nil {
			return nil, err
		} else if prior.Key != "" {
			refs = append(refs, prior)
		}
	}
	if err := q.e.indexRange(q.ctx, root, lookupBegin, lookupEnd, &refs); err != nil {
		return nil, err
	}
	if level == 0 {
		if next, err := q.e.indexNeighbor(q.ctx, root, end, false); err != nil {
			return nil, err
		} else if next.Key != "" {
			refs = append(refs, next)
		}
	}
	dedup := refs[:0]
	seen := make(map[blob]bool, len(refs))
	for _, ref := range refs {
		if !seen[ref] {
			seen[ref] = true
			dedup = append(dedup, ref)
		}
	}
	refs = dedup
	var out []hta.Record
	for start := 0; start < len(refs); start += maxQueryBlockBatch {
		if err := q.ctx.Err(); err != nil {
			return nil, err
		}
		batch := refs[start:min(start+maxQueryBlockBatch, len(refs))]
		blocks, err := q.fetchBlocks(batch)
		if err != nil {
			return nil, err
		}
		for _, entries := range blocks {
			out = append(out, entries...)
		}
	}
	out = append(out, q.e.flushing.stream(q.metric, level)...)
	out = append(out, q.e.pending.stream(q.metric, level)...)
	return out, nil
}

// fetchBlocks resolves each ref from the per-query cache, then the
// cross-query shared cache, then fetches remaining misses from the store
// concurrently. Every ref newly entering the per-query cache this call is
// charged against the query memory budget together, so a scan across many
// small blocks still bails out promptly instead of assembling everything
// first.
func (q *reader) fetchBlocks(refs []blob) ([][]hta.Record, error) {
	blocks := make([][]hta.Record, len(refs))
	var toFetch, charged []int
	for i, ref := range refs {
		if entries, ok := q.cache[ref]; ok {
			blocks[i] = entries
			continue
		}
		if q.e.sharedBlocks != nil {
			if entries, ok := q.e.sharedBlocks.get(ref); ok {
				blocks[i] = entries
				q.cache[ref] = entries
				charged = append(charged, i)
				continue
			}
		}
		toFetch = append(toFetch, i)
	}
	if len(toFetch) > 0 {
		if err := q.fetchRanges(refs, toFetch, blocks); err != nil {
			return nil, err
		}
		charged = append(charged, toFetch...)
	}
	var cost int64
	for _, i := range charged {
		cost += recordsCost(len(blocks[i]))
	}
	if cost > 0 {
		q.bytes += cost
		used := q.bytes
		if q.budget != nil {
			used = q.budget.Add(cost)
		}
		if used > 256<<20 {
			return nil, fmt.Errorf("query object memory budget exceeded; narrow the time range")
		}
	}
	for _, i := range toFetch {
		ref, entries := refs[i], blocks[i]
		q.cache[ref] = entries
		if q.e.sharedBlocks != nil {
			q.e.sharedBlocks.add(ref, entries, recordsCost(len(entries)))
		}
	}
	return blocks, nil
}

// Group cache misses by object and offset, then restore their original order.
// Each block keeps its own checksum; bytes in small gaps are never decoded.
func (q *reader) fetchRanges(refs []blob, misses []int, blocks [][]hta.Record) error {
	order := append([]int(nil), misses...)
	for _, i := range order {
		r := refs[i]
		if r.Key == "" || r.Offset < 0 || r.Length <= 0 || r.Length > 512<<20 || r.Offset > math.MaxInt64-r.Length {
			return fmt.Errorf("invalid object block reference")
		}
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
		spans = append(spans, span{key: r.Key, begin: r.Offset, end: r.Offset + r.Length, indices: []int{i}})
	}
	// A coalesced range saves requests, but its blocks still verify and decode
	// in parallel; serial decoding made multi-block raw windows slower.
	blockErrs := make([]error, len(refs))
	decoders := make(chan struct{}, maxParallelBlockFetches)
	for start := 0; start < len(spans); start += maxParallelBlockFetches {
		batch := spans[start:min(start+maxParallelBlockFetches, len(spans))]
		errs := make([]error, len(batch))
		var wg sync.WaitGroup
		for k, s := range batch {
			wg.Add(1)
			go func(k int, s span) {
				defer wg.Done()
				b, err := q.e.readRange(q.ctx, s.key, s.begin, s.end-s.begin)
				if err != nil {
					errs[k] = err
					return
				}
				for _, i := range s.indices {
					wg.Add(1)
					decoders <- struct{}{}
					go func(i int) {
						defer func() { <-decoders; wg.Done() }()
						r := refs[i]
						part := b[r.Offset-s.begin : r.Offset-s.begin+r.Length]
						if sha256.Sum256(part) != r.Hash {
							blockErrs[i] = fmt.Errorf("object block checksum mismatch: %s@%d", r.Key, r.Offset)
						} else if err := decode(part, &blocks[i]); err != nil {
							blockErrs[i] = fmt.Errorf("data block %s: %w", r.Key, err)
						}
					}(i)
				}
			}(k, s)
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		for _, s := range batch {
			for _, i := range s.indices {
				if blockErrs[i] != nil {
					return blockErrs[i]
				}
			}
		}
	}
	return nil
}
func wire(a hta.Aggregate) *metricq.HistoryResponse_Aggregate {
	return &metricq.HistoryResponse_Aggregate{Minimum: a.Minimum, Maximum: a.Maximum, Sum: a.Sum, Count: a.Count, Integral: a.Integral, ActiveTime: a.ActiveTime}
}
func (q *reader) rawAggregate(begin, end int64) (hta.Aggregate, error) {
	a := hta.Empty()
	rs, err := q.records(0, begin, end)
	if err != nil {
		return a, err
	}
	previous := begin
	for _, r := range rs {
		if r.Time < begin {
			continue
		}
		if r.Time >= end {
			a.Add(hta.Value(r.Value, end-previous, 0))
			break
		}
		a.Add(hta.Value(r.Value, r.Time-previous, 1))
		previous = r.Time
	}
	return a, nil
}
func (q *reader) levelAggregate(begin, end, level int64) (hta.Aggregate, error) {
	a := hta.Empty()
	if begin >= end {
		return a, nil
	}
	rs, err := q.records(level, begin, end)
	if err != nil {
		return a, err
	}
	for _, r := range rs {
		lo := max(r.Time, begin)
		hi := min(r.LastTime()+level, end)
		if lo < hi {
			a.Add(r.Aggregate.Times((hi - lo) / level))
		}
	}
	return a, nil
}
func (q *reader) aggregate(begin, end int64) (hta.Aggregate, error) {
	a := hta.Empty()
	s := q.e.state.Series[q.metric]
	if end <= s.First.Time || begin > s.Last.Time || s.First.Time == 0 {
		return a, nil
	}
	begin = max(begin, s.First.Time)
	end = min(end, s.Last.Time)
	level := s.Config.IntervalMin
	ceil := func(t, l int64) int64 {
		if t%l == 0 {
			return t
		}
		if t > math.MaxInt64-(l-t%l) {
			return math.MaxInt64
		}
		return t + l - t%l
	}
	nb, ne := ceil(begin, level), end-end%level
	if nb >= ne {
		return q.rawAggregate(begin, end)
	}
	type span struct{ begin, end, level int64 }
	var spans []span
	for _, part := range [][2]int64{{begin, nb}, {ne, end}} {
		if part[0] < part[1] {
			spans = append(spans, span{begin: part[0], end: part[1]})
		}
	}
	begin, end = nb, ne
	for {
		if level > s.Config.IntervalMax/s.Config.IntervalFactor {
			spans = append(spans, span{begin: begin, end: end, level: level})
			break
		}
		next := level * s.Config.IntervalFactor
		nb, ne = ceil(begin, next), end-end%next
		if nb >= ne {
			spans = append(spans, span{begin: begin, end: end, level: level})
			break
		}
		for _, part := range [][2]int64{{begin, nb}, {ne, end}} {
			if part[0] < part[1] {
				spans = append(spans, span{begin: part[0], end: part[1], level: level})
			}
		}
		begin, end, level = nb, ne, next
	}
	// Spans at different HTA levels are independent. Run those reads in
	// parallel, then combine in the original order for stable floating sums.
	type levelJob struct {
		level   int64
		indices []int
	}
	var jobs []levelJob
	byLevel := map[int64]int{}
	for i, part := range spans {
		j, ok := byLevel[part.level]
		if !ok {
			j = len(jobs)
			byLevel[part.level] = j
			jobs = append(jobs, levelJob{level: part.level})
		}
		jobs[j].indices = append(jobs[j].indices, i)
	}
	results := make([]hta.Aggregate, len(spans))
	errs := make([]error, len(jobs))
	var budget atomic.Int64
	var wg sync.WaitGroup
	for j, job := range jobs {
		wg.Add(1)
		go func(j int, job levelJob) {
			defer wg.Done()
			localEngine := &Engine{store: q.e.store, options: q.e.options, metrics: q.e.metrics, state: q.e.state, pending: q.e.pending, flushing: q.e.flushing, nodeCache: make(map[blob]indexNode), sharedNodes: q.e.sharedNodes, sharedBlocks: q.e.sharedBlocks}
			local := reader{e: localEngine, ctx: q.ctx, metric: q.metric, cache: make(map[blob][]hta.Record), budget: &budget}
			for _, i := range job.indices {
				part := spans[i]
				if part.level == 0 {
					results[i], errs[j] = local.rawAggregate(part.begin, part.end)
				} else {
					results[i], errs[j] = local.levelAggregate(part.begin, part.end, part.level)
				}
				if errs[j] != nil {
					return
				}
			}
		}(j, job)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return a, err
		}
	}
	for _, result := range results {
		a.Add(result)
	}
	return a, nil
}

// Query answers a MetricQ history request for the canonical metric name from
// a snapshot: stored blocks of the chosen HTA level plus records not yet
// written. It does not block ingestion or checkpoints.
func (e *Engine) Query(ctx context.Context, name string, req *metricq.HistoryRequest) (resp *metricq.HistoryResponse, err error) {
	start := time.Now()
	e.metrics.Queries.Inc()
	defer func() {
		e.metrics.Query.Observe(time.Since(start).Seconds())
		if err != nil {
			e.metrics.QueryErrors.Inc()
		}
	}()
	resp = &metricq.HistoryResponse{Metric: name}
	if err = ctx.Err(); err != nil {
		return resp, err
	}
	snapshot, snapshotErr := e.readSnapshot(name)
	if snapshotErr != nil {
		return resp, snapshotErr
	}
	owner := e
	generation := snapshot.state.Generation
	defer func() {
		owner.mu.Lock()
		owner.readers--
		owner.pins[generation]--
		if owner.pins[generation] == 0 {
			delete(owner.pins, generation)
		}
		owner.mu.Unlock()
	}()
	e = snapshot
	s := e.state.Series[name]
	if req == nil {
		return resp, fmt.Errorf("nil history request")
	}
	if req.Type == metricq.HistoryRequest_LAST_VALUE {
		if s.Last.Time > 0 {
			resp.TimeDelta = []int64{s.Last.Time}
			resp.Value = []float64{s.Last.Value}
		}
		return resp, nil
	}
	if req.StartTime < 0 || req.EndTime < req.StartTime {
		return resp, fmt.Errorf("invalid time range")
	}
	if req.Type < metricq.HistoryRequest_AGGREGATE_TIMELINE || req.Type > metricq.HistoryRequest_FLEX_TIMELINE {
		return resp, fmt.Errorf("unknown history request type %d", req.Type)
	}
	q := reader{e: e, ctx: ctx, metric: name, cache: make(map[blob][]hta.Record)}
	if req.Type == metricq.HistoryRequest_AGGREGATE || req.IntervalMax < 0 {
		if req.StartTime >= req.EndTime {
			return resp, fmt.Errorf("aggregate requires start < end")
		}
		a, err := q.aggregate(req.StartTime, req.EndTime)
		if err != nil {
			return resp, err
		}
		resp.TimeDelta = []int64{req.StartTime}
		resp.Aggregate = []*metricq.HistoryResponse_Aggregate{wire(a)}
		return resp, nil
	}
	var previous int64
	add := func(t int64, a hta.Aggregate) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(resp.TimeDelta) >= e.options.MaxQueryRows {
			return fmt.Errorf("query exceeds maximum rows")
		}
		resp.TimeDelta = append(resp.TimeDelta, t-previous)
		previous = t
		resp.Aggregate = append(resp.Aggregate, wire(a))
		return nil
	}
	if req.IntervalMax >= s.Config.IntervalMin {
		upper := min(req.IntervalMax, s.Config.IntervalMax)
		level := s.Config.IntervalMin
		for level <= upper/s.Config.IntervalFactor {
			level *= s.Config.IntervalFactor
		}
		for level >= s.Config.IntervalMin {
			rs, err := q.records(level, req.StartTime, req.EndTime)
			if err != nil {
				return resp, err
			}
			factor := int64(1)
			if req.Type == metricq.HistoryRequest_FLEX_TIMELINE {
				factor = upper / level
			}
			group := hta.Empty()
			var count, groupTime int64
			begin := req.StartTime - req.StartTime%level
			for _, r := range rs {
				lo := max(r.Time, begin)
				hi := min(r.LastTime(), req.EndTime-1)
				if req.EndTime == 0 || hi < lo {
					continue
				}
				// Runs can be combined without expanding a billion empty intervals.
				n := (hi-lo)/level + 1
				for n > 0 {
					if count == 0 {
						groupTime = lo
					}
					take := min(factor-count, n)
					group.Add(r.Aggregate.Times(take))
					count += take
					lo += take * level
					n -= take
					if count == factor {
						if err = add(groupTime, group); err != nil {
							return resp, err
						}
						group = hta.Empty()
						count = 0
					}
				}
			}
			if count > 0 {
				if err = add(groupTime, group); err != nil {
					return resp, err
				}
			}
			if len(resp.TimeDelta) > 0 {
				return resp, nil
			}
			level /= s.Config.IntervalFactor
		}
		return resp, nil
	}
	rs, err := q.records(0, req.StartTime, req.EndTime)
	if err != nil {
		return resp, err
	}
	first := sort.Search(len(rs), func(i int) bool { return rs[i].Time > req.StartTime })
	if first > 0 {
		first--
	}
	last := sort.Search(len(rs), func(i int) bool { return rs[i].Time >= req.EndTime })
	if first >= last {
		return resp, nil
	}
	rs = rs[first:last]
	if len(rs) > e.options.MaxQueryRows {
		return resp, fmt.Errorf("query exceeds maximum rows")
	}
	if req.Type == metricq.HistoryRequest_FLEX_TIMELINE && req.IntervalMax > 0 && (req.EndTime-req.StartTime)/int64(len(rs)) < req.IntervalMax {
		// This intentionally follows legacy smoothing, including the predecessor's
		// contribution when the requested start falls between raw points.
		prev := min(req.StartTime, rs[0].Time)
		i := 0
		for i < len(rs) && rs[i].Time < req.StartTime {
			prev = rs[i].Time
			i++
		}
		if i == len(rs) {
			return resp, nil
		}
		for begin := req.StartTime; begin < req.EndTime; {
			end := req.EndTime
			if req.IntervalMax <= req.EndTime-begin {
				end = begin + req.IntervalMax
			}
			a := hta.Empty()
			for i < len(rs) && rs[i].Time < end {
				a.Add(hta.Value(rs[i].Value, rs[i].Time-prev, 1))
				prev = rs[i].Time
				i++
			}
			if i < len(rs) {
				a.Add(hta.Value(rs[i].Value, end-prev, 0))
				prev = end
			}
			if err = add(begin, a); err != nil {
				return resp, err
			}
			if i == len(rs) {
				break
			}
			begin = end
		}
		return resp, nil
	}
	prev := rs[0].Time
	for _, r := range rs {
		if req.Type == metricq.HistoryRequest_AGGREGATE_TIMELINE {
			if err = add(r.Time, hta.Value(r.Value, r.Time-prev, 1)); err != nil {
				return resp, err
			}
			prev = r.Time
		} else {
			resp.TimeDelta = append(resp.TimeDelta, r.Time-previous)
			previous = r.Time
			resp.Value = append(resp.Value, r.Value)
		}
	}
	return resp, nil
}

// A query pins immutable object references and copies only its metric's hot tail.
// Slow historical GETs therefore do not hold the ingestion/WAL lock.
func (e *Engine) readSnapshot(name string) (*Engine, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fmt.Errorf("engine closed")
	}
	series, ok := e.state.Series[name]
	if !ok {
		return nil, fmt.Errorf("unknown metric %q", name)
	}
	copySeries := *series
	e.readers++
	e.pins[e.state.Generation]++
	snapshot := &Engine{store: e.store, options: e.options, metrics: e.metrics, nodeCache: make(map[blob]indexNode), sharedNodes: e.sharedNodes, sharedBlocks: e.sharedBlocks, state: manifest{
		Generation: e.state.Generation, Series: map[string]*hta.Series{name: &copySeries}, Roots: map[string]map[int64]blob{name: e.state.Roots[name]}}}
	// Share this metric's unflushed records without copying them. Records
	// frozen by a running flush precede newer pending records.
	snapshot.flushing = e.flushing.forMetric(name)
	snapshot.pending = e.pending.forMetric(name)
	return snapshot, nil
}
