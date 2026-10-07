package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	"google.golang.org/protobuf/proto"
)

type reader struct {
	e      *Engine
	ctx    context.Context
	metric string
	cache  map[blob][]hta.Record
	// res holds the query's share of the shared query memory budget.
	res *queryReservation
	// check, if set, validates the number of records a read would return
	// (from the index) before any block is fetched.
	check func(records int) error
}

// Bound request fan-out, compressed ranges and decoded records independently.
const maxParallelBlockFetches = 8
const maxQueryBlockBatch = 128
const maxCoalescedRange = 8 << 20
const maxCoalescedGap = 64 << 10

func recordsCost(n int) int64 { return int64(n) * 128 }

// records reads the records of a level in [begin, end]; raw reads include the
// blocks around the window for boundary semantics.
func (q *reader) records(level, begin, end int64) ([]hta.Record, error) {
	root := q.e.state.Roots[q.metric][level]
	var entries []indexEntry
	lookupBegin, lookupEnd := begin, end
	if level > 0 {
		if begin >= end {
			return nil, nil
		}
		// FLEX includes the bucket containing begin, including empty runs.
		lookupBegin = begin - begin%level
		lookupEnd = end - 1
	} else {
		if prior, err := q.e.indexNeighborEntry(q.ctx, root, begin, true); err != nil {
			return nil, err
		} else if prior.Blob.Key != "" {
			entries = append(entries, prior)
		}
	}
	if err := q.e.indexRangeEntries(q.ctx, root, lookupBegin, lookupEnd, &entries); err != nil {
		return nil, err
	}
	if level == 0 {
		if next, err := q.e.indexNeighborEntry(q.ctx, root, end, false); err != nil {
			return nil, err
		} else if next.Blob.Key != "" {
			entries = append(entries, next)
		}
	}
	// Size the read from the index before fetching anything: reject requests
	// whose response would be too large and reserve memory for the decoded
	// records, which are held twice (block cache of this query and result).
	total := len(q.e.flushing.stream(q.metric, level)) + len(q.e.pending.stream(q.metric, level))
	refs := make([]blob, 0, len(entries))
	seenEntry := make(map[blob]bool, len(entries))
	for _, entry := range entries {
		if !seenEntry[entry.Blob] {
			seenEntry[entry.Blob] = true
			total += entry.Records
			refs = append(refs, entry.Blob)
		}
	}
	if q.check != nil {
		if err := q.check(total); err != nil {
			return nil, err
		}
	}
	if err := q.res.reserve(q.ctx, 2*recordsCost(total)); err != nil {
		return nil, err
	}
	out := make([]hta.Record, 0, total)
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
	if q.res != nil {
		q.res.requests.Add(int64(len(spans)))
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

// aggregate computes a single aggregate from the raw index. Each entry
// stores the aggregate of its subtree, counting its first value from the
// stream's previous one, so an entry whose values and their preceding
// durations lie inside the window contributes its stored aggregate. Only the
// blocks at the two window borders are read, in one round trip, independent
// of the window length. Values not yet in the index come last.
func (q *reader) aggregate(begin, end int64) (hta.Aggregate, error) {
	a := hta.Empty()
	s := q.e.state.Series[q.metric]
	if end <= s.First.Time || begin > s.Last.Time || s.First.Time == 0 {
		return a, nil
	}
	begin = max(begin, s.First.Time)
	end = min(end, s.Last.Time)
	// The plan lists, in time order, stored aggregates and border blocks.
	type step struct {
		entry indexEntry
		read  bool
	}
	var plan []step
	var walk func(ref blob) error
	walk = func(ref blob) error {
		if ref.Key == "" {
			return nil
		}
		n, err := q.e.readNode(q.ctx, ref)
		if err != nil {
			return err
		}
		for _, entry := range n.Entries {
			switch {
			case entry.Last < begin:
			case entry.rawPrevious() >= end:
				return nil
			case entry.rawPrevious() >= begin && entry.Last < end:
				plan = append(plan, step{entry: entry})
			case n.Leaf:
				plan = append(plan, step{entry: entry, read: true})
			default:
				if err := walk(entry.Blob); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(q.e.state.Roots[q.metric][0]); err != nil {
		return a, err
	}
	var refs []blob
	records := 0
	for _, st := range plan {
		if st.read {
			refs = append(refs, st.entry.Blob)
			records += st.entry.Records
		}
	}
	if err := q.res.reserve(q.ctx, recordsCost(records)); err != nil {
		return a, err
	}
	blocks, err := q.fetchBlocks(refs)
	if err != nil {
		return a, err
	}
	// Raw values hold from the previous value (or the window start) to their
	// time; the first value at or after end holds up to end.
	previous, done := begin, false
	add := func(rs []hta.Record) {
		for _, r := range rs {
			if done || r.Time < begin {
				continue
			}
			if r.Time >= end {
				a.Add(hta.Value(r.Value, end-previous, 0))
				done = true
				return
			}
			a.Add(hta.Value(r.Value, r.Time-previous, 1))
			previous = r.Time
		}
	}
	for _, st := range plan {
		if !st.read {
			a.Add(st.entry.agg)
			previous = st.entry.Last
			continue
		}
		add(blocks[0])
		blocks = blocks[1:]
	}
	add(q.e.flushing.stream(q.metric, 0))
	add(q.e.pending.stream(q.metric, 0))
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
	res := &queryReservation{budget: e.queryBudget}
	defer res.releaseAll()
	resp, err = e.query(ctx, name, req, res)
	e.metrics.QueryDataRequests.Observe(float64(res.requests.Load()))
	// Estimates bound the work; the encoded size is the binding limit, since
	// the broker rejects larger messages.
	if err == nil {
		if size := proto.Size(resp); size > e.options.QueryMaxResponseBytes {
			resp, err = &metricq.HistoryResponse{Metric: name}, fmt.Errorf("history response of %d bytes exceeds query_max_response_bytes (%d); request a shorter range or a larger interval", size, e.options.QueryMaxResponseBytes)
		}
	}
	return resp, err
}

// responseTooLarge rejects a response of about points points of perPoint
// encoded bytes each.
func (e *Engine) responseTooLarge(points, perPoint int64) error {
	if limit := int64(e.options.QueryMaxResponseBytes); points > limit/perPoint {
		return fmt.Errorf("history response of about %d points would exceed query_max_response_bytes (%d bytes); request a shorter range or a larger interval", points, limit)
	}
	return nil
}

func (e *Engine) query(ctx context.Context, name string, req *metricq.HistoryRequest, res *queryReservation) (resp *metricq.HistoryResponse, err error) {
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
	q := reader{e: e, ctx: ctx, metric: name, cache: make(map[blob][]hta.Record), res: res}
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
		if err := e.responseTooLarge(int64(len(resp.TimeDelta))+1, aggregatePointBytes); err != nil {
			return err
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
			factor := int64(1)
			if req.Type == metricq.HistoryRequest_FLEX_TIMELINE {
				factor = upper / level
			}
			// Every output point covers factor intervals of this level within
			// the series' data; reject before reading if they cannot fit.
			if first, last := max(req.StartTime-req.StartTime%level, s.First.Time-s.First.Time%level), min(req.EndTime, s.Last.Time+level); s.First.Time > 0 && first < last {
				if err := e.responseTooLarge((last-first)/(level*factor)+1, aggregatePointBytes); err != nil {
					return resp, err
				}
			}
			rs, err := q.records(level, req.StartTime, req.EndTime)
			if err != nil {
				return resp, err
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
	// Raw values: the index tells how many records the range holds. FLEX
	// smooths ranges denser than interval_max into one aggregate per interval.
	q.check = func(records int) error {
		points, perPoint := int64(records), int64(rawPointBytes)
		if req.Type == metricq.HistoryRequest_AGGREGATE_TIMELINE {
			perPoint = aggregatePointBytes
		} else if req.IntervalMax > 0 {
			if buckets := (req.EndTime-req.StartTime)/req.IntervalMax + 1; points > buckets {
				points, perPoint = buckets, aggregatePointBytes
			}
		}
		return e.responseTooLarge(points, perPoint)
	}
	rs, err := q.records(0, req.StartTime, req.EndTime)
	q.check = nil
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
