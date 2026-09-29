package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// Holding streams back avoids writing a tiny block, and rewriting its index
// path, for every stream at every checkpoint. A checkpoint writes a stream only
// in full 1024-record blocks, or completely once its oldest held record is
// older than the hold limit or builder memory runs short. Held records that no
// earlier delta covers are persisted together in one held/ object per
// checkpoint, so the WAL is still released after every checkpoint.
//
// A stream's held records are a suffix of its history. The first `covered`
// ones are persisted in the listed delta objects (oldest first); the rest
// exist only in the WAL. After a restart, delta records at or before a
// stream's watermark (its last written record) are already in blocks.
// Deltas are shared between streams, so a delta may outlive the segments of
// a stream it contains: the watermark is kept while any of them is live.
type heldStream struct {
	since     time.Time // arrival of the oldest held record
	covered   int
	segments  []heldSegment
	deltas    []string // live delta objects containing records of this stream
	watermark int64
	written   bool
}

type heldSegment struct {
	key   string
	count int
}

type heldDelta struct {
	Streams []deltaStream
}

type deltaStream struct {
	Metric  string
	Level   int64
	Records []hta.Record
}

// holdPlan is decided at freeze time and applied only after a successful commit.
type holdPlan struct {
	write      map[string]map[int64]int // leading records to write per stream
	delta      heldDelta                // newly covered held records
	segments   map[string][]heldSegment // after commit
	covered    map[string]int
	deltas     map[string][]string
	watermarks map[string]int64
	live       []blob // delta objects still needed, oldest first
	obsolete   []string
}

func (e *Engine) holding() bool { return e.options.HoldMaxAgeSeconds > 0 }

func (e *Engine) clock() time.Time {
	if e.now != nil {
		return e.now()
	}
	return time.Now()
}

// addPending records a new unflushed record and when its stream started holding.
func (e *Engine) addPending(metric string, r hta.Record) {
	if e.holding() && len(e.pending.streams[metric][r.Level]) == 0 {
		h := e.heldStream(metric, r.Level)
		h.since = e.clock()
	}
	e.pending.add(metric, r)
	e.pendingBytes += pendingRecordBytes
}

func (e *Engine) heldStream(metric string, level int64) *heldStream {
	key := streamKey(metric, level)
	h := e.held[key]
	if h == nil {
		if e.held == nil {
			e.held = make(map[string]*heldStream)
		}
		h = &heldStream{}
		e.held[key] = h
	}
	return h
}

// holdDue reports whether some stream exceeded the hold limit. Caller holds mu.
func (e *Engine) holdDue() bool {
	if !e.holding() {
		return false
	}
	limit := e.clock().Add(-time.Duration(e.options.HoldMaxAgeSeconds) * time.Second)
	for metric, levels := range e.pending.streams {
		for level, records := range levels {
			if h := e.held[streamKey(metric, level)]; len(records) > 0 && h != nil && h.since.Before(limit) {
				return true
			}
		}
	}
	return false
}

// holdPressure reports held records above their memory budget. Caller holds mu.
func (e *Engine) holdPressure() bool {
	return e.holding() && e.pendingBytes > e.options.HoldMemoryBytes
}

// unsavedBytes counts records neither written nor persisted in a delta.
func (e *Engine) unsavedBytes() int64 {
	return e.pendingBytes - e.coveredRecords*pendingRecordBytes
}

// planHold splits every pending stream into a written prefix and a held
// suffix. Caller holds mu.
func (e *Engine) planHold(deltaKey string) holdPlan {
	plan := holdPlan{write: make(map[string]map[int64]int)}
	type candidate struct {
		metric string
		level  int64
		n      int
	}
	var streams []candidate
	for metric, levels := range e.pending.streams {
		for level, records := range levels {
			if len(records) > 0 {
				streams = append(streams, candidate{metric, level, len(records)})
			}
		}
	}
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].n != streams[j].n {
			return streams[i].n > streams[j].n
		}
		if streams[i].metric != streams[j].metric {
			return streams[i].metric < streams[j].metric
		}
		return streams[i].level < streams[j].level
	})
	set := func(metric string, level int64, k int) {
		if plan.write[metric] == nil {
			plan.write[metric] = make(map[int64]int)
		}
		plan.write[metric][level] = k
	}
	if !e.holding() {
		for _, s := range streams {
			set(s.metric, s.level, s.n)
		}
		return plan
	}
	limit := e.clock().Add(-time.Duration(e.options.HoldMaxAgeSeconds) * time.Second)
	// Above the hold budget, write the largest streams completely until the
	// held remainder drops to half of it.
	remaining := e.pendingBytes
	pressure := e.holdPressure()
	for _, s := range streams {
		h := e.heldStream(s.metric, s.level)
		k := s.n - s.n%maxDataBlockRecords
		if h.since.Before(limit) || (pressure && remaining > e.options.HoldMemoryBytes/2) {
			k = s.n
		}
		remaining -= int64(k) * pendingRecordBytes
		if k > 0 {
			set(s.metric, s.level, k)
		}
	}
	plan.segments = make(map[string][]heldSegment)
	plan.covered = make(map[string]int)
	plan.deltas = make(map[string][]string)
	plan.watermarks = make(map[string]int64)
	live := make(map[string]bool)
	for _, s := range streams {
		key := streamKey(s.metric, s.level)
		h := e.heldStream(s.metric, s.level)
		records := e.pending.streams[s.metric][s.level]
		k := plan.write[s.metric][s.level]
		// Drop written records from the front of the delta segments.
		var segments []heldSegment
		drop := k
		for _, seg := range h.segments {
			if drop >= seg.count {
				drop -= seg.count
				continue
			}
			segments = append(segments, heldSegment{seg.key, seg.count - drop})
			drop = 0
		}
		covered := max(h.covered, k)
		if fresh := records[covered:s.n]; len(fresh) > 0 {
			plan.delta.Streams = append(plan.delta.Streams, deltaStream{Metric: s.metric, Level: s.level, Records: fresh})
			segments = append(segments, heldSegment{deltaKey, len(fresh)})
			plan.deltas[key] = append(append([]string(nil), h.deltas...), deltaKey)
		}
		if len(segments) == 0 {
			continue
		}
		plan.segments[key] = segments
		plan.covered[key] = s.n - k
		for _, seg := range segments {
			live[seg.key] = true
		}
	}
	// Written records stay in their deltas while other streams need them, and
	// streams without pending records may still appear in live deltas.
	for key, h := range e.held {
		deltas, ok := plan.deltas[key]
		if !ok {
			deltas = h.deltas
		}
		var kept []string
		for _, d := range deltas {
			if live[d] {
				kept = append(kept, d)
			}
		}
		if len(kept) == 0 {
			delete(plan.deltas, key)
			continue
		}
		plan.deltas[key] = kept
		metric, level := splitStreamKey(key)
		if k := plan.write[metric][level]; k > 0 {
			plan.watermarks[key] = e.pending.streams[metric][level][k-1].Time
		} else if h.written {
			plan.watermarks[key] = h.watermark
		}
	}
	for _, ref := range e.state.Held {
		if live[ref.Key] {
			plan.live = append(plan.live, ref)
		} else {
			plan.obsolete = append(plan.obsolete, ref.Key)
		}
	}
	return plan
}

// commitHold applies a published plan. Caller holds mu.
func (e *Engine) commitHold(plan holdPlan) {
	if !e.holding() {
		return
	}
	now := e.clock()
	e.coveredRecords = 0
	for key, h := range e.held {
		metric, level := splitStreamKey(key)
		if k := plan.write[metric][level]; k > 0 {
			h.watermark, h.written = e.flushingLast(metric, level), true
			// Remaining records arrived after the written ones.
			h.since = now
		}
		h.segments = plan.segments[key]
		h.covered = plan.covered[key]
		h.deltas = plan.deltas[key]
		e.coveredRecords += int64(h.covered)
		if len(h.segments) == 0 && len(h.deltas) == 0 && len(e.pending.streams[metric][level]) == 0 {
			delete(e.held, key)
		}
	}
}

func (e *Engine) flushingLast(metric string, level int64) int64 {
	s := e.flushing.streams[metric][level]
	return s[len(s)-1].Time
}

func splitStreamKey(key string) (string, int64) {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == 0 {
			var level int64
			fmt.Sscan(key[i+1:], &level)
			return key[:i], level
		}
	}
	return key, 0
}

// loadHeld restores held records from the delta objects of the manifest,
// skipping records already written to blocks.
func (e *Engine) loadHeld(ctx context.Context) error {
	now := e.clock()
	for _, ref := range e.state.Held {
		b, err := e.readBlob(ctx, ref)
		if err != nil {
			return fmt.Errorf("held delta %s: %w", ref.Key, err)
		}
		var delta heldDelta
		if err = decode(b, &delta); err != nil {
			return fmt.Errorf("held delta %s: %w", ref.Key, err)
		}
		for _, s := range delta.Streams {
			key := streamKey(s.Metric, s.Level)
			watermark, written := e.state.HeldWatermarks[key]
			kept := 0
			for _, r := range s.Records {
				if written && r.Time <= watermark {
					continue
				}
				e.pending.add(s.Metric, r)
				e.pendingBytes += pendingRecordBytes
				kept++
			}
			h := e.heldStream(s.Metric, s.Level)
			h.deltas = append(h.deltas, ref.Key)
			h.watermark, h.written = watermark, written
			if kept == 0 {
				continue
			}
			if h.covered == 0 {
				h.since = now
			}
			h.covered += kept
			h.segments = append(h.segments, heldSegment{ref.Key, kept})
			e.coveredRecords += int64(kept)
		}
	}
	return nil
}

// updateHoldMetrics scans held streams; called once per flush-loop tick
// rather than per delivery. Caller holds mu.
func (e *Engine) updateHoldMetrics() {
	streams := 0
	var oldest time.Time
	for metric, levels := range e.pending.streams {
		for level, records := range levels {
			if len(records) == 0 {
				continue
			}
			streams++
			if h := e.held[streamKey(metric, level)]; h != nil && (oldest.IsZero() || h.since.Before(oldest)) {
				oldest = h.since
			}
		}
	}
	e.metrics.HeldStreams.Set(float64(streams))
	age := 0.0
	if e.holding() && !oldest.IsZero() {
		age = e.clock().Sub(oldest).Seconds()
	}
	e.metrics.HeldOldestAge.Set(age)
}

// Group age-only deadlines on a common cadence, independent of metric count.
// Held records are already durable, so this only delays transfer into blocks.
func (e *Engine) needsScheduledFlush() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal.size >= e.options.WALTarget || e.unsavedBytes() >= e.options.CheckpointUnsavedBytes || e.holdPressure() {
		return true
	}
	if !e.holding() {
		return false
	}
	period := time.Duration(e.options.HoldExpiryIntervalSeconds) * time.Second
	now := e.clock()
	for metric, levels := range e.pending.streams {
		for level, records := range levels {
			h := e.held[streamKey(metric, level)]
			if len(records) == 0 || h == nil {
				continue
			}
			due := h.since.Add(time.Duration(e.options.HoldMaxAgeSeconds) * time.Second)
			rounded := due.Truncate(period)
			if rounded.Before(due) {
				rounded = rounded.Add(period)
			}
			if now.After(rounded) {
				return true
			}
		}
	}
	return false
}
