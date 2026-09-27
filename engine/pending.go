package engine

import (
	"sort"

	"github.com/metricq/metricq-db-hta-go/hta"
)

// pendingSet holds records not yet in a published checkpoint, grouped by
// stream in time order. Streams only grow by appending, so a query snapshot can
// share a stream's current prefix (capacity-capped) instead of copying it under
// the ingestion lock, and a checkpoint needs no sort.
type pendingSet struct {
	streams map[string]map[int64][]hta.Record
	records int
}

func (p *pendingSet) add(metric string, r hta.Record) {
	if p.streams == nil {
		p.streams = make(map[string]map[int64][]hta.Record)
	}
	levels := p.streams[metric]
	if levels == nil {
		levels = make(map[int64][]hta.Record)
		p.streams[metric] = levels
	}
	levels[r.Level] = append(levels[r.Level], r)
	p.records++
}

func (p *pendingSet) len() int { return p.records }

// stream returns a read-only view; later appends never become visible in it.
func (p *pendingSet) stream(metric string, level int64) []hta.Record {
	s := p.streams[metric][level]
	return s[:len(s):len(s)]
}

// forMetric shares one metric's streams in O(levels).
func (p *pendingSet) forMetric(metric string) pendingSet {
	levels := p.streams[metric]
	if len(levels) == 0 {
		return pendingSet{}
	}
	view := pendingSet{streams: map[string]map[int64][]hta.Record{metric: make(map[int64][]hta.Record, len(levels))}}
	for level, s := range levels {
		view.streams[metric][level] = s[:len(s):len(s)]
		view.records += len(s)
	}
	return view
}

// prepend puts older records (from a failed checkpoint) before newer ones.
func (p *pendingSet) prepend(older pendingSet) {
	for metric, levels := range older.streams {
		for level, records := range levels {
			if p.streams == nil {
				p.streams = make(map[string]map[int64][]hta.Record)
			}
			if p.streams[metric] == nil {
				p.streams[metric] = make(map[int64][]hta.Record)
			}
			p.streams[metric][level] = append(append([]hta.Record(nil), records...), p.streams[metric][level]...)
		}
	}
	p.records += older.records
}

type pendingStream struct {
	metric  string
	level   int64
	records []hta.Record
}

// sorted lists streams by metric and level, the checkpoint pack order.
func (p *pendingSet) sorted() []pendingStream {
	var out []pendingStream
	for metric, levels := range p.streams {
		for level, records := range levels {
			if len(records) > 0 {
				out = append(out, pendingStream{metric, level, records})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].metric != out[j].metric {
			return out[i].metric < out[j].metric
		}
		return out[i].level < out[j].level
	})
	return out
}
