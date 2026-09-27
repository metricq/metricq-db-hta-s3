package engine

import (
	"testing"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

func TestPendingViewsAreStableAndPrependKeepsOrder(t *testing.T) {
	var p pendingSet
	for i := int64(1); i <= 3; i++ {
		p.add("x", hta.Record{Time: i})
	}
	p.add("x", hta.Record{Time: 10, Level: 100})
	p.add("y", hta.Record{Time: 1})
	view := p.forMetric("x")
	raw := view.stream("x", 0)
	// Appending may reuse spare capacity of the shared backing array.
	p.add("x", hta.Record{Time: 4})
	if len(raw) != 3 || len(view.stream("x", 0)) != 3 || view.len() != 4 || view.stream("y", 0) != nil {
		t.Fatalf("snapshot changed or leaked: %v %d", raw, view.len())
	}
	if got := append(raw, hta.Record{Time: 99}); &got[0] == &p.streams["x"][0][0] {
		t.Fatal("appending to a view can overwrite live records")
	}
	var newer pendingSet
	newer.add("x", hta.Record{Time: 5})
	newer.prepend(p)
	s := newer.stream("x", 0)
	if newer.len() != 7 || len(s) != 5 || s[0].Time != 1 || s[4].Time != 5 {
		t.Fatalf("prepend order %v", s)
	}
	streams := newer.sorted()
	if len(streams) != 3 || streams[0].metric != "x" || streams[0].level != 0 || streams[1].level != 100 || streams[2].metric != "y" {
		t.Fatalf("stream order %+v", streams)
	}
}
