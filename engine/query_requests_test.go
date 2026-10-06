package engine

import (
	"testing"
	"time"

	metricq "github.com/metricq/metricq-go"
	dto "github.com/prometheus/client_model/go"
)

func histogramSum(t *testing.T, h interface{ Write(*dto.Metric) error }) (float64, uint64) {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.Histogram.GetSampleSum(), m.Histogram.GetSampleCount()
}

// Each history request records its data range requests: a cold read needs
// some, the same read from the block cache none.
func TestQueryDataRequestsMetric(t *testing.T) {
	f := newHoldFixture(t)
	f.ingest("x", 3000)
	f.flush()
	f.now = f.now.Add(2 * time.Hour)
	f.flush()
	req := &metricq.HistoryRequest{Type: metricq.HistoryRequest_FLEX_TIMELINE, StartTime: 0, EndTime: 1 << 40}
	f.restart() // cold caches
	for i, want := range []string{"cold", "warm"} {
		before, n := histogramSum(t, f.e.metrics.QueryDataRequests)
		if _, err := f.e.Query(f.ctx, "x", req); err != nil {
			t.Fatal(err)
		}
		after, m := histogramSum(t, f.e.metrics.QueryDataRequests)
		if m != n+1 || (i == 0) != (after > before) {
			t.Fatalf("%s query: %v data requests, %d observations", want, after-before, m-n)
		}
	}
}
