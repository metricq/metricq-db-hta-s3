//go:build integration

package integration

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func measureCompactionMetrics(t *testing.T, r *prometheus.Registry) func() {
	t.Helper()
	server := httptest.NewServer(promhttp.HandlerFor(r, promhttp.HandlerOpts{}))
	stop, done := make(chan struct{}), make(chan struct{})
	scrapes, failures := 0, 0
	client := &http.Client{Timeout: 2 * time.Second}
	scrape := func() {
		resp, err := client.Get(server.URL)
		if err != nil {
			failures++
			return
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || !strings.Contains(string(b), "metricq_db_compaction_") {
			failures++
		}
		scrapes++
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		scrape()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				scrape()
			}
		}
	}()
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		close(stop)
		<-done
		server.Close()
		t.Logf("Prometheus scrapes=%d failures=%d", scrapes, failures)
		if failures > 0 {
			t.Error("Prometheus endpoint failed during compaction")
		}
		families, err := r.Gather()
		if err != nil {
			t.Error(err)
			return
		}
		for _, f := range families {
			if f.GetName() != "metricq_db_compaction_phase_seconds" && f.GetName() != "metricq_db_metadata_cache_requests_total" && f.GetName() != "metricq_db_checkpoint_metadata_pages_total" {
				continue
			}
			for _, m := range f.Metric {
				labels := []string{}
				for _, l := range m.Label {
					labels = append(labels, l.GetName()+"="+l.GetValue())
				}
				if h := m.Histogram; h != nil {
					t.Logf("phase %s count=%d seconds=%.6f", strings.Join(labels, ","), h.GetSampleCount(), h.GetSampleSum())
				}
				if c := m.Counter; c != nil {
					if f.GetName() == "metricq_db_checkpoint_metadata_pages_total" {
						t.Logf("metadata pages %s count=%.0f", strings.Join(labels, ","), c.GetValue())
					} else {
						t.Logf("cache %s calls=%.0f", strings.Join(labels, ","), c.GetValue())
					}
				}
			}
		}
	}
	t.Cleanup(finish)
	return finish
}
