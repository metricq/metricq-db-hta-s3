package engine

import (
	"context"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/prometheus/client_golang/prometheus"
)

// The dashboard relies on these families and labels; the executable adds the
// token label by wrapping the registerer.
func TestMetricsCoverOperationalState(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	options := maintenanceOptions(t.TempDir(), true)
	options.HoldMaxAgeSeconds = 3600
	e, err := Open(ctx, &gcStore{memoryStore: newStore()}, options, batchConfig, NewMetrics(prometheus.WrapRegistererWith(prometheus.Labels{"token": "db-test"}, registry)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < 3; i++ {
		if err = e.Ingest(ctx, "x", chunk(hta.Point{Time: int64(100 + 50*i), Value: 1})); err != nil {
			t.Fatal(err)
		}
	}
	e.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err = e.CompactOnce(ctx); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.updateHoldMetrics()
	e.mu.Unlock()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]map[string]bool{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if labels[f.GetName()] == nil {
					labels[f.GetName()] = map[string]bool{}
				}
				labels[f.GetName()][l.GetName()+"="+l.GetValue()] = true
			}
		}
	}
	for name, want := range map[string][]string{
		"metricq_db_config":                  {"token=db-test", "parameter=hold_max_age_seconds"},
		"metricq_db_checkpoints_total":       {"reason=hold_age"},
		"metricq_db_checkpoint_blocks_total": {"size=partial"},
		"metricq_db_store_requests_total":    {"op=put", "kind=data", "kind=manifest"},
		"metricq_db_ingest_batch_deliveries": {"token=db-test"},
		"metricq_db_wal_segments":            {"token=db-test"},
		"metricq_db_held_streams":            {"token=db-test"},
		"metricq_db_compaction_object_limit": {"token=db-test"},
		"metricq_db_live_objects":            {"token=db-test"},
		"metricq_db_manifest_bytes":          {"token=db-test"},
	} {
		for _, label := range want {
			if !labels[name][label] {
				t.Errorf("%s lacks %s (has %v)", name, label, labels[name])
			}
		}
	}
}
