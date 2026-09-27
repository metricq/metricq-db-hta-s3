package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/metricq/metricq-db-hta-go/hta"
)

// Mixed checkpoint packs carry one block descriptor per stream. Selecting and
// publishing merges across many such packs must adapt to the catalog byte
// budget instead of failing every attempt.
func TestCompactionProgressesWithLargeCatalogInventories(t *testing.T) {
	previous, grace := compactionCatalogBudget, compactionAbortGrace
	compactionCatalogBudget, compactionAbortGrace = 64<<10, 0
	defer func() { compactionCatalogBudget, compactionAbortGrace = previous, grace }()
	ctx := context.Background()
	configs := map[string]hta.Config{}
	for i := 0; i < 300; i++ {
		configs[fmt.Sprintf("m%03d", i)] = hta.Config{IntervalMin: 1000000, IntervalMax: 10000000, IntervalFactor: 10}
	}
	s := &gcStore{memoryStore: newStore()}
	options := maintenanceOptions(t.TempDir(), true)
	options.Compaction.MaxBlocks = 512
	e, err := Open(ctx, s, options, configs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for batch := 0; batch < 30; batch++ {
		for name := range configs {
			if err = e.Ingest(ctx, name, chunk(hta.Point{Time: int64(batch + 1), Value: float64(batch)})); err != nil {
				t.Fatal(err)
			}
		}
		if err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	converged := false
	for pass := 0; pass < 1000 && !converged; pass++ {
		// As the maintenance loop: sweep aborted jobs, then compact.
		if err = e.recoverCompaction(ctx); err != nil {
			t.Fatal(err)
		}
		if err = e.CompactOnce(ctx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		drain(t, e)
		if pass%10 == 9 {
			converged = true
			for name := range configs {
				if len(streamBlocks(t, e, name, 0)) != 1 {
					converged = false
					break
				}
			}
		}
	}
	if !converged {
		t.Fatal("compaction did not converge")
	}
	checkCatalog(t, e)
}
