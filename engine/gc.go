package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/metricq/metricq-db-hta-s3/storage"
)

// Rebuild counts from the committed index on startup, without reading data
// blocks. Normal checkpoints adjust counts only for newly written blocks and
// the copied rightmost index path; they never scan historical trees.
func (e *Engine) initializeGC(ctx context.Context) error {
	if e.options.MaintenanceEnabled {
		return e.bootstrapCatalog(ctx)
	}
	if _, ok := e.store.(storage.Deleter); !ok {
		return nil
	}
	e.objectRefs = make(map[string]int64)
	e.garbage = make(map[string]bool)
	var walk func(blob) error
	walk = func(ref blob) error {
		if ref.Key == "" {
			return nil
		}
		e.objectRefs[ref.Key]++
		node, err := e.readNode(ctx, ref)
		if err != nil {
			return fmt.Errorf("rebuild object references: %w", err)
		}
		for _, edge := range node.Entries {
			if node.Leaf {
				e.objectRefs[edge.Blob.Key]++
			} else if err := walk(edge.Blob); err != nil {
				return err
			}
		}
		return nil
	}
	for _, levels := range e.state.Roots {
		for _, root := range levels {
			if err := walk(root); err != nil {
				return err
			}
		}
	}
	for key := range e.state.Garbage {
		if !strings.HasPrefix(key, "data/") && !strings.HasPrefix(key, "index/") && !metadataKey(key) {
			return fmt.Errorf("invalid garbage object: %q", key)
		}
		if e.objectRefs[key] != 0 {
			return fmt.Errorf("garbage object still referenced: %s", key)
		}
		e.garbage[key] = true
	}
	e.metrics.GCPending.Set(float64(len(e.garbage)))
	return nil
}

// Caller holds publishMu and mu, so no new snapshot can start between the reader check and
// DELETE. Existing snapshots keep every old pack alive until their queries end.
// Limit each pass to avoid an unbounded deletion batch under the ingestion lock.
func (e *Engine) collectGarbage(ctx context.Context) {
	if e.objectRefs == nil {
		return
	}
	e.metrics.GCPending.Set(float64(len(e.garbage)))
	if e.readers != 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	deleter := e.store.(storage.Deleter)
	attempts := 0
	for key := range e.garbage {
		if attempts == 16 || ctx.Err() != nil {
			break
		}
		attempts++
		err := deleter.Delete(ctx, key)
		e.metrics.observeStore("delete", key, 0, err)
		if err != nil {
			e.metrics.GCErrors.Inc()
			slog.Warn("retired object deletion failed", "key", key, "error", err)
			break
		}
		delete(e.garbage, key)
		e.metrics.GCDeleted.Inc()
	}
	e.metrics.GCPending.Set(float64(len(e.garbage)))
}
