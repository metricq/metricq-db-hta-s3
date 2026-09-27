package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"

	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
)

// Hash partitioning keeps changes to a bounded directory instead of rewriting
// all metrics. Pages and their directory share a pack: one PUT per changed kind,
// independent of cardinality. Offsets let unchanged pages survive later commits.
const metadataShards = 256

type metadataDirectory struct{ Pages [metadataShards]blob }
type heldMetadata struct {
	Deltas     []blob
	Watermarks map[string]int64
}

func metadataShard(name string) int { return int(sha256.Sum256([]byte(name))[0]) }
func metadataKey(key string) bool {
	return strings.HasPrefix(key, "state/") || strings.HasPrefix(key, "roots/") || strings.HasPrefix(key, "held-state/")
}
func metadataObjects(root blob, directory metadataDirectory) map[string]bool {
	keys := make(map[string]bool)
	if root.Key != "" {
		keys[root.Key] = true
	}
	for _, ref := range directory.Pages {
		if ref.Key != "" {
			keys[ref.Key] = true
		}
	}
	return keys
}

// encodeManifest uploads immutable metadata first. Only the final CAS makes it
// reachable and permits reclaiming its predecessor or the covered WAL prefix.
// Caller serializes publications and supplies an immutable checkpoint snapshot.
func (e *Engine) encodeManifest(ctx context.Context, next *manifest, base manifest) ([]byte, error) {
	var obsolete []string
	write := func(prefix string, oldRoot blob, oldDir metadataDirectory, changed [metadataShards]bool, value func(int) any) (blob, metadataDirectory, error) {
		dir := oldDir
		var p *pack
		for i, dirty := range changed {
			if !dirty {
				continue
			}
			if p == nil {
				var err error
				name := prefix
				if next.stagingNamespace != "" {
					name += "/compact-" + next.stagingNamespace
				}
				p, err = newPack(name)
				if err != nil {
					return blob{}, dir, err
				}
			}
			b, err := encode(value(i))
			if err != nil {
				return blob{}, dir, err
			}
			dir.Pages[i] = p.add(b)
		}
		if p == nil {
			return oldRoot, dir, nil
		}
		b, err := encode(dir)
		if err != nil {
			return blob{}, dir, err
		}
		root := p.add(b)
		absent := ""
		if _, err = e.put(ctx, p.key, p.buf.Bytes(), &absent); err != nil {
			return blob{}, dir, err
		}
		live := metadataObjects(root, dir)
		for key := range metadataObjects(oldRoot, oldDir) {
			if !live[key] {
				obsolete = append(obsolete, key)
			}
		}
		return root, dir, nil
	}
	var seriesChanged, rootsChanged [metadataShards]bool
	series := [metadataShards]map[string]*hta.Series{}
	roots := [metadataShards]map[string]map[int64]blob{}
	for name, v := range next.Series {
		i := metadataShard(name)
		if base.CheckpointState.Key == "" || (v != base.Series[name] && !reflect.DeepEqual(v, base.Series[name])) {
			seriesChanged[i] = true
		}
	}
	for name := range base.Series {
		if _, ok := next.Series[name]; !ok {
			seriesChanged[metadataShard(name)] = true
		}
	}
	for name, v := range next.Roots {
		i := metadataShard(name)
		if base.StreamIndex.Key == "" || !reflect.DeepEqual(v, base.Roots[name]) {
			rootsChanged[i] = true
		}
	}
	for name := range base.Roots {
		if _, ok := next.Roots[name]; !ok {
			rootsChanged[metadataShard(name)] = true
		}
	}
	for name, v := range next.Series {
		i := metadataShard(name)
		if seriesChanged[i] {
			if series[i] == nil {
				series[i] = make(map[string]*hta.Series)
			}
			series[i][name] = v
		}
	}
	for name, v := range next.Roots {
		i := metadataShard(name)
		if rootsChanged[i] {
			if roots[i] == nil {
				roots[i] = make(map[string]map[int64]blob)
			}
			roots[i][name] = v
		}
	}
	var err error
	next.CheckpointState, next.seriesPages, err = write("state", base.CheckpointState, base.seriesPages, seriesChanged, func(i int) any { return series[i] })
	if err != nil {
		return nil, err
	}
	next.StreamIndex, next.rootPages, err = write("roots", base.StreamIndex, base.rootPages, rootsChanged, func(i int) any { return roots[i] })
	if err != nil {
		return nil, err
	}
	next.HeldState = base.HeldState
	if !reflect.DeepEqual(next.Held, base.Held) || !reflect.DeepEqual(next.HeldWatermarks, base.HeldWatermarks) || (base.HeldState.Key == "" && (len(next.Held) > 0 || len(next.HeldWatermarks) > 0)) {
		next.HeldState = blob{}
		if len(next.Held) > 0 || len(next.HeldWatermarks) > 0 {
			prefix := "held-state"
			if next.stagingNamespace != "" {
				prefix += "/compact-" + next.stagingNamespace
			}
			p, err := newPack(prefix)
			if err != nil {
				return nil, err
			}
			b, err := encode(heldMetadata{next.Held, next.HeldWatermarks})
			if err != nil {
				return nil, err
			}
			next.HeldState = p.add(b)
			absent := ""
			if _, err = e.put(ctx, p.key, p.buf.Bytes(), &absent); err != nil {
				return nil, err
			}
		}
		if base.HeldState.Key != "" {
			obsolete = append(obsolete, base.HeldState.Key)
		}
	}
	if e.options.BackgroundMaintenance {
		// Use the registered job namespace for crash cleanup of publication objects.
		publisher := &Engine{store: e.store, metrics: e.metrics, activeMaintenance: next.stagingNamespace}
		if err = publisher.appendTrash(ctx, next, obsolete, 0); err != nil {
			return nil, err
		}
	} else if _, ok := e.store.(storage.Deleter); ok {
		if next.Garbage == nil {
			next.Garbage = make(map[string]bool)
		}
		for _, key := range obsolete {
			next.Garbage[key] = true
		}
	}
	wire := *next
	if wire.CheckpointState.Key != "" {
		wire.Series = nil
	}
	if wire.StreamIndex.Key != "" {
		wire.Roots = nil
	}
	wire.Held, wire.HeldWatermarks = nil, nil
	next.stagingNamespace = ""
	return encode(wire)
}

func (e *Engine) loadManifestState(ctx context.Context, m *manifest) error {
	load := func(root blob, dir *metadataDirectory, apply func(int, []byte) error) error {
		if root.Key == "" {
			return nil
		}
		b, err := e.readBlob(ctx, root)
		if err != nil {
			return err
		}
		if err = decode(b, dir); err != nil {
			return err
		}
		var refs []blob
		var shards []int
		for i, ref := range dir.Pages {
			if ref.Key != "" {
				refs = append(refs, ref)
				shards = append(shards, i)
			}
		}
		// Pages from each pack are adjacent; use bounded, checksummed range reads.
		for start := 0; start < len(refs); start += maxQueryBlockBatch {
			end := min(start+maxQueryBlockBatch, len(refs))
			blocks, err := e.fetchEncoded(ctx, refs[start:end])
			if err != nil {
				return err
			}
			for i, b := range blocks {
				if err = apply(shards[start+i], b); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if m.CheckpointState.Key != "" {
		m.Series = make(map[string]*hta.Series)
		if err := load(m.CheckpointState, &m.seriesPages, func(shard int, b []byte) error {
			var values map[string]*hta.Series
			if err := decode(b, &values); err != nil {
				return err
			}
			for name, v := range values {
				if metadataShard(name) != shard || v == nil {
					return fmt.Errorf("invalid checkpoint page")
				}
				m.Series[name] = v
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if m.StreamIndex.Key != "" {
		m.Roots = make(map[string]map[int64]blob)
		if err := load(m.StreamIndex, &m.rootPages, func(shard int, b []byte) error {
			var values map[string]map[int64]blob
			if err := decode(b, &values); err != nil {
				return err
			}
			for name, v := range values {
				if metadataShard(name) != shard || v == nil {
					return fmt.Errorf("invalid stream root page")
				}
				m.Roots[name] = v
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if m.HeldState.Key != "" {
		b, err := e.readBlob(ctx, m.HeldState)
		if err != nil {
			return err
		}
		var held heldMetadata
		if err = decode(b, &held); err != nil {
			return err
		}
		m.Held, m.HeldWatermarks = held.Deltas, held.Watermarks
	}
	return nil
}
