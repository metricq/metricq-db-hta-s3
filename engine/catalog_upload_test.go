package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var errInventoryUploadTest = errors.New("inventory upload failure")

type inventoryGateStore struct {
	*memoryStore
	enabled bool
	fail    bool
	started chan struct{}
	release chan struct{}
	active  atomic.Int32
	maximum atomic.Int32
	calls   atomic.Int32
	roots   atomic.Int32
}

func (s *inventoryGateStore) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	if !s.enabled {
		return s.memoryStore.Put(ctx, key, b, expected)
	}
	var blocks []BlockInfo
	if err := decode(b, &blocks); err != nil {
		s.roots.Add(1)
		return s.memoryStore.Put(ctx, key, b, expected)
	}
	call := s.calls.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.maximum.Load(); active > old; old = s.maximum.Load() {
		if s.maximum.CompareAndSwap(old, active) {
			break
		}
	}
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if s.fail && call == 1 {
		return "", errInventoryUploadTest
	}
	return s.memoryStore.Put(ctx, key, b, expected)
}

func TestCatalogInventoryUploadsBoundedAndAwaited(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("failure=", fail), func(t *testing.T) {
			s := &inventoryGateStore{memoryStore: newStore(), fail: fail, started: make(chan struct{}, 16), release: make(chan struct{})}
			e := openTest(t, s, t.TempDir())
			s.enabled = true
			e.activeMaintenance = "inventory-upload-test"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			changes := map[string]*ObjectInfo{}
			for i := 0; i < 8; i++ {
				key := fmt.Sprintf("data/inventory/%d", i)
				o := &ObjectInfo{Key: key, Size: 129 * 64, LiveBytes: 129 * 64}
				for j := 0; j < 129; j++ {
					o.Blocks = append(o.Blocks, BlockInfo{Metric: "x", Entry: indexEntry{Blob: blob{Key: key, Offset: int64(j * 64), Length: 64}}})
				}
				changes[key] = o
			}
			next := e.state
			done := make(chan error, 1)
			go func() { _, err := e.catalogChanges(ctx, &next, changes); done <- err }()
			for i := 0; i < inventoryUploadConcurrency; i++ {
				select {
				case <-s.started:
				case <-ctx.Done():
					t.Fatal("uploads did not overlap")
				}
			}
			if s.roots.Load() != 0 {
				t.Error("catalog written before inventory uploads complete")
			}
			select {
			case err := <-done:
				t.Fatalf("returned before uploads complete: %v", err)
			default:
			}
			close(s.release)
			err := <-done
			registered := map[string]bool{}
			for _, key := range e.stagingKeys {
				registered[key] = true
			}
			s.mu.Lock()
			for key := range s.objects {
				if strings.HasPrefix(key, "catalog/compact-inventory-upload-test/") && !registered[key] {
					t.Errorf("uploaded pack not registered for crash cleanup: %s", key)
				}
			}
			s.mu.Unlock()
			if s.active.Load() != 0 {
				t.Fatal("upload worker still running after return")
			}
			if s.maximum.Load() != inventoryUploadConcurrency {
				t.Fatalf("maximum concurrency %d", s.maximum.Load())
			}
			if fail {
				if !errors.Is(err, errInventoryUploadTest) {
					t.Fatalf("lost original upload error: %v", err)
				}
				if s.roots.Load() != 0 || next.Catalog != e.state.Catalog {
					t.Fatal("failed inventory upload published catalog references")
				}
				if s.calls.Load() > 8 {
					t.Fatal("failed uploads retried")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.calls.Load() != 8 {
				t.Fatalf("uploaded %d inventories", s.calls.Load())
			}
			owners := map[string]string{}
			for key := range changes {
				got, ok, err := e.catalogGet(ctx, next.Catalog, key)
				if err != nil || !ok || len(got.Blocks) != 129 {
					t.Fatalf("catalog recovery: %v", err)
				}
				for pack := range inventoryObjects(got.Inventory) {
					if owner, exists := owners[pack]; exists && owner != key {
						t.Fatal("inventory pack shared across owners")
					}
					owners[pack] = key
				}
			}
		})
	}
}

func TestInventoryUploadCancellationJoinsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	stopped := make(chan struct{})
	u := newInventoryUploads(ctx, func(ctx context.Context, _ *pack) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	defer u.close()
	if err := u.submit(&pack{}); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	if err := u.wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("worker not joined")
	}
	if err := u.submit(&pack{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("accepted work after cancellation: %v", err)
	}
}
