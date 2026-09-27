package engine

import (
	"context"
	"sync"
)

const inventoryUploadConcurrency = 4

// Only immutable, already registered packs enter the queue. Preparation and
// staging-key registration stay on the caller goroutine. The queue bounds both
// in-flight requests and retained pack buffers; it never retries a failed PUT.
type inventoryUploads struct {
	ctx    context.Context
	cancel context.CancelFunc
	slots  chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
	put    func(context.Context, *pack) error
}

func newInventoryUploads(ctx context.Context, put func(context.Context, *pack) error) *inventoryUploads {
	ctx, cancel := context.WithCancel(ctx)
	return &inventoryUploads{ctx: ctx, cancel: cancel, slots: make(chan struct{}, inventoryUploadConcurrency), put: put}
}

func (u *inventoryUploads) submit(p *pack) error {
	select {
	case u.slots <- struct{}{}:
	case <-u.ctx.Done():
		return u.failure()
	}
	if u.ctx.Err() != nil {
		<-u.slots
		return u.failure()
	}
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		defer func() { <-u.slots }()
		if err := u.put(u.ctx, p); err != nil {
			u.mu.Lock()
			if u.err == nil {
				u.err = err
			}
			u.mu.Unlock()
			u.cancel()
		}
	}()
	return nil
}

func (u *inventoryUploads) failure() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.err != nil {
		return u.err
	}
	return u.ctx.Err()
}

func (u *inventoryUploads) wait() error {
	u.wg.Wait()
	return u.failure()
}

func (u *inventoryUploads) close() {
	u.cancel()
	u.wg.Wait()
}
