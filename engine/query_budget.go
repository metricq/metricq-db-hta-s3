package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Wire sizes of typical history response points, used to reject requests
// before reading their blocks: a raw point is a varint time delta plus a
// double, an aggregate point a time delta plus six fields. The encoded
// response is checked exactly before it is returned.
const (
	rawPointBytes       = 13
	aggregatePointBytes = 53
)

// errQueryMemoryBusy reports that a query needing more memory could not get
// it without waiting while already holding a reservation.
var errQueryMemoryBusy = errors.New("query memory budget exhausted by concurrent queries; retry later")

// queryBudget bounds the decoded records of all running history queries
// together. Queries reserve their need before reading blocks; a query that
// does not fit waits until others release memory.
type queryBudget struct {
	mu       sync.Mutex
	capacity int64
	free     int64
	released chan struct{} // closed and replaced on every release
}

func newQueryBudget(capacity int64) *queryBudget {
	return &queryBudget{capacity: capacity, free: capacity, released: make(chan struct{})}
}

func (b *queryBudget) acquire(ctx context.Context, n int64, wait bool) error {
	if n > b.capacity {
		return fmt.Errorf("history query needs %d MiB of decoded records, more than query_memory_bytes (%d MiB); request a shorter range or a larger interval", n>>20, b.capacity>>20)
	}
	for {
		b.mu.Lock()
		if n <= b.free {
			b.free -= n
			b.mu.Unlock()
			return nil
		}
		released := b.released
		b.mu.Unlock()
		if !wait {
			return errQueryMemoryBusy
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for query memory: %w", ctx.Err())
		case <-released:
		}
	}
}

func (b *queryBudget) release(n int64) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	b.free += n
	close(b.released)
	b.released = make(chan struct{})
	b.mu.Unlock()
}

// queryReservation is the memory one history query holds. Only its first
// reservation may wait: waiting while holding memory could stall queries
// that wait for each other.
type queryReservation struct {
	budget *queryBudget
	held   atomic.Int64
	// requests counts the query's data range requests (levels read concurrently).
	requests atomic.Int64
}

func (r *queryReservation) reserve(ctx context.Context, n int64) error {
	if r == nil || r.budget == nil || n <= 0 {
		return nil
	}
	if err := r.budget.acquire(ctx, n, r.held.Load() == 0); err != nil {
		return err
	}
	r.held.Add(n)
	return nil
}

func (r *queryReservation) releaseAll() {
	if r == nil || r.budget == nil {
		return
	}
	r.budget.release(r.held.Swap(0))
}
