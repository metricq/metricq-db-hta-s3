//go:build integration

package integration

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-go/storage"
)

type compactionIO struct {
	calls, bytes int64
	elapsed      time.Duration
}

// Embedding the concrete backend preserves Stat/List/Identity as well as CAS.
type compactionMeter struct {
	*storage.S3
	mu     sync.Mutex
	totals map[string]compactionIO
}

func (s *compactionMeter) record(op, key string, n int, start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totals == nil {
		s.totals = map[string]compactionIO{}
	}
	k := op + " " + strings.SplitN(key, "/", 2)[0]
	v := s.totals[k]
	v.calls++
	v.bytes += int64(n)
	v.elapsed += time.Since(start)
	s.totals[k] = v
}
func (s *compactionMeter) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	start := time.Now()
	v, err := s.S3.Put(ctx, key, b, expected)
	s.record("PUT", key, len(b), start)
	return v, err
}
func (s *compactionMeter) Get(ctx context.Context, key string) ([]byte, string, error) {
	start := time.Now()
	b, v, err := s.S3.Get(ctx, key)
	s.record("GET", key, len(b), start)
	return b, v, err
}
func (s *compactionMeter) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	start := time.Now()
	b, err := s.S3.GetRange(ctx, key, offset, length)
	s.record("RANGE", key, len(b), start)
	return b, err
}
func (s *compactionMeter) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.S3.Delete(ctx, key)
	s.record("DELETE", key, 0, start)
	return err
}
func (s *compactionMeter) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totals = map[string]compactionIO{}
}
func (s *compactionMeter) report(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.totals))
	for k := range s.totals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := s.totals[k]
		t.Logf("maintenance IO %s: calls=%d bytes=%d sum_request_seconds=%.3f", k, v.calls, v.bytes, v.elapsed.Seconds())
	}
}
