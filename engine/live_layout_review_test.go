//go:build review

package engine

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/storage"
	"github.com/prometheus/client_golang/prometheus"
)

// Read-only object layout report of a live database: streams the catalog and
// prints aggregates by object origin, size and age. It issues only GETs.
//
//	METRICQ_LIVE_S3_ENDPOINT=http://127.0.0.1:19001 METRICQ_LIVE_S3_BUCKET=metricq \
//	METRICQ_LIVE_S3_PREFIX=db-hta-s3-dummy AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//	go test -tags review ./engine -run TestReviewLiveObjectLayout -v
func TestReviewLiveObjectLayout(t *testing.T) {
	endpoint := os.Getenv("METRICQ_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set METRICQ_LIVE_S3_ENDPOINT")
	}
	ctx := context.Background()
	s, err := storage.NewS3(ctx, storage.S3Config{Bucket: os.Getenv("METRICQ_LIVE_S3_BUCKET"), Prefix: os.Getenv("METRICQ_LIVE_S3_PREFIX"), Endpoint: endpoint, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{store: s, metrics: NewMetrics(prometheus.NewRegistry()), options: Options{}}
	b, _, err := e.get(ctx, "manifest")
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := decode(b, &m); err != nil {
		t.Fatal(err)
	}
	origin := func(key string) string {
		switch {
		case strings.HasPrefix(key, "index/"):
			return "index"
		case strings.HasPrefix(key, "data/compact-"):
			return "compact"
		}
		return "checkpoint"
	}
	sizeBucket := func(n int64) string {
		for _, b := range []struct {
			limit int64
			name  string
		}{{64 << 10, "<64K"}, {256 << 10, "<256K"}, {1 << 20, "<1M"}, {2 << 20, "<2M"}, {4 << 20, "<4M"}} {
			if n < b.limit {
				return b.name
			}
		}
		return ">=4M"
	}
	now := time.Now()
	ageBucket := func(created int64) string {
		age := now.Sub(time.Unix(0, created))
		switch {
		case age < time.Hour:
			return "<1h"
		case age < 6*time.Hour:
			return "<6h"
		case age < 24*time.Hour:
			return "<24h"
		}
		return ">=24h"
	}
	type stat struct {
		objects, blocks, small int
		size, live             int64
	}
	byOriginSize := map[string]*stat{}
	byOriginAge := map[string]*stat{}
	smallOnly := map[string]*stat{} // objects whose data blocks are all small
	levels := map[string]map[int64]int{}
	blocksPer := map[string][]int{}
	examples := map[string][]string{}
	smallObjects := map[string]map[string]int{}
	get := func(m map[string]*stat, k string) *stat {
		if m[k] == nil {
			m[k] = &stat{}
		}
		return m[k]
	}
	err = e.catalogWalk(ctx, m.Catalog, math.MaxInt, func(o ObjectInfo) bool {
		org := origin(o.Key)
		data, small := 0, 0
		lv := map[int64]int{}
		for _, b := range o.Blocks {
			if b.Index {
				continue
			}
			data++
			lv[b.Level]++
			if b.Entry.Records <= maxDataBlockRecords/2 {
				small++
			}
		}
		for _, st := range []*stat{get(byOriginSize, org+" "+sizeBucket(o.Size)), get(byOriginAge, org+" "+ageBucket(o.Created))} {
			st.objects++
			st.blocks += data
			st.small += small
			st.size += o.Size
			st.live += o.LiveBytes
		}
		if o.Size < 256<<10 && org != "locality" {
			k := org + " " + ageBucket(o.Created)
			if smallObjects[k] == nil {
				smallObjects[k] = map[string]int{}
			}
			so := smallObjects[k]
			so["objects"]++
			dead := float64(o.Size-o.LiveBytes) / float64(o.Size)
			so[fmt.Sprintf("dead<%.0f%%", 10*math.Ceil(dead*10+1e-9))]++
			for l, n := range lv {
				so[fmt.Sprintf("level=%d", l)] += n
			}
			so["full blocks"] += data - small
			so["small blocks"] += small
			if data == 0 {
				so["index-only"]++
			}
		}
		if data > 0 && small == data {
			k := org + " " + ageBucket(o.Created)
			st := get(smallOnly, k)
			st.objects++
			st.blocks += data
			st.size += o.Size
			st.live += o.LiveBytes
			if levels[k] == nil {
				levels[k] = map[int64]int{}
			}
			for l, n := range lv {
				levels[k][l] += n
			}
			blocksPer[k] = append(blocksPer[k], data)
			if len(examples[k]) < 3 {
				examples[k] = append(examples[k], fmt.Sprintf("%s size=%d live=%d blocks=%d levels=%v", o.Key, o.Size, o.LiveBytes, data, lv))
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	print := func(title string, m map[string]*stat) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Logf("== %s", title)
		for _, k := range keys {
			st := m[k]
			t.Logf("%-22s objects=%5d blocks=%7d small=%7d size=%7.1fMB live=%7.1fMB", k, st.objects, st.blocks, st.small, float64(st.size)/1e6, float64(st.live)/1e6)
		}
	}
	print("by origin and size", byOriginSize)
	print("by origin and age", byOriginAge)
	print("small-only objects by origin and age", smallOnly)
	for k, l := range levels {
		n := blocksPer[k]
		sort.Ints(n)
		t.Logf("small-only %s: levels %v, data blocks per object p50=%d max=%d", k, l, n[len(n)/2], n[len(n)-1])
		for _, ex := range examples[k] {
			t.Logf("  e.g. %s", ex)
		}
	}
	keys := make([]string, 0, len(smallObjects))
	for k := range smallObjects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("objects <256K %s: %v", k, smallObjects[k])
	}
	candidates := map[string]int{}
	err = e.catalogWalk(ctx, m.Candidates, math.MaxInt, func(o ObjectInfo) bool {
		candidates[origin(o.Target)]++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidates by origin: %v; live objects %d", candidates, m.LiveObjects)
}
