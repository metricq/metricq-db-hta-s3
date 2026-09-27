//go:build review

package engine

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/hta"
)

// Codec-only opportunity estimate, not a production patch format. The absolute
// level replacements can preserve floating-point values without replaying HTA.
// It excludes periodic bases, references, PUT latency and recovery overhead.
func TestReviewSeriesPatchOpportunity(t *testing.T) {
	type patch struct {
		Last   hta.Point
		Levels map[int64]hta.Level
	}
	const base = int64(1700000000) * int64(time.Second)
	series := map[string]*hta.Series{}
	for i := 0; i < 1500; i++ {
		s := hta.New(hta.Config{IntervalMin: int64(time.Second), IntervalMax: 10000000 * int64(time.Second), IntervalFactor: 10})
		for j := 0; j < 3600; j++ {
			s.Insert(hta.Point{Time: base + int64(j)*int64(time.Second), Value: math.Sin(float64(j)/100) + float64(i)/1500}, func(hta.Record) {})
		}
		series[fmt.Sprintf("canonical.metric.%04d", i)] = s
	}
	before := cloneSeries(series)
	snapshots := [metadataShards]map[string]*hta.Series{}
	patches := [metadataShards]map[string]patch{}
	changedLevels, totalLevels := 0, 0
	for name, s := range series {
		for j := 3600; j < 3630; j++ {
			s.Insert(hta.Point{Time: base + int64(j)*int64(time.Second), Value: math.Sin(float64(j)/100) + s.First.Value}, func(hta.Record) {})
		}
		p := patch{Last: s.Last, Levels: map[int64]hta.Level{}}
		for level, v := range s.Levels {
			totalLevels++
			if before[name].Levels[level] != v {
				p.Levels[level] = v
				changedLevels++
			}
		}
		rebuilt := *before[name]
		rebuilt.Last = p.Last
		for level, v := range p.Levels {
			rebuilt.Levels[level] = v
		}
		if !reflect.DeepEqual(&rebuilt, s) {
			t.Fatal("absolute replacements differ")
		}
		i := metadataShard(name)
		if snapshots[i] == nil {
			snapshots[i] = map[string]*hta.Series{}
			patches[i] = map[string]patch{}
		}
		snapshots[i][name] = s
		patches[i][name] = p
	}
	snapshotBytes, patchBytes := 0, 0
	for i := 0; i < metadataShards; i++ {
		if snapshots[i] == nil {
			continue
		}
		b, err := encode(snapshots[i])
		if err != nil {
			t.Fatal(err)
		}
		snapshotBytes += len(b)
		b, err = encode(patches[i])
		if err != nil {
			t.Fatal(err)
		}
		patchBytes += len(b)
	}
	t.Logf("1500 active series, 30s checkpoint: snapshot=%dB candidate_patch=%dB savings=%.1f%% changed_levels=%d/%d (excludes bases/recovery)", snapshotBytes, patchBytes, 100*(1-float64(patchBytes)/float64(snapshotBytes)), changedLevels, totalLevels)
}
