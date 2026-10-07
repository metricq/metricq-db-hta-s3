//go:build ignore

// Dissertation-style query matrix (Ilsche 2020, §4.3.5) against a running
// database: spans from 1 s on a logarithmic scale, aggregate timelines with at
// least 1000 intervals and single aggregates, one random metric or six in
// parallel, random windows. Sequential configurations; client end-to-end
// latency, database-side duration and data range requests per configuration.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	metricq "github.com/metricq/metricq-go"
)

func dataRequests() float64 {
	resp, err := http.Get("http://127.0.0.1:9092/metrics")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	s := bufio.NewScanner(resp.Body)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		if line := s.Text(); strings.HasPrefix(line, "metricq_db_query_data_requests_sum") {
			f := strings.Fields(line)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			return v
		}
	}
	panic("metric not found")
}

func main() {
	dataset := flag.String("dataset", "load", "load (1000 metrics at 1 Sa/s) or dummy (dummy.source.hta-s3 at 100 Sa/s)")
	minSpan := flag.Float64("min-span", 1, "smallest span in seconds")
	maxSpan := flag.Float64("max-span", 3e5, "largest span in seconds")
	history := flag.Duration("history", 100*time.Hour, "windows end at most this long ago")
	reps := flag.Int("reps", 20, "random windows per configuration")
	perDecade := flag.Int("per-decade", 4, "spans per decade")
	flag.Parse()
	agent, _ := metricq.NewAgent("latency-probe-matrix", "amqp://admin:admin@localhost")
	ctx := context.Background()
	if err := agent.Connect(ctx); err != nil {
		panic(err)
	}
	defer agent.Close()
	h, err := metricq.NewHistoryClient(ctx, agent)
	if err != nil {
		panic(err)
	}
	defer h.Close()
	r := rand.New(rand.NewPCG(4, 35))
	metric := func() string {
		if *dataset == "dummy" {
			return "dummy.source.hta-s3"
		}
		return fmt.Sprintf("load.hta-s3.m%04d", r.IntN(1000))
	}
	targets := []int{1, 6}
	if *dataset == "dummy" {
		targets = []int{1}
	}
	var spans []float64
	for e := 0.0; e <= math.Log10(*maxSpan)+1e-9; e += 1 / float64(*perDecade) {
		if span := math.Pow(10, e); span >= *minSpan*(1-1e-9) {
			spans = append(spans, span)
		}
	}
	now := time.Now().Add(-5 * time.Minute)
	if os.Getenv("NO_HEADER") == "" {
		fmt.Println("dataset,type,metrics,span_s,rep,latency_ms,db_max_ms,data_requests,points")
	}
	for _, typ := range []string{"timeline", "aggregate"} {
		for _, n := range targets {
			for _, span := range spans {
				d := time.Duration(span * float64(time.Second))
				for rep := 0; rep < *reps; rep++ {
					end := now.Add(-time.Duration(r.Int64N(int64(*history - d))))
					start := end.Add(-d)
					names := map[string]bool{}
					for len(names) < n {
						names[metric()] = true
					}
					before := dataRequests()
					var wg sync.WaitGroup
					var mu sync.Mutex
					var dbMax time.Duration
					points := 0
					var firstErr error
					t0 := time.Now()
					for name := range names {
						wg.Add(1)
						go func(name string) {
							defer wg.Done()
							qctx, cancel := context.WithTimeout(ctx, time.Minute)
							defer cancel()
							var resp *metricq.HistoryResponse
							var dur time.Duration
							var err error
							if typ == "timeline" {
								resp, dur, err = h.Request(qctx, name, start, end, d/1000, metricq.HistoryRequest_AGGREGATE_TIMELINE)
							} else {
								resp, dur, err = h.Request(qctx, name, start, end, 0, metricq.HistoryRequest_AGGREGATE)
							}
							mu.Lock()
							defer mu.Unlock()
							if err == nil && resp.GetError() != "" {
								err = fmt.Errorf("%s", resp.GetError())
							}
							if err != nil {
								firstErr = err
								return
							}
							dbMax = max(dbMax, dur)
							points += len(resp.GetValue()) + len(resp.GetAggregate())
						}(name)
					}
					wg.Wait()
					latency := time.Since(t0)
					if firstErr != nil {
						fmt.Fprintln(os.Stderr, typ, n, span, firstErr)
						continue
					}
					fmt.Printf("%s,%s,%d,%.4g,%d,%.2f,%.2f,%.0f,%d\n", *dataset, typ, n, span, rep, latency.Seconds()*1000, dbMax.Seconds()*1000, dataRequests()-before, points)
				}
			}
		}
	}
}
