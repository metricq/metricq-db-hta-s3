// Command metricq-db-hta-s3 is the MetricQ HTA history database with S3
// object storage. It registers with the MetricQ manager under its token,
// stores the configured metrics and answers history requests.
//
// Options come from flags, METRICQ_* environment variables (also from .metricq
// files) and an optional JSON file (--config); run with --help for the list.
// Prometheus metrics are served on --metrics-listen at /metrics, readiness at
// /readyz. See docs/operations in the repository.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/metricq/metricq-db-hta-s3/engine"
	"github.com/metricq/metricq-db-hta-s3/hta"
	"github.com/metricq/metricq-db-hta-s3/storage"
	metricq "github.com/metricq/metricq-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func run() error {
	if err := loadDotMetricq(); err != nil {
		return err
	}
	o, err := parseOptions(os.Args[1:], os.Getenv, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if o.version {
		fmt.Println("metricq-db-hta-s3", version)
		return nil
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: o.verbosity})))
	cfg := o.config
	slog.Info("starting", "version", version, "token", cfg.Token, "server", redactURL(cfg.Server), "bucket", cfg.S3.Bucket, "prefix", cfg.S3.Prefix, "wal", cfg.Engine.WALDirectory)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	backend, err := storage.NewS3(ctx, cfg.S3)
	if err != nil {
		return err
	}
	// Every series carries the database token, so one Prometheus can scrape
	// several databases and the dashboard can select one.
	registry := prometheus.NewRegistry()
	labelled := prometheus.WrapRegistererWith(prometheus.Labels{"token": cfg.Token}, registry)
	labelled.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "metricq_db", Name: "build_info", Help: "Build version; value 1."}, []string{"version", "goversion"})
	labelled.MustRegister(buildInfo)
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)
	metrics := engine.NewMetrics(labelled)
	metrics.Config.WithLabelValues("ingest_prefetch").Set(float64(cfg.Prefetch))
	var mu sync.RWMutex
	var dbEngine *engine.Engine
	mapping := map[string]string{}
	var workers sync.WaitGroup
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		ready := dbEngine != nil
		mu.RUnlock()
		if !ready {
			http.Error(w, "database not initialized", 503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	httpErrors := make(chan error, 1)
	go func() { httpErrors <- server.ListenAndServe() }()
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = server.Shutdown(stopCtx)
	}()
	db, err := metricq.NewDB(cfg.Token, cfg.Server)
	if err != nil {
		return err
	}
	if cfg.Prefetch > 0 {
		db.Prefetch = cfg.Prefetch
	}
	handlers := metricq.DBHandlers{
		Configure: func(callCtx context.Context, raw json.RawMessage) ([]metricq.DBBinding, error) {
			var c struct {
				Metrics map[string]hta.Config `json:"metrics"`
			}
			if err := json.Unmarshal(raw, &c); err != nil {
				return nil, err
			}
			if len(c.Metrics) == 0 {
				return nil, fmt.Errorf("manager config requires metrics")
			}
			aliases := map[string]string{}
			bindings := make([]metricq.DBBinding, 0, len(c.Metrics))
			for name, mc := range c.Metrics {
				input := mc.Input
				if input == "" {
					input = name
				}
				if old, ok := aliases[input]; ok {
					return nil, fmt.Errorf("ambiguous input %q: %s and %s", input, old, name)
				}
				aliases[input] = name
				bindings = append(bindings, metricq.DBBinding{Name: name, Input: input})
			}
			sort.Slice(bindings, func(i, j int) bool { return bindings[i].Name < bindings[j].Name })
			mu.Lock()
			defer mu.Unlock()
			for input, name := range mapping {
				if next, ok := aliases[input]; !ok || next != name {
					return nil, fmt.Errorf("removing or remapping active inputs requires restart")
				}
			}
			if dbEngine == nil {
				e, err := engine.Open(callCtx, backend, cfg.Engine, c.Metrics, metrics)
				if err != nil {
					return nil, err
				}
				dbEngine = e
				workers.Add(2)
				go func() { defer workers.Done(); e.RunFlush(ctx) }()
				go func() { defer workers.Done(); e.RunMaintenance(ctx) }()
			} else if err := dbEngine.Configure(c.Metrics); err != nil {
				return nil, err
			}
			mapping = aliases
			return bindings, nil
		},
		// Prefetched deliveries share one WAL fsync and one multiple ACK.
		DataBatch: func(callCtx context.Context, messages []metricq.DataMessage) error {
			mu.RLock()
			e := dbEngine
			deliveries := make([]engine.Delivery, len(messages))
			for i, m := range messages {
				deliveries[i] = engine.Delivery{Metric: mapping[m.Input], Chunk: m.Chunk}
			}
			mu.RUnlock()
			if e == nil {
				return fmt.Errorf("database not ready")
			}
			for i, d := range deliveries {
				if d.Metric == "" {
					return fmt.Errorf("unconfigured input %q", messages[i].Input)
				}
			}
			for len(deliveries) > 0 {
				n, err := e.IngestBatch(callCtx, deliveries)
				deliveries = deliveries[n:]
				if err == nil || len(deliveries) == 0 {
					continue
				}
				if !errors.Is(err, engine.ErrPressure) {
					return err
				}
				if err = e.Flush(callCtx); err != nil {
					slog.Warn("WAL pressure: leaving delivery unacknowledged", "error", err)
					select {
					case <-callCtx.Done():
						return callCtx.Err()
					case <-time.After(time.Second):
					}
				}
			}
			return nil
		},
		History: func(callCtx context.Context, name string, req *metricq.HistoryRequest) (*metricq.HistoryResponse, error) {
			mu.RLock()
			e := dbEngine
			mu.RUnlock()
			if e == nil {
				return nil, fmt.Errorf("database not ready")
			}
			return e.Query(callCtx, name, req)
		},
	}
	dbErrors := make(chan error, 1)
	go func() { dbErrors <- db.Run(ctx, handlers) }()
	select {
	case err = <-dbErrors:
		cancel()
	case err = <-httpErrors:
		cancel()
		<-dbErrors
	case <-ctx.Done():
		err = <-dbErrors
	}
	workers.Wait()
	if dbEngine != nil {
		flushCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
		if flushErr := dbEngine.Flush(flushCtx); flushErr != nil {
			slog.Warn("shutdown checkpoint failed; durable data retained in WAL", "error", flushErr)
		}
		done()
		if closeErr := dbEngine.Close(); err == nil {
			err = closeErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// redactURL removes credentials from an AMQP URL for logging.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.User(u.User.Username())
	return u.String()
}

func main() {
	if err := run(); err != nil {
		slog.Error("database stopped", "error", err)
		os.Exit(1)
	}
}
