package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/metricq/metricq-db-hta-s3/engine"
	"github.com/metricq/metricq-db-hta-s3/storage"
)

// version is set at build time: -ldflags "-X main.version=...".
var version = "dev"

// localConfig is the optional JSON file (--config). Metric definitions come
// from the MetricQ manager; this file holds connection, storage and tuning.
type localConfig struct {
	Server   string           `json:"server"`
	Token    string           `json:"token"`
	Listen   string           `json:"metrics_listen"`
	Prefetch int              `json:"ingest_prefetch"`
	Pprof    bool             `json:"pprof"`
	S3       storage.S3Config `json:"s3"`
	Engine   engine.Options   `json:"engine"`
}

// defaultConfig holds the executable's defaults; the engine applies its own
// defaults for everything left at zero.
func defaultConfig() localConfig {
	var cfg localConfig
	cfg.Token = "db-hta-s3"
	cfg.Listen = "127.0.0.1:9090"
	cfg.Prefetch = 400
	cfg.Engine.WALDirectory = "/var/lib/metricq-db-hta-s3/wal"
	cfg.Engine.CheckpointAppendOnlyAggregates = true
	cfg.Engine.HoldMaxAgeSeconds = 3600
	cfg.Engine.CompactionOptions.Enabled = true
	cfg.Engine.CompactionOptions.MergeEnabled = true
	cfg.Engine.CompactionOptions.MergeCooldownSeconds = 60
	cfg.Engine.CompactionOptions.JobMaxBlocks = 4096
	cfg.Engine.CompactionOptions.CycleMaxSeconds = 30
	return cfg
}

type options struct {
	config    localConfig
	verbosity slog.Level
	version   bool
}

// option is one command-line flag; env names the METRICQ_* variable that
// provides it when the flag is not given.
type option struct {
	name, env, usage string
	set              func(string) error
}

// parseOptions applies, in increasing precedence: executable defaults, the
// --config file, METRICQ_* environment variables (also read from .metricq
// files in the working and home directory), and command-line flags.
func parseOptions(args []string, getenv func(string) string, stderr io.Writer) (options, error) {
	var o options
	o.config = defaultConfig()
	o.verbosity = slog.LevelWarn
	fs := flag.NewFlagSet("metricq-db-hta-s3", flag.ContinueOnError)
	fs.SetOutput(stderr)
	values := map[string]*string{}
	var defs []option
	def := func(name, usage string, set func(string) error) {
		defs = append(defs, option{name: name, env: "METRICQ_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")), usage: usage, set: set})
	}
	cfg := &o.config
	def("server", "MetricQ server URL; $USER and $HOST are replaced", func(v string) error { cfg.Server = placeholders(v, getenv); return nil })
	def("token", "client token of this database (default db-hta-s3); $USER and $HOST are replaced", func(v string) error { cfg.Token = placeholders(v, getenv); return nil })
	def("verbosity", "log level: debug, info, warning or error (default warning)", func(v string) error {
		level, err := parseLevel(v)
		o.verbosity = level
		return err
	})
	def("metrics-listen", "address of the Prometheus /metrics and /readyz endpoint (default 127.0.0.1:9090)", func(v string) error { cfg.Listen = v; return nil })
	def("pprof", "serve Go profiles under /debug/pprof/ on the metrics address; only on trusted networks (default false)", func(v string) error {
		b, err := strconv.ParseBool(v)
		cfg.Pprof = b
		return err
	})
	def("ingest-prefetch", "AMQP data prefetch; deliveries of one batch share a WAL fsync (default 400, as metricq-go and the file database)", func(v string) error { return setInt(&cfg.Prefetch, v) })
	def("wal-dir", "local WAL directory on durable storage (default /var/lib/metricq-db-hta-s3/wal)", func(v string) error { cfg.Engine.WALDirectory = v; return nil })
	def("s3-bucket", "S3 bucket", func(v string) error { cfg.S3.Bucket = v; return nil })
	def("s3-prefix", "key prefix inside the bucket; one database per prefix", func(v string) error { cfg.S3.Prefix = v; return nil })
	def("s3-endpoint", "S3 endpoint URL, e.g. https://s3.example.org (default AWS)", func(v string) error { cfg.S3.Endpoint = v; return nil })
	def("s3-region", "S3 region (default us-east-1)", func(v string) error { cfg.S3.Region = v; return nil })
	def("s3-path-style", "use path-style bucket addressing (true for most non-AWS endpoints)", func(v string) error {
		b, err := strconv.ParseBool(v)
		cfg.S3.PathStyle = b
		return err
	})
	for _, d := range defs {
		v := new(string)
		values[d.name] = v
		fs.StringVar(v, d.name, "", d.usage+" [$"+d.env+"]")
	}
	fs.StringVar(values["verbosity"], "v", "", "shorthand for --verbosity")
	configPath := fs.String("config", "", "optional JSON file with connection, S3 and engine tuning options [$METRICQ_CONFIG]")
	fs.BoolVar(&o.version, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: metricq-db-hta-s3 [options]\n\nMetricQ HTA database storing aggregated time series in S3.\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nAll options can be passed as environment variables prefixed with METRICQ_, e.g.\nMETRICQ_SERVER=amqps://... A .metricq file in the working or home directory can\nprovide such variables. S3 credentials use the standard AWS variables\n(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or AWS profiles.\n")
	}
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if o.version {
		return o, nil
	}
	path := *configPath
	if path == "" {
		path = getenv("METRICQ_CONFIG")
	}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return o, err
		}
		// Unknown keys are errors, so renamed or misspelled options do not
		// silently fall back to defaults.
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(cfg); err != nil {
			return o, fmt.Errorf("%s: %w", path, err)
		}
	}
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	if given["v"] {
		given["verbosity"] = true
	}
	for _, d := range defs {
		v := *values[d.name]
		if !given[d.name] {
			if v = getenv(d.env); v == "" {
				continue
			}
		}
		if err := d.set(v); err != nil {
			return o, fmt.Errorf("--%s: %w", d.name, err)
		}
	}
	if cfg.Server == "" {
		return o, errors.New("--server (or METRICQ_SERVER) is required")
	}
	if cfg.Token == "" {
		return o, errors.New("--token must not be empty")
	}
	if cfg.S3.Bucket == "" {
		return o, errors.New("--s3-bucket (or METRICQ_S3_BUCKET) is required")
	}
	// The executable always runs background maintenance; append-only
	// aggregates depend on block consolidation.
	cfg.Engine.MaintenanceEnabled = true
	if !cfg.Engine.CompactionOptions.Enabled || !cfg.Engine.CompactionOptions.MergeEnabled {
		cfg.Engine.CheckpointAppendOnlyAggregates = false
	}
	return o, nil
}

func setInt(dst *int, v string) error {
	n, err := strconv.Atoi(v)
	*dst = n
	return err
}

func parseLevel(v string) (slog.Level, error) {
	switch strings.ToLower(v) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warning", "warn":
		return slog.LevelWarn, nil
	case "error", "critical":
		return slog.LevelError, nil
	}
	return slog.LevelWarn, fmt.Errorf("unknown log level %q", v)
}

// placeholders replaces $USER and $HOST like the other MetricQ clients.
func placeholders(v string, getenv func(string) string) string {
	host, _ := os.Hostname()
	user := getenv("USER")
	return strings.NewReplacer("$USER", user, "${USER}", user, "$HOST", host, "${HOST}", host).Replace(v)
}

// loadDotMetricq sets variables from .metricq files (working directory first,
// then home) that are not already set in the environment.
func loadDotMetricq() error {
	var paths []string
	if wd, err := os.Getwd(); err == nil {
		paths = append(paths, filepath.Join(wd, ".metricq"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".metricq"))
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, value, ok := strings.Cut(strings.TrimPrefix(line, "export "), "=")
			if !ok {
				continue
			}
			key, value = strings.TrimSpace(key), strings.Trim(strings.TrimSpace(value), `"'`)
			if _, set := os.LookupEnv(key); !set {
				os.Setenv(key, value)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}
