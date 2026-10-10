package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func env(values map[string]string) func(string) string {
	return func(k string) string { return values[k] }
}

func TestOptionPrecedence(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(config, []byte(`{"server":"amqp://file/","token":"db-file","ingest_prefetch":7,
		"s3":{"bucket":"file-bucket","prefix":"p"},"engine":{"hold_max_age_seconds":60,"compaction_job_max_blocks":64}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := parseOptions([]string{"--config", config, "--token", "db-flag", "-v", "debug"},
		env(map[string]string{"METRICQ_TOKEN": "db-env", "METRICQ_INGEST_PREFETCH": "9", "METRICQ_S3_BUCKET": "env-bucket"}), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	c := o.config
	// defaults < config file < environment < flags
	if c.Server != "amqp://file/" || c.Token != "db-flag" || c.Prefetch != 9 || c.S3.Bucket != "env-bucket" || c.S3.Prefix != "p" || c.Pprof {
		t.Fatalf("precedence: %+v", c)
	}
	if c.Engine.HoldMaxAgeSeconds != 60 || c.Engine.CompactionOptions.JobMaxBlocks != 64 || !c.Engine.CompactionOptions.Enabled || !c.Engine.MaintenanceEnabled || o.verbosity != slog.LevelDebug {
		t.Fatalf("engine options: %+v verbosity %v", c.Engine, o.verbosity)
	}
}

func TestOptionsFromEnvironmentOnly(t *testing.T) {
	o, err := parseOptions(nil, env(map[string]string{
		"METRICQ_SERVER": "amqp://$USER@broker/", "METRICQ_TOKEN": "db-$USER", "METRICQ_S3_BUCKET": "b",
		"METRICQ_S3_PATH_STYLE": "true", "METRICQ_WAL_DIR": "/data/wal", "METRICQ_VERBOSITY": "INFO", "USER": "alice",
		"METRICQ_PPROF": "true",
	}), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	c := o.config
	if c.Server != "amqp://alice@broker/" || c.Token != "db-alice" || !c.S3.PathStyle || c.Engine.WALDirectory != "/data/wal" || o.verbosity != slog.LevelInfo || !c.Pprof {
		t.Fatalf("%+v", c)
	}
	if c.Listen != "127.0.0.1:9090" || c.Prefetch != 400 || c.Engine.HoldMaxAgeSeconds != 3600 {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestOptionErrors(t *testing.T) {
	for _, tc := range []struct {
		args []string
		env  map[string]string
		want string
	}{
		{nil, map[string]string{"METRICQ_S3_BUCKET": "b"}, "--server"},
		{[]string{"--server", "amqp://x/"}, nil, "--s3-bucket"},
		{[]string{"--server", "amqp://x/", "--s3-bucket", "b", "-v", "loud"}, nil, "log level"},
		{[]string{"--server", "amqp://x/", "--s3-bucket", "b", "--ingest-prefetch", "many"}, nil, "--ingest-prefetch"},
		{[]string{"--server", "amqp://x/", "--s3-bucket", "b", "extra"}, nil, "unexpected argument"},
		{[]string{"--server", "amqp://x/", "--s3-bucket", "b", "--pprof", "maybe"}, nil, "--pprof"},
	} {
		if _, err := parseOptions(tc.args, env(tc.env), io.Discard); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: got %v, want %q", tc.args, err, tc.want)
		}
	}
}

func TestDisabledCompactionDisablesAppendOnly(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(config, []byte(`{"engine":{"compaction_enabled":false}}`), 0o600)
	o, err := parseOptions([]string{"--config", config, "--server", "amqp://x/", "--s3-bucket", "b"}, env(nil), io.Discard)
	if err != nil || o.config.Engine.CheckpointAppendOnlyAggregates {
		t.Fatalf("%v %+v", err, o.config.Engine)
	}
}

func TestExampleConfigAndUnknownKeys(t *testing.T) {
	o, err := parseOptions([]string{"--config", "../../configs/local.example.json"}, env(nil), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	e := o.config.Engine
	if o.config.Prefetch != 400 || e.CheckpointUnsavedBytes != 4<<20 || e.HoldMemoryBytes != 512<<20 || e.JobMaxBlocks != 4096 || e.ReclaimDeadFraction != 0.4 || !e.MergeEnabled {
		t.Fatalf("example config not fully applied: %+v", o.config)
	}
	config := filepath.Join(t.TempDir(), "old.json")
	os.WriteFile(config, []byte(`{"engine":{"hold_seconds":60}}`), 0o600)
	if _, err := parseOptions([]string{"--config", config, "--server", "amqp://x/", "--s3-bucket", "b"}, env(nil), io.Discard); err == nil || !strings.Contains(err.Error(), "hold_seconds") {
		t.Fatalf("unknown key accepted: %v", err)
	}
}
