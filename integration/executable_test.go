//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/metricq/metricq-db-hta-s3/engine"
	"github.com/metricq/metricq-db-hta-s3/hta"
	metricq "github.com/metricq/metricq-go"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

func TestExecutablePrometheusAndShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id := fmt.Sprintf("hta-s3-cli-%d", time.Now().UnixNano())
	token := "db-" + id
	metric := id + ".sample"
	server := env("METRICQ_AMQP", "amqp://admin:admin@localhost/")
	t.Cleanup(func() {
		conn, err := amqp.Dial(server)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Error(err)
			return
		}
		defer ch.Close()
		for _, name := range []string{token + "-data", token + "-hreq"} {
			_, _ = ch.QueueDelete(name, false, false, false)
		}
	})
	backend, _ := newS3(t, ctx, id)
	cfg := map[string]hta.Config{metric: {IntervalMin: 100000000, IntervalMax: 10000000000, IntervalFactor: 10}}
	seed(t, "metadata", metric, map[string]any{"description": "CLI smoke test"})
	seed(t, "config", token, map[string]any{"metrics": cfg})
	dir := t.TempDir()
	binary := filepath.Join(dir, "metricq-db-hta-s3")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../cmd/metricq-db-hta-s3")
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %s %v", b, err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	configPath := filepath.Join(dir, "config.json")
	b, err := json.Marshal(map[string]any{"server": server, "token": token, "listen": address, "s3": map[string]any{"bucket": id, "endpoint": env("METRICQ_TEST_S3", "http://localhost:19000"), "path_style": true}, "engine": map[string]any{"wal_directory": filepath.Join(dir, "wal")}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(configPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "db.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	command := exec.CommandContext(ctx, binary, "-config", configPath)
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		if t.Failed() {
			b, _ := os.ReadFile(logPath)
			t.Log(string(b))
		}
	}()
	client := http.Client{Timeout: time.Second}
	poll := func(path, contains string) {
		deadline := time.Now().Add(20 * time.Second)
		for {
			resp, err := client.Get("http://" + address + path)
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 && strings.Contains(string(b), contains) {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("endpoint %s not ready (%s)", path, contains)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	poll("/readyz", "")
	poll("/metrics", "metricq_db_wal_pressure_ratio")
	// Subscription follows Configure; verify it has happened before publishing.
	conn, err := amqp.Dial(server)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	// /readyz currently signifies the initialized engine; retry publishing the
	// same timestamp safely until ingestion confirms the binding is active.
	payload, _ := proto.Marshal(&metricq.DataChunk{TimeDelta: []int64{1000000000}, Value: []float64{42}})
	for i := 0; i < 5; i++ {
		if err = ch.PublishWithContext(ctx, "metricq.data", metric, false, false, amqp.Publishing{Body: payload, DeliveryMode: amqp.Persistent}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	poll("/metrics", "metricq_db_samples_total{token=\""+token+"\"} 1\n")
	if err = command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	stopped = true
	if err != nil {
		t.Fatalf("unclean shutdown: %v", err)
	}
	restored, err := engine.Open(ctx, backend, engine.Options{WALDirectory: t.TempDir()}, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	resp, err := restored.Query(ctx, metric, &metricq.HistoryRequest{Type: metricq.HistoryRequest_LAST_VALUE})
	if err != nil || len(resp.GetValue()) != 1 || resp.Value[0] != 42 {
		t.Fatalf("shutdown did not checkpoint data to S3: %v %v", resp, err)
	}
}
