# metricq-db-hta-s3

MetricQ history database in Go. It aggregates incoming time series into the
HTA hierarchy, keeps a local write-ahead log for durability and stores all
data as immutable objects in S3-compatible object storage. It answers all
MetricQ history request types and can replace the file-based
`metricq-db-hta` for the same manager configuration.

**Documentation:** [`docs/`](docs/index.md), published with GitHub Pages —
architecture, requirements, sizing, deployment, configuration, tuning,
monitoring and troubleshooting.

## Quick start

Requires Linux and Go 1.25.

```sh
go build ./cmd/metricq-db-hta-s3 ./cmd/metricq-db-hta-s3-wal-repair
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
./metricq-db-hta-s3 --server amqp://admin:admin@localhost/ --token db-hta-s3 \
  --s3-bucket metricq --s3-prefix db-hta-s3 --s3-endpoint http://localhost:9000 \
  --s3-path-style true --wal-dir ./wal -v info
curl http://127.0.0.1:9090/metrics
```

The manager needs a configuration document for the token (see
`configs/manager.example.json` and
[Deployment](docs/operations/deployment.md)). All options can also be given as
`METRICQ_*` environment variables or in a JSON file (`--config`,
`configs/local.example.json`); see
[Configuration](docs/operations/configuration.md).

Container image and a compose file for the MetricQ development environment,
including Prometheus and Grafana with the dashboard:

```sh
docker build -t metricq-db-hta-s3 .
cd docker && docker compose -f compose.metricq-dev.yml --profile monitoring up --build
```

## Repository

| Path | Content |
| --- | --- |
| `cmd/` | executable, WAL repair tool, S3 feature probe |
| `engine/`, `hta/`, `storage/` | storage engine, aggregation, object store |
| `integration/` | integration tests and benchmarks |
| `docs/` | documentation (MkDocs) |
| `grafana/` | dashboard and generator |
| `docker/` | container entrypoint, development compose file |
| `measurements/` | benchmark reports and design history |

## Tests

```sh
go test -race ./...
docker compose -f compose.test.yml up -d
go test -race -tags=integration ./integration -count=1
```

See [Testing and benchmarks](docs/development/testing.md).
