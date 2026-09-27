# Testing and benchmarks

## Unit tests

```sh
go test ./...
go test -race ./...           # required before committing engine changes
(cd ../metricq-go && go test -race ./...)
```

Engine tests use an in-memory object store (`memoryStore`, `gcStore`) and a
temporary WAL directory. Many tests compare every request type against a
reference engine without the feature under test; failure-injection stores
cover lost PUT responses, failed uploads, blocked uploads and concurrent
writers. Tests that simulate time inject `e.now`.

## Integration tests

They use the MetricQ development environment (RabbitMQ, CouchDB, manager) and
an S3 test service, create uniquely named resources and remove them afterwards.

```sh
docker compose -f compose.test.yml up -d        # S3 on 127.0.0.1:19000
go test -race -tags=integration ./integration -count=1 -v
docker compose -f compose.test.yml down
```

| Variable | Default |
| --- | --- |
| `METRICQ_AMQP` | `amqp://admin:admin@localhost/` |
| `METRICQ_COUCHDB` | `http://admin:admin@localhost:5984` |
| `METRICQ_DOCKER_NETWORK` | `metricq_metricq-network` |
| `METRICQ_DOCKER_AMQP` | `amqp://admin:admin@rabbitmq-server/` |
| `METRICQ_LEGACY_IMAGE` | `metricq-db-hta` |
| `METRICQ_TEST_S3` | `http://localhost:19000` |

The parity tests start the legacy C++ database and compare thousands of
response pairs for all request types, across WAL replay, S3-only recovery,
compaction and storage outages.

## Benchmarks

Benchmarks are integration tests selected with `-run` and configured with
`METRICQ_BENCH_*`, `METRICQ_INGEST_*`, `METRICQ_CARDINALITY_*` and
`METRICQ_COMPACTION_*` variables. Each report in `measurements/` contains its
exact command. Write outputs into `measurements/` and describe the setup and
the limits of a result next to the numbers.

| Test | Measures |
| --- | --- |
| `TestIngestThroughput` | end-to-end ingest rate by prefetch and batching |
| `TestRequestLatency` | query latency and range GETs against the legacy database |
| `TestMetricCardinality` | many active metrics with fixed memory |
| `TestCompactionS3` | compaction cost on real S3 |
| `TestLegacyRequestParity` | response parity with the legacy database |

Diagnostic probes that document known limitations use the `review` build tag.

## Documentation

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements-docs.txt
.venv/bin/mkdocs serve          # http://127.0.0.1:8000
.venv/bin/mkdocs build --strict
python3 grafana/generate_dashboard.py
python3 scripts/metrics-reference.py   # table for operations/monitoring.md
```

GitLab CI builds the site on every branch and publishes it with GitLab Pages
from the default branch (`.gitlab-ci.yml`).
