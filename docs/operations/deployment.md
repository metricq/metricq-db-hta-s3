# Deployment

## 1. Prepare S3

Create a bucket (or reuse one) and choose a prefix for this database, e.g.
`db-hta-s3/`. Check the endpoint with `check-ceph-s3` (see
[Requirements](requirements.md#object-storage-s3)). Create credentials limited
to the prefix.

## 2. Register the database with the manager

The MetricQ manager reads the database configuration from the CouchDB
`config` database, document id = token. Metric names must be unique across
all databases: if the legacy database already stores `dummy.source`, store the
same input under another name.

```json
{
  "metrics": {
    "dummy.source.hta-s3": {
      "input": "dummy.source",
      "interval_min": 400000000,
      "interval_max": 400000000000000,
      "interval_factor": 10
    }
  }
}
```

```sh
curl -X PUT -H 'Content-Type: application/json' \
  http://admin:admin@couchdb:5984/config/db-hta-s3 --data-binary @db-hta-s3.json
```

## 3. Run

### Container

```sh
docker build -t metricq-db-hta-s3 .

docker run -d --name db-hta-s3 \
  -v db-hta-s3:/var/lib/metricq-db-hta-s3 \
  -p 127.0.0.1:9090:9090 \
  -e METRICQ_SERVER=amqp://user:pass@rabbitmq/ \
  -e METRICQ_TOKEN=db-hta-s3 \
  -e METRICQ_S3_BUCKET=metricq -e METRICQ_S3_PREFIX=db-hta-s3/ \
  -e METRICQ_S3_ENDPOINT=https://s3.example.org -e METRICQ_S3_PATH_STYLE=true \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  metricq-db-hta-s3
```

- The volume holds the WAL and **must be persistent**; losing it loses
  acknowledged samples that are not yet in S3.
- The image also accepts the variables of the MetricQ development environment:
  `token`, `metricq_url` and `wait_for_rabbitmq_url` (host:port to wait for,
  with `WAITFORIT_TIMEOUT` seconds, 0 = forever). Without waiting, the process
  exits if RabbitMQ is not reachable at start; restart policies handle that.
- `--build-arg VERSION=…` sets the reported version.
- Metrics listen on `0.0.0.0:9090` inside the container.

### MetricQ development environment

`docker/compose.metricq-dev.yml` adds the database, a local S3 (RustFS) and a
seeded configuration `db-hta-s3-dummy` to a running development stack of
[metricq/metricq](https://github.com/metricq/metricq):

```sh
cd docker
docker compose -f compose.metricq-dev.yml up --build
# with Prometheus (127.0.0.1:9091) and Grafana (127.0.0.1:3030, anonymous):
docker compose -f compose.metricq-dev.yml --profile monitoring up --build
```

It expects the stack's network `metricq_metricq-network`.

### systemd

```ini
[Unit]
Description=MetricQ HTA database (S3)
After=network-online.target

[Service]
User=metricq
EnvironmentFile=/etc/metricq-db-hta-s3/env
ExecStart=/usr/local/bin/metricq-db-hta-s3 --config /etc/metricq-db-hta-s3/config.json
Restart=on-failure
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

`/etc/metricq-db-hta-s3/env` holds `METRICQ_SERVER`, `METRICQ_TOKEN`, the S3
options and AWS credentials; `config.json` only tuning options.

## 4. Verify

- `curl http://127.0.0.1:9090/readyz` returns 200 once the manager sent the
  configuration and the engine opened.
- `metricq_db_samples_total` increases; `metricq_db_series` equals the number
  of configured metrics.
- Import `grafana/metricq-db-hta-s3.json` into Grafana and select the token
  (see [Monitoring](monitoring.md)).

## Shutdown and upgrades

SIGTERM stops AMQP consumption and history workers and runs a final
checkpoint (up to 30 s). If S3 is unreachable, the data stays in the WAL and
is replayed at the next start. Allow at least 60 s stop timeout.

The storage format is versioned in the manifest; releases state explicitly if
they change it. There is no importer from the legacy file format; run both
databases in parallel under different metric names while migrating.
