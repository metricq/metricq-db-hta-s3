# syntax=docker/dockerfile:1
#
# Build from this repository:
#   docker build -t metricq-db-hta-go .
#   docker build --build-arg VERSION=1.2.3 -t metricq-db-hta-go .

ARG GO_VERSION=1.25

FROM golang:${GO_VERSION} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/ ./cmd/metricq-db-hta-go ./cmd/metricq-db-hta-wal-repair

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1000 --home-dir /var/lib/metricq-db-hta-go metricq \
    && mkdir -p /var/lib/metricq-db-hta-go/wal \
    && chown -R metricq:metricq /var/lib/metricq-db-hta-go
COPY --from=build /out/ /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
USER metricq
WORKDIR /var/lib/metricq-db-hta-go
# The WAL must survive container restarts: mount a volume here.
VOLUME /var/lib/metricq-db-hta-go
ENV METRICQ_WAL_DIR=/var/lib/metricq-db-hta-go/wal \
    METRICQ_METRICS_LISTEN=0.0.0.0:9090 \
    METRICQ_VERBOSITY=info
EXPOSE 9090
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
