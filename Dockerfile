# Build from the parent workspace to include the extended metricq-go checkout:
# docker build -f metricq-db-hta-go/Dockerfile -t metricq-db-hta-go .
FROM golang:1.26 AS build
WORKDIR /src
COPY metricq-go/ metricq-go/
COPY metricq-db-hta-go/ metricq-db-hta-go/
WORKDIR /src/metricq-db-hta-go
RUN CGO_ENABLED=0 go build -trimpath -o /metricq-db-hta-go ./cmd/metricq-db-hta-go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/metricq && chown 65532:65532 /var/lib/metricq
COPY --from=build /metricq-db-hta-go /usr/local/bin/metricq-db-hta-go
USER 65532:65532
WORKDIR /var/lib/metricq
ENTRYPOINT ["/usr/local/bin/metricq-db-hta-go"]
CMD ["-config", "/etc/metricq/config.json"]
