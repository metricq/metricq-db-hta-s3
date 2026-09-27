#!/bin/bash
# Container entrypoint: accepts the variables of the MetricQ development
# environment (token, metricq_url, wait_for_rabbitmq_url) in addition to the
# METRICQ_* variables and command-line options of metricq-db-hta-s3.
set -eu

if [ -n "${token:-}" ] && [ -z "${METRICQ_TOKEN:-}" ]; then
  export METRICQ_TOKEN="$token"
fi
if [ -n "${metricq_url:-}" ] && [ -z "${METRICQ_SERVER:-}" ]; then
  export METRICQ_SERVER="$metricq_url"
fi

# Optionally wait for RabbitMQ (host:port) before connecting; the database
# exits if the first connection fails. WAITFORIT_TIMEOUT=0 waits forever.
if [ -n "${wait_for_rabbitmq_url:-}" ]; then
  host="${wait_for_rabbitmq_url%:*}"
  port="${wait_for_rabbitmq_url##*:}"
  timeout="${WAITFORIT_TIMEOUT:-60}"
  waited=0
  until (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    if [ "$timeout" -gt 0 ] && [ "$waited" -ge "$timeout" ]; then
      echo "timed out waiting for $wait_for_rabbitmq_url" >&2
      exit 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
fi

exec /usr/local/bin/metricq-db-hta-s3 "$@"
