# Requirements

## Object storage (S3)

The database uses the S3 API through the AWS SDK and works with AWS S3, Ceph
RGW, MinIO and RustFS. The backend must provide:

| Requirement | Used for |
| --- | --- |
| Strong read-after-write consistency for GET, PUT and LIST | reading a just-published manifest, recovery |
| Conditional `PutObject` with `If-None-Match: *` | create-only writes of immutable objects |
| Conditional `PutObject` with `If-Match: <etag>` | the atomic manifest commit (compare-and-swap) |
| `GetObject` with `Range` | reading single blocks and coalesced ranges |
| `DeleteObject` | garbage collection |
| `ListObjectsV2` with prefix | cleaning up aborted compaction jobs |
| `HeadObject` | size checks during catalog bootstrap |

Check an endpoint before use with the bundled probe (it creates the bucket if
needed and writes and deletes one test object):

```sh
S3_CHECK_URL=https://s3.example.org/metricq S3_CHECK_KEY=... S3_CHECK_SECRET=... \
  go run ./cmd/check-ceph-s3
```

Further requirements:

- **Exclusive prefix.** One database per bucket prefix. Several databases may
  share a bucket with different prefixes. Nothing else may write below the
  prefix.
- **Versioning.** If bucket versioning is enabled, configure a lifecycle rule
  that expires noncurrent versions; otherwise deleted objects keep using space.
- **Credentials** via the standard AWS SDK chain (`AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY`, shared credentials file, instance roles). The
  database needs `GetObject`, `PutObject`, `DeleteObject` and `ListBucket` on
  the prefix; it does not create buckets.
- **Latency.** Checkpoints and compaction issue sequential metadata requests;
  tens of milliseconds per request are fine, but each request's latency adds to
  checkpoint duration. Queries issue one range GET per non-adjacent byte range.

## Local storage for the WAL

- A local file system with working `fsync` (ext4, xfs). **Not tmpfs**: data
  acknowledged but not yet checkpointed would be lost on reboot; tmpfs also hides
  the real fsync cost.
- Capacity: at least `wal_hard_bytes` (default 80 MiB) plus headroom for one
  frozen segment; see [Sizing](sizing.md).
- fsync latency directly limits batches per second; SSDs are recommended.
- The WAL is bound to the bucket prefix. Do not copy a WAL directory to another
  database or reuse it after pointing the database at a different prefix.

## Compute and network

- One core handles ingestion (the AMQP consumer is single-threaded); checkpoints
  and compaction encode and decode on all cores.
- Memory is dominated by held records, caches and query budgets; see
  [Sizing](sizing.md#memory).
- AMQP connection to the MetricQ RabbitMQ; the manager must know the database
  token (configuration document in CouchDB).
- Linux; the executable is statically linked (`CGO_ENABLED=0`).
