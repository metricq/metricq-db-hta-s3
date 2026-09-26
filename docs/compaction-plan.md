# Plan: standalone online compaction

Status: proposed implementation plan. No compactor is implemented yet.

## Objective and boundaries

Add `metricq-db-hta-compact`, running independently of ingestion and history
requests, with its own schedule, memory limit, I/O budget and Prometheus
endpoint. It can pause or crash while the database continues operating.
It retains every accepted raw sample and every aggregate, regardless of age.
The database remains the only publisher of the namespace's root manifest.

Compaction has two distinct stages:

1. **Pack reclamation:** copy still-referenced compressed blocks into new packs,
   without decoding or recompressing them. This reclaims dead byte ranges while
   retaining the existing query block boundaries and checksums.
2. **Block consolidation:** optionally combine adjacent small blocks belonging
   to the same canonical metric and HTA level, up to the block-size limit.
   Decode/encode records without recomputing aggregates or discarding samples.
   This can reduce GET counts for fragmented raw history; pack reclamation
   alone does not reduce the number of data blocks read by a query.

Implement and validate stage 1 first. Stage 2 uses the same publication and
recovery protocol. Compaction adds write traffic; it does not fix repeated
compression of a growing aggregate tail during ingestion.

## Current gaps

- `engine/gc.go` maintains only in-memory live block counts per object and
  reconstructs them by scanning the entire current index at startup.
- The current manifest embeds the deletion queue; an outage can grow it.
- GC waits for *all* queries to finish and performs DELETE under the engine
  mutex, from `Flush` or `RunFlush`.
- Index updates currently support append and tail replacement, not bounded
  replacement of arbitrary historical blocks.
- `Engine.state.Series` includes newer, WAL-backed live state. It must not be
  serialized as though it belonged to an older committed WAL sequence during
  a compaction-only publication.

## Process responsibilities

| Component | Responsibilities |
| --- | --- |
| DB ingestion | WAL, ACKs, HTA construction, normal data checkpoints |
| DB maintenance coordinator | Job admission, snapshot pins, validation, bounded index changes, manifest publication, deletion authorization |
| Separate compactor | Scheduling, copying/repacking, optional block consolidation, staging uploads, authorized deletion and cleanup |

Start with one active compactor job per namespace. Use a versioned maintenance
RPC over a permission-restricted Unix socket, separate from the public metrics
endpoint. The compactor accesses S3 directly through `storage.Store` and
`RangeGetter`; it never opens the database WAL. This first deployment places
both processes on the same host. A separately authenticated remote transport
can follow without changing the job protocol. When the DB coordinator is
unavailable, the compactor cannot acquire jobs, publish or obtain new deletion
authorizations; registered work remains recoverable.

A dedicated DB maintenance worker handles these RPCs. Neither a history request
nor an ingest call schedules or performs compaction. Root publication must
serialize with ordinary checkpoints, so zero contention is not promised.
Expensive data reads, compression and uploads happen in the compactor. Bound
metadata work per job and prioritize WAL-pressure checkpoints.

## Persistent metadata and candidate selection

Introduce a sharded copy-on-write object catalog and bounded job records,
referenced by roots in the manifest. The manifest must not gain a list of all
objects, blocks or historical jobs. Catalog pages and their own retirement
must participate in the same reachability and deletion protocol.

Extend the backend interfaces with object metadata/size lookup and paginated
prefix inventory for bootstrap and interrupted-job cleanup. Implement these
with the existing S3 SDK; keep their contracts suitable for a future file backend.

Each object entry records its physical size, live byte count, live block count,
creation/retirement generation, state and a reference to paged block descriptors.
Descriptors identify metric, level, time bounds, record count and blob address.
Checkpoint publication atomically commits object/index reference changes and
catalog changes. A single owner updates this metadata: the DB publisher.

Maintain an incremental candidate index from changed object entries. No full
bucket listing, all-history scan or raw block reads are required on each
compactor cycle. An explicit one-time, restartable bootstrap may scan the
existing index/object inventory to create the catalog. It must distinguish
unknown objects from proven garbage and must not delete unknown objects.

Candidate policy considers dead-byte fraction, reclaimable bytes versus copy
cost, object age and per-metric fairness. Batch small candidates so they can
also be reclaimed. Avoid repeatedly copying recently rewritten aggregate
tails; apply a cooldown. Separate stable blocks from frequently rewritten
tails in compactor output where practical, without creating an object per
metric. Keep metric/level/time locality and range-addressable blocks.

Start with configurable thresholds and preserve the current 4 MiB pack target;
benchmark alternative targets before changing that default. Enforce explicit
limits for job input/output bytes, metadata pages, resident buffers, concurrent
S3 requests and bytes/second. A fragmented object larger than a job budget can
be evacuated through multiple jobs; delete it only after its final reference
is gone. Keep all work schedulable within these bounds.

## Job and publication protocol

1. **Reserve.** The coordinator durably registers a job ID, attempt/fencing
   token, input descriptors and output namespace. It pins the source generation
   and grants a renewable lease. Registration precedes any output upload.
2. **Copy.** The compactor reads source ranges and checks hashes. It writes
   immutable packs under its job/attempt namespace with conditional creation.
   Unchanged compressed blocks retain their content checksums. It reports a
   bounded replacement proposal only after all target uploads are confirmed.
3. **Validate.** The DB checks the token, lease, target completeness and exact
   old-to-new mapping. Match old references by metric, level, time bounds,
   object, offset, length and hash. Never authorize replacement based on time
   bounds alone. The first version accepts a whole bounded job or rejects it;
   it does not partially publish a proposal.
4. **Merge with current history.** Add a general copy-on-write index operation
   that replaces these exact references in the *current* trees. Unrelated
   appends or changed root ETags do not invalidate the copied data. If an input
   block was itself replaced meanwhile, reject that job and schedule fresh
   work. Preserve all newer append/tail changes. Bound the number of changed
   paths/pages; build outside the ingestion mutex and validate/serialize the
   final publication with the checkpoint publisher. If rebuilding against a
   newer tree is needed, bound it and yield to pressured ingestion.
5. **Publish.** Atomically publish new data/index/catalog roots and committed
   job status with a conditional manifest PUT. Add a manifest generation
   independent of the committed WAL sequence. A compaction-only commit leaves
   that sequence, the committed HTA state and the WAL unchanged. Keep committed
   state separate from the newer live state used for ingestion. The publisher
   reconciles an uncertain PUT by reading the durable job/commit state; it
   must not guess or resubmit blindly. Continue using unquoted If-Match.
6. **Retire.** Update live references/bytes and retirement generations in the
   same commit. Release the compactor's source pin once its attempt is committed
   or fenced out. Old-query pins continue to protect source objects.
7. **Reclaim.** The DB marks a zero-reference object deletable only when no
   reader/job pin can need it. That durable state forbids reintroducing its
   references in any future job. The external process performs idempotent
   DELETE using this authorization, reports completion and removes catalog
   tombstones through a later metadata commit. No DELETE runs under the
   ingestion lock or as part of a query/ingest operation.

Use generation-based query pins rather than requiring a moment with zero
queries. New queries on the current generation must not indefinitely protect
objects retired before they started. A lease expiry fences a compactor attempt
before releasing its pin. A surviving old process cannot publish after a DB
restart: a new coordinator epoch invalidates its token, and it must acquire a
new job. Late uploads from fenced attempts can only become garbage.

The job ledger makes uploaded-but-unpublished objects identifiable. Cleanup
must account for uploads already in flight at fencing time: reconcile the
attempt's output namespace again after upload timeouts, rather than assuming
one inventory scan saw every object. Keep interrupted-job cleanup separate
from deleting retired published packs. Do not use age alone as proof of
unreachability. No automatic HTTP/S3 retries are added; failed jobs stop their
attempt, and later scheduled work starts from reconciled durable state.

## Failure and recovery requirements

| Failure | Required behavior |
| --- | --- |
| Source GET fails or hash mismatches | Stop the job; keep the original index and data; report error |
| Output PUT fails, quota is full or worker crashes | No publication; originals remain readable; reconcile registered outputs before cleanup |
| Worker dies after upload | Recover or abort its registered attempt; outputs remain tracked |
| Ingest flush overlaps the job | Preserve newer samples; merge unchanged input references or reject stale inputs |
| Manifest PUT response is lost | Inspect durable job status/current manifest; never delete based on an unconfirmed outcome |
| DB crashes before publication | Original published history plus WAL remain authoritative |
| DB crashes after publication | New history and retired-object metadata recover together; WAL replay starts at the unchanged committed sequence |
| DELETE fails or its response is lost | Retain cleanup work; repeated authorized deletion is safe; database remains usable |
| Worker resumes after lease expiry/restart | Reject stale token; never publish its late proposal |
| Query remains open across publication | It reads its pinned old blocks until it ends |

S3 versioned buckets need lifecycle management of noncurrent versions; ordinary
DELETE alone does not reclaim those physical versions. Compaction temporarily
needs free space for its outputs. Reserve an output budget/headroom before
starting and stop when it is exhausted; never free originals early to make room.

## Implementation commits

1. Separate committed/live state and publication generation; introduce durable
   job state and epoch/token validation. Add independent coordinator lifecycle.
2. Add sharded object catalog, live-byte accounting, candidate index and
   restartable bootstrap. Bound metadata/root growth and startup working set.
3. Add generation pins and durable deletion authorization. Move physical
   deletion from `Flush`/`RunFlush` into the external maintenance process.
4. Implement exact-reference historical index replacement and publication of
   bounded proposals while preserving concurrent ingestion and WAL state.
5. Add `cmd/metricq-db-hta-compact`: dry-run, one-job and daemon modes, budgets,
   metrics, copy-only pack reclamation, interruption recovery and cleanup.
6. Add optional small-block consolidation after copy-only reclamation passes
   correctness, failure and performance tests.

Develop each layer with tests; keep automatic scheduling disabled until the
complete failure matrix passes. No additional Go dependency is required for
this plan; use existing S3/Prometheus dependencies and the standard library.

## Tests and acceptance

- Compare complete raw timestamp/value sequences and aggregate records before
  and after compaction, after restart and after WAL replay. Copy-only blocks
  must retain their byte hashes. Consolidation must preserve record content,
  repeat runs and boundary behavior exactly.
- Reuse legacy request parity for LAST_VALUE, AGGREGATE, aggregate timelines
  and FLEX_TIMELINE across narrow/wide ranges and boundaries. Raw fallback
  continues to depend on interval_max, never a sample-count heuristic.
- Inject failures before/after every durable transition, including ambiguous
  PUT outcomes, failed DELETE, corruption, quotas, process kill, stale workers,
  duplicate proposals and simultaneous normal checkpoints.
- Hold real queries across publication/deletion; continuously issue newer
  queries to verify old generations are eventually reclaimable. Run the race
  detector and real S3 integration tests with separate DB/compactor processes.
- Test shared packs across metrics/levels, partially evacuated packs, multi-page
  indexes and source/target packs containing both live and dead blocks.
- Repeat the 6/150/1,500-metric workloads and the long-history latency suite.
  Record live/physical bytes, temporarily duplicated bytes, copy/read/write
  volume, GET/PUT/DELETE counts, memory, CPU, ingestion throughput and p50/p95/p99
  query/ACK latency with the compactor disabled and enabled at fixed budgets.
- At quiescence, all proven-deletable old packs and abandoned job outputs must
  be reclaimed. Pack reclamation must preserve index lookup complexity and
  block count; consolidation must not increase query block count. Under load,
  backlog and interference must remain observable; establish performance limits
  from the measurements rather than promising zero overhead.

Expose compactor/job counts by bounded state, candidate/dead/live bytes,
copy/reclaimed bytes, read/write/request budgets, active pins, oldest pending
job/retirement, publication conflicts, lease expiry, cleanup failures and
stage durations. Do not label metrics by metric name, object key or job ID.
