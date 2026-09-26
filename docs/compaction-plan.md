# Background compaction

The [query/ingestion review](compaction-review.md) records the original findings.
[Implemented fixes and measurements](compaction-optimizations.md) describe stream
selection, index contraction, cache preservation and append-only aggregate flushes.

Implemented as one maintenance goroutine inside the DB process. It has its own
schedule and limits; neither ingestion nor a history request triggers a job.
`RunMaintenance` handles compaction, abandoned-job recovery and garbage collection.
The production executable starts it alongside `RunFlush`, cancels both at shutdown,
waits for them, and then performs the final checkpoint. Library users must enable
`BackgroundMaintenance` and run `RunMaintenance` themselves.

## Configuration

The local `engine.compaction` configuration is shown in
[local.example.json](../configs/local.example.json). Executable defaults:

| Setting | Default | Meaning |
|---|---:|---|
| enabled | true | Run compaction; disabling it leaves GC/recovery enabled |
| interval_seconds | 60 | Compaction scheduling interval |
| cooldown_seconds | 60 | Avoid recently ingested candidate objects |
| max_duration_seconds | 60 | Context deadline per job |
| max_cycle_seconds | 30 | Window for starting consecutive jobs (library default: 10) |
| max_job_bytes | 33554432 | Input selection / index output budget |
| max_blocks | 512 | Maximum selected source blocks (library default: 128) |
| object_bytes | 4194304 | Target output pack size |
| bytes_per_second | 8388608 | Shared preparation/publication read and write byte rate |
| dead_fraction | 0.4 | Dead-byte fraction qualifying a partly live object |
| merge_small_blocks | true | Merge adjacent small data blocks at every HTA level, at most 1024 records |

Catalog, candidate, job and rebuilt-index bytes are included in the shared limiter.
The bounded final metadata rewrite is charged after releasing the publication
mutex, so byte pacing does not lengthen this critical section.
GC DELETE calls remain governed by their separate pass limits. Candidate scans
have bounded pages and a cursor. Preparation limits metadata reads to 32 MiB and
index traversal to 4096 operations. The catalog cache is bounded separately.
Catalog leaves target 256 KiB of estimated descriptors as well as the 64-object
fanout limit. An individually larger inventory gets its own leaf; the 32 MiB
encoded-page limit still applies.
These bound buffers and work, not total Go heap usage. Jobs are skipped while WAL
or pending-builder pressure is high. The worker starts multiple jobs within each
cycle and runs a GC pass between them. The cycle deadline controls new job starts;
an already running job can extend the cycle by its own duration limit. Shutdown
still cancels the running job. The worker is serial, so GC can wait behind one job.

## Publication and durability

The manifest has a generation separate from the committed WAL sequence, catalog
and candidate roots, one pending job pointer and bounded trash-journal pointers.
The catalog is a persistent B-tree recording live block descriptors per object;
normal checkpoints update only affected paths. Candidate keys prioritize dead
space; dense packs with eligible small blocks at every HTA level can also be
consolidated. Selection expands seeds through bounded neighboring index reads in
both time directions. A resumable cursor within each candidate object avoids spending the budget on
unrelated single blocks. Compaction preserves ingestion-age cooldown timestamps.
Merge seeds have at most 512 records; a larger neighbor can still participate if
the combination fits in 1024. Dirty mixed packs larger than one job are evacuated
over several jobs, retaining their source key until the last live block moves.
This requires temporary storage headroom.
Within candidate objects, merge selection precedes copy-only evacuation, preventing
dead-space thresholds from repeatedly relocating still-fragmented streams.

1. Select bounded inputs from the committed snapshot and pin its generation.
2. Durably register the job and its unique output prefixes before uploading.
3. Copy and checksum live encoded blocks into new immutable packs with at most eight
   source reads in flight. Optional data-block
   merging decodes adjacent blocks and preserves their records without aggregating.
4. Collapse redundant index roots and combine bounded sparse sibling leaves. Build
   replacement historical index paths and catalog updates on a private
   snapshot outside the ingestion lock.
5. After data copying, acquire the publication mutex, validate exact source
   identities against the newest committed state, and build the bounded metadata
   rewrite. Flushes wait for this phase; WAL appends and query admission continue.
   A flush that changed a selected source block can still invalidate the job.
   Merely publishing a newer manifest does not discard already copied data.
6. Publish with conditional manifest PUT. A separate publication mutex serializes
   root publication with flushes; maintenance releases the ingestion mutex during
   the PUT. The committed WAL sequence and HTA state do not advance.
7. Add fully retired objects and replaced metadata to the durable trash journal.
   Release the source-generation pin; GC deletes once older readers release theirs.

Old packs stay reachable until publication succeeds. Copy-only compaction retains
encoded block bytes and SHA-256 checksums. Raw and aggregate records remain stored
indefinitely. A maintenance publication cannot accidentally checkpoint newer ACKed
samples: live and committed HTA/WAL state are kept separately.

## Recovery and deletion

S3 errors stop the attempt without deleting the current source objects. Calls make
one SDK attempt and always send unquoted If-Match ETags. An uncertain manifest PUT
is reconciled by reading back the exact proposed bytes. A conflicting publisher
fences this writer. Unsuccessful maintenance does not discard WAL data.

A crash leaves the registered job recoverable. Recovery durably aborts it, waits a
60-second cleanup grace, inventories its registered output prefixes in bounded
pages, and journals abandoned objects for deletion. Successful jobs journal
superseded objects and metadata as part of their publication.

Trash entries carry retirement generations. Query pins protect snapshots that
could reference them; later queries do not globally block old garbage. GC performs
GET and DELETE outside the ingestion mutex. A pass deletes at most 16 entries
within two seconds, then persists its progress. A durable cleanup field retains
journal pages whose own deletion failed. Already authorized deletion can happen
before a metadata PUT, allowing GC to free space when quota blocks new writes.
A crash between deletion and progress publication repeats an idempotent DELETE.

There is no automatic discovery of arbitrary normal-flush uploads orphaned before
manifest publication, nor of objects predating tracking. General namespace orphan
inventory remains separate work. Versioned buckets require lifecycle rules to
actually reclaim old versions. External reader processes are not covered by the
in-process generation pins.

The executable defaults to `engine.append_only_aggregates=true`: flush writes only
new completed aggregates and the worker consolidates their immutable fragments.
Setting it to false restores tail extension during flush. The executable disables
append-only mode when compaction or block merging is disabled. Library callers
must explicitly enable compatible background maintenance.

Catalog fanout is 64, and immutable page PUTs upload in batches of at most eight
with a 4 MiB queued-byte target. Pages remain separate objects for safe reclamation.
Compaction adds its own reads/writes and CPU work. Under sustained overload, old
fragment backlog can still grow; tune limits against the combined ingest/query workload.

## Introspection and verification

Prometheus exports compaction activity, duration, completions, errors, conflicts,
read/write bytes, live/dead object bytes and pending/deleted GC objects. Additional counters expose input/replacement blocks and no-action scans; gauges
expose candidate objects and small data blocks/bytes (including single tails). These have
no labels per metric, object or job. Live/dead byte gauges describe catalog-tracked
data/index objects, not the entire bucket including metadata and orphan uploads.

Tests cover copying and merging, exact catalog accounting, concurrent flush rebasing,
stale-source rejection, generation protection, restart cleanup, lost PUT replies,
publication failure, quota recovery, WAL preservation and worker cancellation.
A blocked maintenance manifest PUT test verifies that ingestion can still ACK.
Real-S3 integration checks storage reclamation and identical query responses after
a fresh S3-only restart. Legacy parity runs before and after compaction and after
restart, plus the executable's Prometheus endpoint and shutdown.

Run unit/race tests with `go test -race ./...`. With the development services and
`compose.test.yml` running, use `go test -tags=integration ./integration`.
[compaction-results.csv](compaction-results.csv) records the real-S3 bounded-pass
experiment: 128 samples per metric delivered in eight flushes. Baseline GC runs
before compaction, so its storage benefit is reported separately. The experiment
uses 12 passes for 6/150 metrics and 64 for 1500 metrics; it does not assert complete
quiescence. Wall times are single-run observations, not a latency benchmark. These storage
experiments instantiate the engine directly and do not run the Prometheus HTTP
endpoint or scrape it. The executable endpoint is checked separately.

Observed S3 results (decimal MB, after baseline GC):

| Metrics | Samples | Data/index before → after | Raw blocks before → after |
|---:|---:|---:|---:|
| 6 | 768 | 0.097 → 0.026 MB | 48 → 6 |
| 150 | 19200 | 2.502 → 1.015 MB | 1200 → 502 |
| 1500 | 192000 | 25.698 → 21.977 MB | 12000 → 11800 |

All raw records remained reachable, with 24 query-response comparisons after an
S3-only restart for each size. The 1500-metric run shows limited raw consolidation
in these passes: reclaiming dead packs has priority. It is not evidence that all
fragmentation has been eliminated or that ten-year storage performance is solved.
