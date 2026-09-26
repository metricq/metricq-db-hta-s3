# Storage v2

Storage v2 is the only object format supported by this Go database. There is
no reader or migration path for the earlier development format.

The root `manifest` contains a version, committed WAL sequence, open HTA
series state, and one index root per canonical metric name and HTA level.
Level zero holds all accepted raw samples. Higher levels hold completed
aggregates, including compressed runs of identical empty intervals. The
incoming MetricQ `input` binding is not a storage identity. The root is
bounded by the number of metrics and levels rather than the age of the data.

At a checkpoint, pending records are grouped by canonical metric and level,
then split into blocks of at most 1,024 records. Each block is independently
gzip-compressed and SHA-256-checked.
The blocks are concatenated into an immutable `data/` object. Each block's
index entry records its time bounds, physical record count, object key, byte
offset, byte length, and hash. The index is a copy-on-write B-tree with a fanout of
64. Its new pages are independently compressed and checked blocks packed into
an immutable `index/` object. Only the rightmost path for each changed stream
is rewritten once per checkpoint batch; intermediate versions never enter
the index pack. Older pages and root pointers stay valid for concurrent reads.

For aggregate levels, the last partially filled block is read and combined
with the next checkpoint's records until it reaches 1,024 physical records.
The new block and its replacement index path are immutable additions; the
manifest switches to them only after both packs have been uploaded. Thus a
low-volume aggregate level does not accumulate one tiny block per flush.
Each changed aggregate stream reads at most one previous data block, plus
its rightmost index path. Raw blocks are appended without tail rewriting.
An aggregate run counts as one physical record, regardless of its duration.

The commit order is data pack, index pack, conditional root manifest PUT,
local WAL checkpoint, and WAL truncation. A failed tail read or PUT leaves
the previous published root and WAL intact.
If a manifest PUT response is lost, the engine reads the manifest back and
reclaims WAL only when it matches the exact proposed checkpoint. Objects
written before a failed root publication may be orphaned and are not yet
discovered automatically. Fully retired published packs are reclaimed as
described below. Objects referenced by the current root or active queries
must not be deleted.

Queries traverse only index pages whose time bounds intersect the requested
window, plus neighboring blocks for boundary semantics. The S3 backend uses
byte-range GETs for individual index and data blocks and verifies each block
hash. Bounded shared caches keep immutable index pages and up to 128 MiB of
decoded data blocks. A stream reads at most eight missing data blocks in
parallel; aggregate queries can also read independent HTA levels concurrently.
Cache hits still count toward the per-query decoded-data budget. A backend
without range support can use full-object GET as a functional
fallback, but must implement `storage.Store`. Query cost grows with the
selected-level blocks in the answer and the logarithmic index depth, not
with the age of the requested interval. Raw and aggregate streams are indexed
separately, so an aggregate timeline does not fetch raw data blocks when its
chosen aggregate level has records.

`FLEX_TIMELINE` uses raw records when requested `interval_max` is below the
metric's configured `interval_min`. Otherwise it selects the largest
available aggregate level within the request limit. Sparse sample count does
not change this choice. The client can derive `interval_max` from the query
span and display width.

This format keeps raw and completed aggregate records indefinitely, subject
to the durability of the WAL and object store. Tail consolidation trades
bounded reads and rewrites during ingestion for fewer historical query GETs.
Superseded tail versions remain inside older packs; there is no object
reclamation, general historical compaction, or sharded manifest yet.
Previously fragmented history is not automatically repacked. Entries without
a physical record count are left intact. Checkpoint frequency still determines
raw block sizes and object count. The root remains bounded, but its open HTA
state still scales with the number of configured metrics and levels. A
1,500-metric, ten-year deployment needs workload benchmarks for object-store
request rate, latency, and cost before production use.

## Deleting completely retired objects

S3 now implements the optional `storage.Deleter` contract. The engine counts
references to individual blocks per data/index pack. A checkpoint adjusts
these counts for its newly written blocks and replaced aggregate tails and
index pages; it does not scan historical trees on each flush. A mixed pack
is retained until its last reachable block is replaced. Permanently retained
raw blocks therefore also keep their containing packs alive. This mechanism
reclaims whole dead packs; it does not compact partially dead packs.

Fully retired keys are included in a durable `Garbage` queue in the newly
published manifest. DELETE occurs only after that manifest is confirmed and
the local WAL checkpoint succeeds. An uncertain manifest PUT cannot cause
premature deletion. Queries in the writer process pin snapshots; deletion is
postponed while any query is active. The ingestion mutex excludes new readers
during DELETE. Historical manifests are not retained as independently usable
snapshots; separate reader processes are not covered by this in-process pin.

A pass attempts at most 16 objects within two seconds, under the ingestion
mutex. A DELETE error stops that pass and leaves the queue intact; it does
not turn a successfully published checkpoint into a failure. S3 sends each
DELETE once (the SDK has no retries). `RunFlush` processes remaining work on
subsequent ticks even without new samples. Successful deletions disappear
from the next checkpoint's queue; after a crash, repeating an already completed
DELETE is safe because deletion is idempotent. With bucket versioning enabled,
ordinary DELETE creates a delete marker rather than removing past versions;
provider lifecycle rules must reclaim those versions separately.

Startup reconstructs the in-memory per-object counts by walking the current
index trees once, without reading data blocks. This adds index reads and startup
time proportional to the published index, and memory proportional to live
object count. No object-count map is added to the manifest. Stores without
`Deleter` skip this mechanism. Packs orphaned before manifest publication,
and dead packs from before this implementation, are not discovered by it;
reclaiming those requires a separate namespace inventory/reachability pass.

Prometheus exposes `metricq_db_gc_pending_objects`,
`metricq_db_gc_deleted_objects_total`, and `metricq_db_gc_delete_errors_total`.
Pending objects include those protected by active queries. Long-running or
continuously overlapping queries can delay collection.
