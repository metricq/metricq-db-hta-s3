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

The production executable writes only newly completed aggregate records on flush.
Existing partial aggregate blocks remain immutable and independently queryable.
Background compaction merges neighboring blocks at every HTA level, up to 1024
physical records, without reaggregating or expanding compressed empty runs.
`engine.append_only_aggregates=false` restores the older tail-extension behavior:
flush reads and fills the last partially filled aggregate block. Raw blocks are
always appended without tail rewriting.

A checkpoint first freezes its records and renames the active WAL segment,
under the ingestion lock. The commit order is then data pack, index pack,
conditional root manifest PUT, local WAL checkpoint, and deletion of the
published segments. Uploads run outside the ingestion lock. A failed tail read
or PUT leaves the previous published root and every WAL segment intact.
If a manifest PUT response is lost, the engine reads the manifest back and
reclaims WAL only when it matches the exact proposed checkpoint. Objects
written before a failed root publication may be orphaned and are not yet
discovered automatically. Fully retired published packs are reclaimed as
described below. Objects referenced by the current root or active queries
must not be deleted.

Queries traverse only index pages whose time bounds intersect the requested
window. Raw queries also read boundary neighbors; aggregate queries include the
bucket containing the start without fetching extra neighbors. The S3 backend
coalesces missing data blocks in the same object, bridging at most 64 KiB gaps
with an 8 MiB range target, and verifies each block hash separately. Blocks larger
than that target use individual reads. Bounded shared caches keep immutable index
pages and up to 128 MiB of decoded data blocks. A stream processes batches of at
most 128 block references with eight range requests in parallel; aggregate queries
can also read independent HTA levels concurrently.
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
to the durability of the WAL and object store. Background consolidation trades maintenance reads/writes for fewer historical
query GETs while keeping flush work proportional to newly completed records.
Superseded tail versions are reclaimed by the background compactor. Checkpoint
frequency still determines initial raw block sizes; compaction can merge adjacent
small data blocks at every HTA level without dropping samples. The root remains bounded by metric and
level count, although its open HTA state still scales with those counts.

## Catalog, compaction and object reclamation

The production DB starts a maintenance goroutine independently of ingestion and
history requests. A persistent copy-on-write catalog records each live block's
object, offset, length, hash, metric and level. A separate candidate tree indexes
partly dead packs and eligible small data blocks at every HTA level. The manifest contains only their
root pointers and bounded maintenance state; it does not contain every object.
Normal checkpoints update touched catalog paths. Existing namespaces without a
catalog bootstrap it once from index trees; subsequent starts use persisted roots.

Compaction selects adjacent blocks through each metric/level time index, copies
live blocks into immutable packs, contracts sparse index paths and replaces the affected
historical index paths. Optional data-block merging preserves every record and
verifies adjacency through the index. Work prepares outside the ingestion lock.
After copying, the bounded final metadata rewrite serializes with flush publication,
validates source identities against the current committed roots and uses conditional
manifest PUT. Ingest retains access to its mutex during this metadata I/O. The
publication retains the committed WAL sequence and HTA
state, so samples ACKed during preparation remain in the WAL until normal flush.

A durable linked trash journal authorizes deletion only after safe publication.
Queries pin their manifest generation; a retired object remains protected while
an older snapshot could reference it. Newer queries do not delay older garbage.
GC reads journal pages and issues DELETE outside the ingestion lock: up to 256
entries from up to 64 pages per pass, eight requests in flight, within a two-second
deletion budget, followed by one manifest publication. It can release space before
writing its progress, which helps recover from a full object-store quota. Durable
page offsets and a list of finished journal pages make interrupted deletion
restartable. Repeating an already completed DELETE is safe.

Registered compaction output namespaces are inventoried and reclaimed after an
abandoned job is fenced. This does not discover arbitrary objects orphaned by
normal failed flushes before publication or by older untracked implementations.
Those require a separate inventory/reachability procedure. Bucket versioning also
requires provider lifecycle rules to remove old versions after DELETE.

See [compaction design and operation](compaction-plan.md) for limits, recovery,
configuration and test coverage. Separate reader processes are not protected by
in-process generation pins; the namespace remains single-writer.
