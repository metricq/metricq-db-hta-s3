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
written before a failed root publication may be orphaned; there is currently
no automatic garbage collector. Objects referenced by any published root
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
