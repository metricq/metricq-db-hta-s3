# Storage layout

All keys live below the configured bucket prefix. Every object except
`manifest` is written once (create-only) and never modified; state changes by
writing new objects and conditionally replacing `manifest`.

| Key | Content | Written by |
| --- | --- | --- |
| `manifest` | Root of the current state: WAL sequence covered, generation, references to all metadata roots, catalog, trash journal and a pending compaction job. About 1 KB. | every publication (conditional PUT) |
| `data/<id>` | Data blocks of one checkpoint, several streams concatenated | checkpoint |
| `data/compact-<job>/…` | Data blocks rewritten by compaction; `…/locality/…` for level-local packs | compaction |
| `index/<id>`, `index/compact-<job>` | Pages of the per-stream index trees | checkpoint, compaction |
| `state/<id>` | Pages of the open HTA state (per metric), hash-partitioned | checkpoint |
| `roots/<id>` | Pages of index roots per metric and level, hash-partitioned | checkpoint, compaction |
| `held/<id>` | Held records persisted by one checkpoint ([write path](write-path.md#holding-streams)) | checkpoint |
| `held-state/<id>` | Versioned root and independent paged trees of needed held deltas and per-stream watermarks, packed together | checkpoint |
| `catalog/…`, `candidates/…` | Pages of the maintenance catalog and the compaction candidate tree | checkpoint, compaction |
| `trash/<id>` | Pages of the deletion journal | checkpoint, compaction, GC |
| `jobs/<id>` | Description of a reserved or aborted compaction job | compaction |

## Blocks

A **block** holds up to 1024 records of one stream in time order, encoded
with a versioned binary codec and compressed with zstd, and is addressed by
`(object key, offset, length, SHA-256)`. It is the unit of reading: one block
is one byte range of one object, verified by its hash. Several blocks are
concatenated into one object ("pack") so a checkpoint needs few PUTs.

## Index

Each stream has a copy-on-write B-tree with fan-out 64. Leaf entries describe
blocks (`first` and `last` time, record count, block address and the
aggregate of the block's records); inner entries describe child pages with
the sum of their entries' aggregates. A raw value counts from the stream's
previous value, so a raw entry's active time is its `last` time minus that
previous time, and the aggregates of adjacent entries add up exactly to the
aggregate of their combined values. Checkpoints compute them from the
records they write; compaction keeps them for copied blocks and recomputes
them for merged and rechunked ones. Index pages are themselves blocks in `index/` objects.
Appending to a stream rewrites only its rightmost path; old roots stay valid,
so concurrent readers keep a consistent view.

## Data/index block codec

New data, index and root metadata pages begin with a six-byte envelope: ASCII
`MQHB`, version byte `1`, and kind byte (`8` and `9` data, `6` index, `3` root
page; kinds `1`, `7` and `2` were data blocks of fixed 80-byte records, gzip
data blocks and index pages without aggregates, no longer read). Data blocks
follow with one zstd frame (with checksum), other pages with an independent
gzip BestSpeed stream. The range SHA-256 covers the envelope and compressed
stream. Unknown versions/kinds, wrong destination types, truncation, invalid
counts, invalid key IDs, checksum errors and trailing payload bytes fail
decoding.

Data blocks are compressed in two stages. A checkpoint writes them with zstd
"fastest" (kind 8), which costs about as much as gzip BestSpeed. Compaction
writes every data block it produces with zstd "best" (kind 9), including
blocks it would otherwise copy unchanged, so each block is recompressed once
and then copied byte for byte. Strong compression costs only encoding time
(about 0.5 ms per raw and 2.5 ms per aggregate block of 1024 records): a kind
9 block decodes about twice as fast as a gzip block and is 15 to 25 % smaller.
The decoded records of a recompressed block stay in the block cache.

Every object type has exactly one format. Data blocks, index, root and state
pages, held deltas and WAL frames exist only in the binary formats described
here; Gob-encoded objects of these types fail decoding. A database written by
a version before these formats cannot be opened; start it with an empty
prefix and WAL.

All fixed fields use little-endian order. Data payloads start with a uvarint
record count, bounded to 1024, and the varint level shared by all records of
the block (one stream). Each record starts with its time as a varint delta
from the previous record (the first from zero). A raw record adds its value
(8 bytes); it always has repeat 1 and no aggregate. An aggregate record adds
uvarint Repeat, Minimum, Maximum and Sum (8 bytes each), uvarint Count,
Integral (8 bytes) and varint ActiveTime; its value is always zero. The
encoder rejects records outside this shape instead of dropping fields. A raw
value thus takes 9-18 bytes before compression instead of 80 for all ten
fields: compressed raw blocks are about 40 % smaller and decode about 2.5
times faster. Integers retain their full 64-bit range; floating fields retain
IEEE-754 bits exactly, including signed zero.

Index payloads contain a one-byte leaf flag, `uint16` entry count and `uint16`
key count, each bounded by fan-out 64. Each dictionary key has a `uint16` byte
length followed by its original bytes. Keys are shared within a page. Each
118-byte entry contains First and Last (`int64`), key ID (`uint16`), Offset and
Length (`int64`), SHA-256 (32 bytes), Records (`uint32`), and the aggregate:
Minimum, Maximum, Sum (IEEE-754 bits), Count (`uint64`), Integral (IEEE-754
bits) and ActiveTime (`int64`). Keys are bounded to
65535 bytes, and total decompressed size is bounded before allocation grows.
Existing index ordering/reference validation remains in its callers.

Root pages contain a `uint32` metric count and a `uint32` dictionary-key count,
then sorted dictionary keys (`uint16` byte length plus bytes). Each sorted
metric has a `uint16` name length, name bytes and a `uint16` level count. Each
level has a 60-byte entry: level (`int64`), dictionary key ID (`uint32`),
offset and length (`int64` each), and SHA-256 (32 bytes). Decoded pages are
bounded to 32 MiB; the key dictionary avoids repeating pack names across
metrics and levels.

Held deltas and checkpoint state pages use the same envelope (kinds 4 and 5)
with varint time deltas and counts: a held raw record stores only its time
delta and value, aggregate records their repeat and six fields; a state page
stores per metric its configuration, first and last sample and the open
interval of each level. Fixed 80-byte records would have been larger than
Gob, which omits zero fields. Both together had cost about a quarter of the
engine CPU (see `measurements/metadata-codecs.md`). The manifest, catalog and candidate pages, object
inventories, held inventory pages, compaction jobs and the trash journal
retain their gzip/Gob representation; they are small or rarely written. WAL frames use their own uncompressed binary
format (magic `MQHW`: receive time, aggregation config, metric name, points
with varint time deltas); gob plus gzip cost about 50 µs per frame, which
limited single-sample deliveries to a few thousand per second. Manifest CAS publication and WAL fsync before
acknowledgement are unchanged by the block codec.

## Paged metadata

The manifest does not contain per-metric data. Open HTA state and index roots
are stored in 256 hash-partitioned pages each (`state/`, `roots/`), addressed
through a directory. A change writes one pack with the changed pages and a new
directory. This bounds PUT count per changed metadata kind; the bytes written
still depend on how many pages change and how much state those pages contain.
With input on every metric, most Series pages can change at every checkpoint.

### Held metadata

`HeldState` addresses a version-1 root with two checksummed references:
`Inventory` and `Watermarks`. Each is a copy-on-write ordered tree with at most
64 entries or children per page. The inventory orders descriptors by checkpoint
generation and ordinal; WAL sequence alone is insufficient because an
age-triggered checkpoint can publish without accepting new samples. Watermarks
use the canonical metric/level stream key and contain absolute written-prefix
timestamps, including timestamp zero as a valid value.

An inventory-only change reuses the complete watermark tree. A localized change
writes affected leaves and ancestor paths, not the full collection. Changed
pages from both collections and the small root share immutable packs capped at
4 MiB; a publication normally needs one `held-state/` PUT. Packed sibling pages
are read together using bounded, checksummed range reads during startup. All
metadata is hydrated before WAL replay; queries need no additional lookups.

Retirement considers the union of both trees and the root. A pack containing an
obsolete inventory page remains live while an unchanged watermark page still
references it. GC deletes it after its last reference disappears and generation
pins permit deletion. This can retain dead bytes inside a partially live pack;
there is no automatic metadata-pack repacking yet.

The reader accepts the previous monolithic `Deltas`/`Watermarks` object and
transitions it on the next held-state change. Unknown versions, invalid page
ordering, missing pages and checksum failures stop startup. All immutable
metadata must be uploaded before the single manifest CAS; only a successful or
exactly reconciled publication permits reclaiming its WAL prefix. Failed
ordinary checkpoints can still leave unreachable output objects, as before.

Series dirty tracking freezes with the WAL sequence. Concurrent samples and new
configurations accumulate in the next set; a failed checkpoint restores the
frozen set. No per-Series patch journal is part of this format.

## Catalog

The catalog is a copy-on-write tree of all live objects with the blocks they
contain (metric, level, address, record count). It tells compaction and GC
which bytes of an object are still referenced. The candidate tree indexes
objects worth compacting: partly dead objects and objects with small blocks.

### Paged object inventories

An object with more than 128 live data/index descriptors stores them in immutable
inventory pages referenced by its catalog entry. The entry retains physical
size, live bytes, timestamps and page bounds/counts; its on-wire `Blocks` field
is empty. Older inline entries remain readable and are paged when updated.
Page bounds use offsets in the owning immutable object. Deleting descriptors
preserves those bounds, so later pages do not shift and need no rewriting.
Partial catalog bootstrap can add newly discovered descriptors.

A changed object's inventory pages share their own pack (4 MiB target, up to
32 MiB for a single page). Packs are not shared between different objects;
retirement can check all remaining page references of that one object without a
catalog-wide reachability scan. Missing, corrupted or invalid inventories fail
the catalog read; complete inventories are hydrated into the bounded decoded
catalog cache. They are maintenance metadata and add no history-query lookups.
Inventory preparation and staging registration are serial. Up to four immutable
packs upload concurrently, overlapping PUT latency with preparation of the next
object. All inventory uploads must succeed before the catalog root is written
and the manifest is published. An error cancels and joins the upload workers;
no failed request is retried and no new inventory reference is published.
Compaction output
uses its registered `catalog/compact-<job>/` namespace for failure cleanup.

## Deletion journal

Objects are never deleted directly. A publication that retires objects appends
their keys to the trash journal, a linked list of `trash/` pages. Garbage
collection deletes journal entries once no running query can reference them
(generation pins) and records its progress in the manifest.
