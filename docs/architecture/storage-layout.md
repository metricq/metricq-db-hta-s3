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
| `held-state/<id>` | List of needed held deltas and per-stream watermarks | checkpoint |
| `catalog/…`, `candidates/…` | Pages of the maintenance catalog and the compaction candidate tree | checkpoint, compaction |
| `trash/<id>` | Pages of the deletion journal | checkpoint, compaction, GC |
| `jobs/<id>` | Description of a reserved or aborted compaction job | compaction |

## Blocks

A **block** holds up to 1024 records of one stream in time order, encoded
with `encoding/gob` and compressed with gzip, and is addressed by
`(object key, offset, length, SHA-256)`. It is the unit of reading: one block
is one byte range of one object, verified by its hash. Several blocks are
concatenated into one object ("pack") so a checkpoint needs few PUTs.

## Index

Each stream has a copy-on-write B-tree with fan-out 64. Leaf entries describe
blocks (`first` and `last` time, record count, block address); inner entries
describe child pages. Index pages are themselves blocks in `index/` objects.
Appending to a stream rewrites only its rightmost path; old roots stay valid,
so concurrent readers keep a consistent view.

## Paged metadata

The manifest does not contain per-metric data. Open HTA state and index roots
are stored in 256 hash-partitioned pages each (`state/`, `roots/`), addressed
through a directory. A change writes one pack with the changed pages and a new
directory. This bounds PUT count per changed metadata kind; the bytes written
still depend on how many pages change and how much state those pages contain.
With input on every metric, most Series pages can change at every checkpoint.

## Catalog

The catalog is a copy-on-write tree of all live objects with the blocks they
contain (metric, level, address, record count). It tells compaction and GC
which bytes of an object are still referenced. The candidate tree indexes
objects worth compacting: partly dead objects and objects with small blocks.

## Deletion journal

Objects are never deleted directly. A publication that retires objects appends
their keys to the trash journal, a linked list of `trash/` pages. Garbage
collection deletes journal entries once no running query can reference them
(generation pins) and records its progress in the manifest.
