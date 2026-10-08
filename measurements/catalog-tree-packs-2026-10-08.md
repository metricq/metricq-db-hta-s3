# Catalog tree packs, 2026-10-08

The catalog and candidate trees wrote every page as its own object, and
leaves were sized by the hydrated descriptors of paged objects, so almost
every large object had a leaf of its own: 573 catalog pages (1.9 KB on
average) for 675 objects. A catalog update rewrote one object per page on its
path. Over ten minutes, 73 % of all PUT and DELETE requests of the
development database were catalog and candidate pages.

Changes:

- Leaves count the inventory references of paged entries, not their
  descriptors, and aim at 64 KiB before compression.
- All tree pages of one update share one pack. An update retires the packs
  that only the old tree referenced; once a tree references 16 packs, the next
  update rewrites the whole tree into a fresh pack.
- Tree pages are read and cached without loading inventories; an object's
  inventory is loaded only when that object is looked up or visited, with its
  own cache of decoded inventory pages. With full leaves, loading all
  inventories of a leaf on every read had raised catalog range reads from 104
  to 67,398 in ten minutes (2.25 GB).

Store requests of the development database over ten minutes each, at
comparable load (1100 samples/s, checkpoints, compaction), from the
`metricq_db_store_requests_total` and `metricq_db_store_bytes_total`
counters:

| | before | after |
| --- | ---: | ---: |
| manifest publications | 92 | 84 |
| checkpoints and jobs writing index pages / compaction jobs | 35 / 17 | 29 / 14 |
| catalog PUT / DELETE | 595 / 594 | 68 / 61 |
| candidates PUT / DELETE | 377 / 379 | 29 / 36 |
| catalog and candidate PUTs per index-writing publication | 27.8 | 3.3 |
| all PUT / DELETE | 1307 / 1342 | 374 / 278 |
| catalog and candidate share of PUT and DELETE | 73 % | 30 % |
| catalog range reads | 104 (2.1 MB) | 40 (0.4 MB) |
| catalog and candidate bytes written | 3.9 MB | 4.5 MB |

The remaining catalog PUTs are mostly inventory packs of changed objects,
one per object as before. The first update after the deployment rebuilt both
trees: catalog 573 pages in 573 objects became 38 pages in one pack,
candidates 62 pages became 12; in steady state the trees use 4-16 packs.
A bucket audit (`engine/bucket_audit_review_test.go`) afterwards found no
orphaned and no missing objects and identical block sets in catalog and
index; the bucket held 2136 instead of 2487 objects, although the data
objects had grown from 675 to 804.
