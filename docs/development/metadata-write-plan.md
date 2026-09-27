# Plan: reduce checkpoint metadata writes

Status: proposal, based on c3189d8. No storage implementation changes are part
of this plan.

## Finding and scope

The 90 MB/hour figure in `measurements/hold-back.md` predates paged metadata.
It describes a one-hour mixed-rate workload with 1500 metrics, approximately
1930 samples/s, and a one-hour hold limit. It is not a measurement of the
current manifest or of 1500 metrics all sampled at 1 Hz.

Today `engine/manifest_pages.go` writes a roughly 1 KB CAS manifest referring to
`state/`, `roots/` and `held-state/`. Series and index roots already use 256
hash-partitioned pages. Changed pages and their directory share a pack; this
bounds PUT count per metadata kind but does not make bytes independent of
cardinality. Maintenance reuses checkpoint state and held metadata.

Two remaining sources of amplification deserve separate treatment:

1. `held-state/` is monolithic: changing the delta list rewrites every watermark,
   and changing a watermark rewrites the delta list.
2. A Series contains configuration, first/last points and all open HTA levels.
   Frequent input changes many Series. If nearly every metric changes between
   checkpoints, almost every existing state shard is dirty. Finer hash sharding
   alone does not avoid writing that active state.

The earlier compaction measurement (126 MB of manifests reduced to 0.40 MB,
plus 15.8 MB of stream-root metadata) concerns maintenance publications. It
cannot establish the reduction for this hourly ingestion workload.

## Recommended design

Keep one atomic commit root. All new metadata remains immutable, referenced by
checksummed addresses. A single conditional manifest PUT publishes the matching
WAL sequence, Series state, history roots and held metadata together.

Split held metadata into independently reusable roots:

- **Watermarks:** pages keyed by canonical metric and level, changed only when
  the persisted written prefix changes. A logical removal must also be recorded
  when a stream no longer needs a watermark. Derive updates from the frozen hold
  plan; do not infer changes from creation of a new delta alone.
- **Held-delta inventory:** a paged ordered descriptor collection, supporting
  additions and removals without rewriting the complete list. Use publication
  generation plus an ordinal (or another unique ordered ID), not WAL sequence
  alone: maintenance and age-triggered flushes can share a WAL sequence.
- A small immutable held-metadata root refers to both collections. The existing
  top-level HeldState reference can remain their atomic entry point.

Use copy-on-write pages with a bounded fanout/page-size target. Additions and
removals rebuild affected paths; collapse empty pages. Pack multiple changed
pages together, including changed pages from both held collections if useful.
Retirement must then check reachability across **both** directories before
retiring a shared pack. Bound object sizes and preserve per-page checksums.

Initially retain the current Series pages. Introduce explicit dirty tracking
at checkpoint freeze where it meaningfully reduces full-map comparisons, while
keeping changes arriving during upload in the next dirty set. Publish/clear the
frozen dirty set only on success; merge it back on failure. Track configuration
additions and recovery mutations as well as samples.

If measurements still show Series bytes dominating, evaluate a bounded state
patch format in a separate phase. A patch stores absolute replacements for
changed per-metric fields and HTA levels, with removals where applicable,
against an identified checkpoint base. Do not reconstruct state by re-running
floating-point aggregation in a different order. Limit both patch count and
encoded bytes; periodically publish a materialized base. A full replacement
must remain available when most state changes or compression favors a snapshot.
This phase proceeds only if the end-to-end result improves enough to justify
extra startup reads and metadata maintenance.

## Tradeoffs

- More indirection and potentially more requests during recovery. Pages sharing
  packs can use coalesced range reads, but distinct objects still require GETs.
  Startup must load and validate the complete committed checkpoint before WAL
  replay; missing or corrupt referenced metadata fails startup.
- Additional PUTs/DELETEs and request latency can outweigh saved bytes. Retain
  shared packs and batch publications rather than writing an object per metric.
- Shared packs can retain obsolete bytes while one page remains live. Track
  live/retained bytes; add metadata repacking only if this retention is material.
- GC and recovery become more complex: old generations, both held directories,
  optional patch bases, and interrupted maintenance jobs must remain reachable.
  Register new maintenance output namespaces for abort cleanup. Failed ordinary
  flushes can still leave unreachable objects; the existing orphan limitation
  is not solved merely by changing the metadata layout.
- Dirty tracking requires strict snapshot isolation. A concurrent ingest must
  neither leak into an older WAL checkpoint nor disappear when it commits.
- Sharding has limited benefit when all shards change. Patch journals add a
  recovery/compaction burden and must never grow without explicit bounds.
- This improves write cost and potentially latency under concurrent load. It
  does not directly reduce cold history data GETs: the query path already uses
  hydrated stream roots. Level-local data compaction remains a separate concern.

## Implementation sequence and acceptance

1. **Rebaseline hourly ingestion.** Repeat the original mixed-rate workload and
   1500 metrics at 1 Hz. Run for several simulated hours, covering startup and
   multiple hold expirations. Include aligned and staggered arrivals, sparse
   metrics and a cold metric retaining an old held delta. Measure holding and
   maintenance together, and separately for attribution. Record per-prefix
   bytes and calls for manifest, state, roots, held-state, held payload, catalog,
   index, data and trash, plus objects deleted and bytes still physically stored.
   Count dirty state/watermark pages, live held descriptors, checkpoint/ACK
   latency, maintenance backlog and S3-only recovery calls/bytes/time. Use the
   in-process collectors and expose/scrape the Prometheus endpoint in the
   sustained integration workload. Use the existing dependencies.
2. **Introduce the two held collections.** Add codecs and a versioned held-root
   descriptor, bounded page readers/writers and one pack writer for changed
   metadata. Materialize the current in-memory structures at startup so history
   handlers need no new storage lookups. Keep the existing manifest CAS boundary.
   Reject unknown layouts explicitly. Decide deployment handling before writing
   a changed format: previous session assumptions about disposable data need
   checking if a persistent namespace is now in use. A narrow reader for the
   present held format can transition it on a successful checkpoint if needed.
3. **Wire checkpoint updates and GC.** Generate watermark and inventory changes
   from the same frozen hold plan. Account page reachability across collections,
   journal retired packs and retain reader pins. Add explicit dirty tracking
   with failed-flush restoration. Maintenance must reuse unchanged held roots.
4. **Fault and concurrency coverage.** Inject failure before/after every metadata
   PUT, failed/lost CAS replies, restart after CAS but before WAL release,
   stale-writer CAS conflicts, missing/corrupt pages, storage quota exhaustion,
   cancellation and partial deletion. Test a shared pack whose last reference
   disappears in the other collection. Verify acknowledged records survive via
   WAL, held data or history blocks, with no duplication or premature deletion.
   Exercise concurrent ingest/flush/maintenance and bound startup patch reads
   if the optional patch phase is introduced.
5. **Compare and decide on Series patches.** Repeat identical workloads and
   legacy response comparisons, including compacted S3-only recovery. Report
   metadata bytes together with requests, retained bytes, CPU, ACK latency and
   recovery cost. Inventory-only updates must write zero watermark/Series pages;
   localized edits must touch bounded metadata paths rather than all descriptors.
   No numerical savings claim for dense Series traffic before this measurement.
   Add the bounded patch format only if Series still dominates and the measured
   write savings justify its recovery cost.

Separate commits: measurement attribution; held-page primitives; checkpoint/GC
integration with failure tests; operational documentation/results; optional
Series patches as a separately justified change.
