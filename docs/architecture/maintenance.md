# Maintenance

`RunMaintenance` runs in the background, independent of ingestion and queries:

- every second: recovery of interrupted compaction jobs, and garbage collection
  when a full batch is pending or 10 s have passed since the last deletion;
- every `compaction_cycle_interval_seconds`: a compaction cycle that starts
  consecutive jobs for up to `compaction_cycle_max_seconds` while there is work.

## Compaction jobs

A job has three phases:

1. **Reserve.** Select input blocks from a pinned snapshot, write a `jobs/`
   object naming all output prefixes, publish it in the manifest. Selection is
   skipped while a checkpoint is due.
2. **Copy.** Read input blocks (coalesced ranges, rate limited to
   `compaction_io_bytes_per_second`), merge adjacent blocks of the same stream up to
   1024 records, write output packs of up to `compaction_output_object_bytes`.
3. **Publish.** Serialized with checkpoints: verify that every input is still
   live, rewrite the affected index paths, update catalog and metadata, move
   retired objects to the trash journal, conditionally PUT the manifest.

A job that fails after reservation is marked aborted; its staging objects are
deleted after one minute, and no new job starts before that. Jobs are bounded
by `compaction_job_max_blocks`, `compaction_job_max_bytes`,
`compaction_job_timeout_seconds` and an adaptive limit of source objects that
halves when publication exceeds its catalog read budget.

## What gets selected

In order of preference:

- **Merges.** Consecutive small blocks of one stream (below 1024 records) whose
  objects are older than `compaction_merge_cooldown_seconds`. Found through candidate
  objects, resumable across calls.
- **Rechunking.** A small block (at most 512 records) inside a stream that no
  neighbor can absorb, because full blocks follow it, is rewritten together
  with its successors into full blocks and one remainder at the end of the run.
  The fragment thus moves towards the stream's end, where the next checkpoint
  completes it (see [Write path](write-path.md#completing-partial-tails)). Such
  fragments stem from data written before tails were tracked or from races with
  concurrent merges; `compaction_rechunked_blocks_total` counts the rewrite.
  Candidate objects only lead to fragments up to 512 records, so a fragment scan
  additionally walks changed streams (64 per pass, only entries newer than the
  part known to be clean) and finds fragments of any size.
- **Deferred merges.** A large tail and a small new block are merged only after
  25 % growth or an hour; such seeds are skipped for up to 10 minutes instead of
  being re-examined on every pass.
- **Index page relocation.** Index pages are never copied: a relocated page
  is rewritten into the job's index pack together with its ancestors, whose
  child pointers change anyway. A copied inner page whose children move
  would be obsolete as soon as it is written. Publication fails unless every
  relocated page was rewritten.
- **Idle candidates.** Most candidate objects hold only open stream tails,
  which nothing can merge until their stream grows. A candidate whose scan
  found no work for structural reasons is remembered in memory together with
  the index roots of its small blocks' streams; later passes skip it without
  reading its catalog entry until the candidate entry (rewritten whenever the
  object changes) or one of those roots changes. Outcomes that depend on time
  (cooldown, deferred merges) or on the job being assembled are not
  remembered. `compaction_idle_candidate_skips_total` counts the skips.
- **Repacking.** Two layout goals are planned together: every stream
  (metric level) should consist of few *sections* (runs of consecutive blocks
  stored contiguously in one object, each read with one range request), and
  objects should be nearly full. Only an object with a single stream that
  fills most of `compaction_output_object_bytes` is final. A repack job
  starts from objects below half the object target (at most 2 MiB) or with a
  dead fraction of at least `compaction_reclaim_dead_fraction`, taken from
  the candidate tree. It moves a start object whole and, for each of its
  streams, the runs of sections that are due to merge. Sections grow in
  tiers like a size-tiered LSM tree: `compaction_locality_fan_in` (default
  4) consecutive sections of one size tier (powers of the fan-in below the
  object target) become one section of the next tier, and in the top tier
  sections merge while they fit into the target. The job applies these
  merges as a cascade, so what tiered jobs would merge one after the other
  is written once. Sections of at least half the target are settled. The
  open suffix (partial tail blocks) is left to merging. A job gathers start
  objects until its input fills 75 % of one output object and never takes
  more than one; outputs are therefore nearly full and never start objects
  themselves. After a complete pass over the candidates a job also runs with
  less if it reclaims dead bytes or merges sections. A start object larger
  than a job moves stream by stream. Index packs of hundreds of small pages
  are start objects only when dirty. Every second job slot repacks first;
  otherwise repacking runs when there is nothing to merge.
  `compaction_locality_disabled` turns off merging sections (start objects
  are still packed). `compaction_repack_jobs_total` counts the jobs.

  Separate packing of small objects and level locality used to undo each
  other's work. Consolidation packed small job outputs within a minute,
  including its own outputs below the limit. Locality then pulled sections
  out of those objects again, and evacuation copied what remained. On the
  development database this wrote 56 times the new data.
- **Orphan sweep.** A checkpoint or job that fails or whose process stops
  after uploading but before publishing leaves objects no manifest
  references. Once an hour the maintenance loop lists the bucket and compares
  it with everything the published manifest reaches: metadata pages, catalog
  and candidate trees with object inventories, held deltas, the trash journal
  including the keys awaiting deletion, and the staging prefixes of a pending
  compaction job. Keys unreachable in two consecutive sweeps go into the
  trash journal and are deleted like any retired object; uploads in flight
  are published long before the second sweep. Listing and reachability run
  without the publication lock.

## Garbage collection

Deletion works through the trash journal: up to 256 keys from up to 64 journal
pages per pass, 8 DELETEs in parallel within 2 seconds, then one manifest
publication. Objects are deleted only when no running query pins an older
generation. Deleting is idempotent, so interrupted passes are simply repeated.

Objects written by a checkpoint whose manifest was never published are not
referenced and not in the journal; they are orphaned (see
[Troubleshooting](../operations/troubleshooting.md#orphaned-objects)).
