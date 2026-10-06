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
- **Level locality.** A stream (metric level) consists of *sections*: runs of
  consecutive blocks stored contiguously in one object, each read with one
  range request. Locality lets sections grow in tiers, like a size-tiered LSM
  tree: `compaction_locality_fan_in` (default 4) consecutive sections of one
  size tier (powers of the fan-in below `compaction_output_object_bytes`) are
  rewritten into one section of the next tier; in the top tier (at least a
  quarter of the target) sections merge while they fit into the target.
  Sections of at least half the target are settled and not rewritten again.
  A stream thus has its settled sections plus at most fan-in − 1 sections per
  smaller tier, and every byte is rewritten about log_fanIn(target / first
  section) times (about 3 times from 40 KiB to 4 MiB). The open suffix
  (partial tail blocks) is left to merging. Every fourth job gives locality a
  turn; otherwise it runs when there is nothing to merge.
  `compaction_locality_disabled` turns it off.
- **Reclamation.** Objects whose dead fraction exceeds `compaction_reclaim_dead_fraction`
  are evacuated (live blocks copied) so the whole object can be deleted.

## Garbage collection

Deletion works through the trash journal: up to 256 keys from up to 64 journal
pages per pass, 8 DELETEs in parallel within 2 seconds, then one manifest
publication. Objects are deleted only when no running query pins an older
generation. Deleting is idempotent, so interrupted passes are simply repeated.

Objects written by a checkpoint whose manifest was never published are not
referenced and not in the journal; they are orphaned (see
[Troubleshooting](../operations/troubleshooting.md#orphaned-objects)).
