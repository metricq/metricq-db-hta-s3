# Maintenance

`RunMaintenance` runs in the background, independent of ingestion and queries:

- every second: recovery of interrupted compaction jobs, garbage collection
  when a full batch is pending or 10 s have passed since the last deletion,
  and, once an hour, the orphan sweep;
- every `compaction_cycle_interval_seconds`: a compaction cycle that starts
  consecutive jobs for up to `compaction_cycle_max_seconds` while there is work.

[Data structures](data-structures.md) shows the structures these operations
read and write.

## Compaction jobs

A job has three phases:

1. **Reserve.** Select input blocks from a pinned snapshot, write a `jobs/`
   object naming all output prefixes, publish it in the manifest. Selection is
   skipped while a checkpoint is due.
2. **Copy.** Read input blocks (coalesced ranges, rate limited to
   `compaction_io_bytes_per_second`), merge adjacent blocks of the same stream
   up to 1024 records, write output packs of up to
   `compaction_output_object_bytes`. Inputs are ordered by stream and time, and
   no job splits a stream's run across two output objects unless the run is
   larger than one. Such a run starts a new object and is cut into full
   objects of its own and a remainder.
3. **Publish.** Serialized with checkpoints: verify that every input is still
   live, rewrite the affected index paths, update catalog and metadata, move
   retired objects to the trash journal, conditionally PUT the manifest.

A job that fails after reservation is marked aborted; its staging objects are
deleted after one minute, and no new job starts before that. Jobs are bounded
by `compaction_job_max_blocks`, `compaction_job_max_bytes`,
`compaction_job_timeout_seconds` and an adaptive limit of source objects that
halves when publication exceeds its catalog read budget.

## Job kinds

There are three kinds of jobs. Merge and rechunk rewrite the *contents* of
blocks (partial blocks become full ones). Repack moves blocks unchanged and
changes only *where* they are stored.

Every second job slot tries repack first. Otherwise a slot tries merges,
then rechunking, then repack, and runs the first that finds work.

### Merge

Consecutive small blocks of one stream (below 1024 records) are merged into
full blocks. Seeds are found through the candidate tree, and the search
resumes across calls. Only objects older than
`compaction_merge_cooldown_seconds` take part, so a tail that is still growing
is not rewritten again and again. A large tail and a small new block are merged
only after 25 % growth or an hour; such seeds are skipped for up to 10
minutes instead of being re-examined on every pass.

### Rechunk

A small block (at most 512 records) inside a stream that no neighbour can
absorb, because full blocks follow it, is rewritten together with its
successors into full blocks and one remainder at the end of the run. The
fragment thus moves towards the stream's end, where the next checkpoint
completes it (see [Write path](write-path.md#completing-partial-tails)). Such
fragments stem from data written before tails were tracked or from races with
concurrent merges. Candidate objects only lead to fragments of up to 512
records, so a fragment scan also walks changed streams (64 per pass, only the
entries newer than the part known to be clean) and finds fragments of any
size. `compaction_rechunked_blocks_total` counts the rewrites.

### Repack

**Goal.** Repack serves two layout goals at once:

- every stream (metric level) consists of few *sections*: runs of
  consecutive blocks stored contiguously in one object, each read with one
  range request;
- objects are nearly full.

A stream's past consists of *full sections*: sections that fill an object of
`compaction_output_object_bytes` alone, up to the stream's largest block (at
least 15/16 of the target). Only such an object is final. Only the newest
sections of a stream are smaller, and their objects may hold many streams,
which suits small streams such as higher aggregate levels.

**Start objects.** A job starts from objects in the candidate tree that are
below half the object target (at most 2 MiB) or have a dead fraction of at
least `compaction_reclaim_dead_fraction`. Small objects are mostly checkpoint
outputs and the last, partly filled output of earlier jobs. Partly dead
objects are left behind when other jobs move blocks out of them. A job takes
a start object whole; a start object larger than one job moves stream by
stream over several jobs.

**Which sections merge (merge rule).** Consecutive sections of one stream,
starting with one that is not full, merge when

- their sum reaches the object target: copying cuts them into full sections
  and a remainder, which joins the next merge; or
- they stay below the target, are at least `compaction_locality_fan_in`
  (default 4) sections, and the largest of them is at most as large as all
  others together.

The oldest such run is taken, extended as far as it stays mergeable but not
beyond the section that brings it to the target. Below the target, sections
grow like the runs of a size-tiered LSM tree: a merge at least doubles each of
its parts, and a large section is never rewritten whole to absorb a few small
ones; the small ones first merge among themselves until they match it.
Checkpoint sections vary in size (2 to 20 KB on the development database); the
rule does not sort them into fixed size classes, so a slightly larger section
does not hold up its neighbours. A full section is written for the last time.
Sections of 2.1, 2.7, 2.9 and 2.5 MiB, for example, become 4 + 0.8 + 2.9 + 2.5
and then 4 + 4 + 2.2 MiB.

A run may end with a full section. A smaller section before full ones (left
by an earlier layout or a rechunk) therefore merges into the next full
section, and the remainder is passed on until the stream's newest sections.
This rewrites the stream behind such a gap once.

Simulated with checkpoint sections of 2 to 18 KB, every byte is rewritten
about six times until it is part of a full section, and a stream keeps at most
about a dozen sections that are not full. A prefix of full sections is not
read again until a job moves blocks within it. The open suffix of a stream
(partial tail blocks) is left to merging.

**Cascade.** For every stream of a start object, the job applies the merge rule
repeatedly, as if each merge were a job of its own. If four 3 KB sections
become one 12 KB section and three more 12 KB sections already exist, the
same job merges all of them into one 48 KB section. What tiered jobs would
merge one after the other is therefore written once. The job moves the start
object's blocks of that stream together with the sections the cascade
merges.

**Stream walk.** Some streams are fragmented only inside large, clean objects
that are no start objects. For example, at a restart a checkpoint writes the
held blocks of all streams into a few large objects. After its start objects,
a job therefore also walks the streams round robin, at most 256 per job, and
adds their due merges. It skips streams whose index root is unchanged since a
walk found nothing due there.

**Index packs.** Every job and checkpoint replaces index pages, so index packs
become partly dead quickly and rank first in the candidate tree. If they came
first, their pages would fill a job's block limit and starve the data merges.
They are therefore only noted during the pass. After the data, a job takes
dirty index packs as filler while it has room, up to a quarter of its blocks.
Clean, small index packs only gather in a job of index packs alone, which
runs after a complete pass when eight have accumulated or one is dirty; this
bounds their number without moving them in every job. Index packs of hundreds of small pages, which packing cannot
grow much, are start objects only when dirty.

**When a job runs.** A job gathers start objects, due merges from the stream
walk and index packs up to the job limits (`compaction_job_max_bytes` and
`compaction_job_max_blocks`, the source object limit, and a budget of index
page reads for planning). Output objects are filled stream by stream without
splitting a run, so at most the last output of a job stays partly empty, and
full outputs are never start objects. A job runs when one of these holds:

- its input fills 75 % of one output object;
- it has reached one of its limits;
- a complete pass over the candidates found dead bytes to reclaim;
- it has due merges that have waited for 30 minutes.

Due merges wait for a nearly full job like everything else: a job that ran
for a few merges would leave a partly filled last output, which becomes a
start object and is moved again. The stream walk moves past due streams only
with a job that merges them, so a waiting merge is found again. The 30-minute
bound lets a quiet database merge as well.

`compaction_locality_disabled` turns off merging sections; start objects are
still packed. `compaction_repack_jobs_total` counts the jobs, and
`compaction_input_bytes_total{kind}` the bytes moved per job kind.

**Costs.** On the development database (1009 metrics, about 14 MB of new data
per hour, after the first catch-up), compaction moved about 190 MB per hour
while due merges ran at once and every job took index packs as filler.
Repack inputs split into roughly 45 % start objects, 23 % index packs, 19 %
due merges of the start objects' streams and 12 % stream walk. Start objects
exceeded the checkpoint outputs (about 23 MB per hour) because jobs that ran
only for due merges left a partly filled last output behind.

**History.** Packing small objects ("consolidation") and merging sections
("level locality") used to be separate job kinds that undid each other's
work. Consolidation packed small job outputs within a minute, including its
own outputs below the limit. Locality then pulled sections out of those
objects again, and evacuation copied what remained. On the development
database this wrote 56 times the new data to S3.

## Supporting mechanisms

- **Index page relocation.** Index pages are never copied: a relocated page
  is rewritten into the job's index pack together with its ancestors, whose
  child pointers change anyway. A copied inner page whose children move
  would be obsolete as soon as it is written. Publication fails unless every
  relocated page was rewritten.
- **Idle candidates.** Most candidate objects hold only open stream tails,
  which nothing can merge until their stream grows. A candidate whose merge
  scan found no work for structural reasons is remembered in memory, together
  with the index roots of its small blocks' streams. Later passes skip it
  without reading its catalog entry until the candidate entry (rewritten
  whenever the object changes) or one of those roots changes. Outcomes that
  depend on time (cooldown, deferred merges) or on the job being assembled
  are not remembered. `compaction_idle_candidate_skips_total` counts the
  skips.
- **Orphan sweep.** A checkpoint or job that fails, or whose process stops,
  after uploading but before publishing leaves objects that no manifest
  references. Once an hour the maintenance loop lists the bucket and compares
  it with everything the published manifest reaches: metadata pages, catalog
  and candidate trees with object inventories, held deltas, the trash journal
  including the keys awaiting deletion, and the staging prefixes of a pending
  compaction job. Keys unreachable in two consecutive sweeps go into the
  trash journal and are deleted like any retired object. Uploads in flight
  are published long before the second sweep. Listing and reachability run
  without the publication lock. `metricq_db_orphan_candidates` and
  `metricq_db_orphans_retired_total` report them.

## Garbage collection

Deletion works through the trash journal: up to 256 keys from up to 64 journal
pages per pass, 8 DELETEs in parallel within 2 seconds, then one manifest
publication. Objects are deleted only when no running query pins an older
generation. Deleting is idempotent, so interrupted passes are simply repeated.
Objects that never reached the journal, because their checkpoint or job was
never published, are found by the orphan sweep.
