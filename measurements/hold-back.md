# Holding streams back

Without holding, every checkpoint writes one data block per stream that received
records since the previous checkpoint, and rewrites that stream's rightmost
index path. With many metrics at low rates this produces tiny blocks and index
pages far faster than compaction can merge them.

With `hold_seconds` > 0 a checkpoint writes a stream (one metric and HTA level)
only

- in complete 1024-record blocks, as soon as it has them,
- completely, once its oldest held record arrived more than `hold_seconds` ago,
- completely, largest streams first, while held records exceed `hold_bytes`
  (until half of that budget remains).

Everything else stays in memory, where queries already read unflushed records.
Held records that no earlier delta covers are written together as one `held/`
object per checkpoint. The manifest lists the deltas still needed and, per
stream with delta records, the time of its last written record. The WAL is
still released after every checkpoint. After a restart the engine reads the
listed deltas, skips records at or before each stream's watermark (they are
already in blocks), and replays the WAL as before. A delta is moved to the
trash journal once no stream needs any of its records.

Holding requires background maintenance. `hold_bytes` defaults to half of
`builder_hard_bytes` and must stay below it; the builder limit also bounds
records being uploaded and newly ingested ones. The executable holds for one
hour by default. The example configuration uses a 512 MiB hold budget and a
768 MiB builder limit.

## Measurement

Engine-level simulation with a simulated clock and a file-backed object store:
1500 metrics for one hour (100 at 10 Hz, 900 at 1 Hz, 300 at 1/10 s, 150 at
1/min, 40 at 1/h, 10 at 1/day, about 1,930 points/s), HTA `interval_min` 1 s,
factor 10, append-only aggregates, 4 MiB object target, 256 MiB builder limit
(hold budget 128 MiB), flushes triggered as in the executable.

| | No holding | Hold 1 h | Hold 15 min |
|---|---:|---:|---:|
| New blocks below 1024 records | 289/s | 3.3/s | 6.2/s |
| Object store writes per hour | 4.3 GB (3.46 GB index) | 460 MB | 970 MB |
| PUT requests per hour | 5,164 | 785 | 9,231 |
| Mean checkpoint duration | 1.1 s | 95 ms | 95 ms |
| Engine heap | 50-100 MB | 140-270 MB | 160-240 MB |

With holding, data blocks are 175 MB, deltas 173 MB (held records are written
twice), manifests 90 MB and index pages 13 MB per hour. (The compaction pass
after that hour reported no jobs, but it was not a valid measurement:
reservation then treated held records as checkpoint backlog and never started a
job while they exceeded the object target. This is fixed and covered by
`TestHoldDoesNotBlockCompaction`.) In steady state
each stream contributes at most about one fragment per hold interval, here
about 3 per second, compared with a measured compaction capacity of about 235
blocks per second on local RustFS. A 15-minute hold is worse overall: some
stream expires every few seconds, so checkpoints (each with a manifest)
become frequent.

This mix holds about 1.3 million records (130 MB estimated). With a 128 MiB
budget, part of the 3.3 fragments per second were pressure writes; size
`hold_bytes` above the expected held set. As a rule of thumb, a stream holds
up to 1023 records plus what it receives within the hold interval, at 96
estimated bytes per record.

Without holding, the same hour produced about one million small blocks, and
compaction then stopped with `catalog metadata byte budget exceeded` because
every checkpoint object carries thousands of block descriptors. Compaction now
adapts its selection to that budget instead of stalling, see
[compaction improvements](compaction-optimizations.md); the fragment rate
without holding still far exceeds its capacity.
