# Durability and recovery

## Guarantees

- A delivery is acknowledged to RabbitMQ only after all its accepted samples are
  fsynced to the WAL. Losing the process loses nothing acknowledged; losing the
  WAL disk loses what was not yet checkpointed.
- The object store state changes atomically with the conditional PUT of
  `manifest`. Everything reachable from it was written before.
- WAL segments are deleted only after a manifest covering them is published and
  the local checkpoint file is synced. Held records are covered by `held/`
  deltas referenced from that manifest.
- Objects are deleted only through the trash journal, after the publication
  that retired them and after all queries pinning older generations finished.

## Crash scenarios

| Crash during | Effect after restart |
| --- | --- |
| Ingest before fsync | The batch is not acknowledged and is redelivered by RabbitMQ. |
| Ingest after fsync, before ACK | Samples are replayed from the WAL; the redelivery is dropped as duplicate. |
| Checkpoint upload | Manifest unchanged; WAL segments intact; uploaded objects are orphaned. |
| Manifest PUT, response lost | Read back on the next attempt; otherwise treated as failed. |
| After manifest PUT, before WAL deletion | Replay skips frames covered by the manifest. |
| Compaction | The job stays recorded; recovery aborts it and deletes its staging prefixes. |
| Garbage collection | Deletion restarts from the recorded journal position. |

## Startup checks

On start the engine verifies that the WAL belongs to this bucket prefix
(`identity`), that the remote manifest is not older than the last local
checkpoint, that metric configurations match the stored aggregation
parameters, and that every WAL frame and metadata page is intact. Any
violation stops startup without modifying data.
