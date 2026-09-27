// Package engine is the storage engine of metricq-db-hta-s3: a single-writer
// MetricQ history database that aggregates samples into the HTA hierarchy,
// makes them durable in a local write-ahead log (WAL) and stores them as
// immutable objects in an object store (package storage).
//
// # Lifecycle
//
// Open loads the committed state from the store (the "manifest" object and
// the metadata it references), restores held records and replays the WAL.
// IngestBatch (and Ingest) accept samples; Query answers history requests;
// RunFlush writes checkpoints in the background; RunMaintenance runs
// compaction and garbage collection; Close stops the engine. Configure adds
// metrics at runtime.
//
// # Write path
//
// IngestBatch aggregates each delivery (package hta), appends one WAL frame
// per delivery and fsyncs once per batch before any state becomes visible,
// so the caller may acknowledge the deliveries when it returns. Records wait
// in memory per stream (metric and HTA level). Flush freezes a WAL segment and
// the records to write, uploads data blocks, index pages, held deltas and
// metadata without holding the ingestion mutex, and commits by conditionally
// replacing the manifest. With Options.HoldSeconds, streams are written only
// in full blocks or after the hold interval; the rest is persisted as held
// deltas so the WAL can still be released.
//
// # Concurrency
//
// e.mu protects in-memory state and is held only for CPU work and the WAL
// fsync. publishMu serializes manifest publications (checkpoints, compaction,
// garbage collection) and is acquired before e.mu. maintenanceMu serializes
// maintenance jobs. Queries and maintenance work on snapshots that pin a
// manifest generation; retired objects are deleted only after every older
// pin is released.
//
// # Invariants
//
//   - A delivery is acknowledged only after its WAL frame is durable.
//   - Every object except "manifest" is created once and never overwritten.
//   - Everything a manifest references is uploaded before the manifest; WAL
//     segments and retired objects are released only after it.
//   - Objects are deleted only through the trash journal.
//
// The architecture and operations documentation in the repository's docs/
// directory describes the layout and the tuning parameters in detail.
package engine
