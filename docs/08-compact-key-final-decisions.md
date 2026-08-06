# Compact-key final decisions

The compact-key project is complete with Bond's existing logical key encoding
and Pebble's comparer-derived legacy physical schema retained in production.
Bond continues to open one Pebble database for each Bond database. Tables and
indexes share its WAL, LSM, manifest, cache, and recovery path; `Set`, `Delete`,
table, index, and batch APIs do not accept a physical schema.

## Accepted production configuration

- Pebble is pinned to commit
  `8fb150d9135d6f94e183a874475e0bd1afb18f63` from
  `https://github.com/cockroachdb/pebble`, with no local module replacement.
- New opens and migrations use the pinned `pebble.FormatNewest`, numeric format
  30. This is a one-way storage-format ratchet, not a promise that an older
  Pebble binary can reopen the database.
- Production uses `DefaultKeyComparer()` and its
  `DefaultKeySchema(leveldb.BytewiseComparator,16)` as the active writer and
  only registered reader. The shared options path installs a fresh validated
  comparer and registry before the single production `pebble.Open` call.
- Corrected legacy compression and uniform Bloom filters remain the measured
  baseline. Progressive filters and alternative compression profiles did not
  clear the project's 5% benefit gate.
- Checkpoints and backups record format, reader epoch, and required schema
  names. Restore validates the complete chain before changing the destination.
  Reader support must precede writer activation and must remain available while
  any live, checkpointed, backed-up, or ingestible SST can require it.
- The pre-open catalog supplies stable, versioned logical descriptors and
  fingerprints, then binds typed handles that emit ordinary logical keys and
  Pebble batch mutations. Its physical-family field is descriptive until a
  future engine supports and justifies routing.

## Rejected experiments

- `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and
  `bond/full-key/v1-b64` remain internal experiments. Their best deterministic
  SST reduction was 2.71%, below the 5% gate, and pinned Pebble's PrefixBytes
  representation cannot preserve Bond's supported empty stored key. Production
  options register none of these readers or writers.
- `bond/pk-u64/v1`, `bond/pk-u32/v1`, and `bond/pk-bytes/v1` remain internal
  experiments. No measured dataset improved SST size by at least 5%; two grew.
  Stock pinned Pebble exposes one global writer schema and no schema selector in
  its range policy, so it cannot route new SST writers per table or index.
- A Pebble fork, a local `replace`, per-index routing, and changes to
  `/home/peter/Dev/other/pebble` were outside this project and were not made.
  Any future upstream schema-routing proposal is a separate approval decision.

## Operational consequences

Offline inspection can report older supported formats and unknown durable
schema names without opening the database. Production open, explicit migration,
checkpoint, restore, and diagnostics share the same compatibility model. A
writer rollback selects an older registered writer while keeping every required
reader; it cannot undo the format-30 ratchet or safely remove a reader still
named by storage.

The reproducible workload, exact commands, manifest contents, and oracle are
documented in the [benchmark methodology](../_benchmarks/compactkeys/README.md).
Detailed measurements are retained with the
[Phase 2 baseline](../specs/projects/bond-pebble-compact-keys/artifacts/phase_2/analysis.md),
[Phase 3 full-key decision](../specs/projects/bond-pebble-compact-keys/artifacts/phase_3/analysis.md),
and [Phase 6 typed-schema decision](../specs/projects/bond-pebble-compact-keys/artifacts/phase_6/analysis.md).
