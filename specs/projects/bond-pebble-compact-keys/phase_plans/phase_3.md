---
status: complete
---

# Phase 3: Prove the Global Full-Key Schema

## Overview

This phase implements the immutable `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and `bond/full-key/v1-b64` physical KeySchema candidates on the pinned stock Pebble revision. The schemas store each complete logical Bond user key in one `PrefixBytes` column while preserving the comparer-defined logical prefix, bytewise ordering, WAL/memtable keys, and query semantics for the supported non-empty corpus. All readers are registered inside the controlled test/benchmark registry before experimental writer selection, the corrected-legacy compression plus uniform-Bloom Phase 2 configuration remains the incumbent, and retained lifecycle evidence decides whether any candidate may become the default.

## Steps

1. Re-run the Pebble revision/format/schema drift gates and add a focused provenance check that the implementation still targets commit `8fb150d9135d6f94e183a874475e0bd1afb18f63` without a replace or fork.
2. Add the isolated full-key schema implementation under `internal/fullkeyexperiment`, with a constructor and immutable names available only to repository tests and benchmarks for bundle sizes 16, 32, and 64; reject nil/incomplete comparers and every other bundle size.
3. Implement the one-column KeyWriter with complete-key physical common-prefix calculation, comparer-derived logical `PrefixLen`, duplicate/internal-suffix ordering, exact materialization, and clean reset/reuse behavior. Record that Pebble's non-empty stored-key precondition makes this candidate incompatible with Bond's existing empty-key API contract and therefore ineligible for rollout.
4. Implement the one-column KeySeeker with exact logical-key reconstruction, bytewise lower-bound seeks, correct logical-prefix equality, empty/short bounds, forward/reverse materialization, synthetic suffix replacement at `Comparer.Split`, safe lower-bound checks, and read-only shared block metadata suitable for independent concurrent iterators.
5. Keep `PebbleOptionsConfig` and all production open paths on Pebble's legacy reader/writer only. Configure experimental readers and writer selection solely through the internal package in controlled tests and benchmarks; preserve Bond's existing database, batch, backend, and raw Pebble mutation behavior, including empty point keys.
6. Add direct writer and SST-level tests covering valid and truncated non-empty Bond keys, empty seek/iterator bounds, long and shared-prefix keys, exact/gap/before/after seeks, duplicate user keys with different internal trailers, block reset/reuse, forward/reverse and strict-prefix iteration, snapshots/batches/range deletion, synthetic suffixes, corruption/truncation safety, randomized bytewise oracle comparisons, and concurrent independent iterators across all bundle sizes. Add a production regression test proving the experiment did not change synchronous empty-key mutations.
7. Add a controlled mixed-schema lifecycle integration test that writes legacy SSTs, switches to a full-key writer with both readers registered through the internal helper, reopens, compacts, checkpoints, reopens the checkpoint, rolls the active writer back to legacy while retaining the full-key reader, and verifies the complete logical oracle throughout.
8. Extend `_benchmarks/compactkeys` with immutable full-key candidates and a Phase 3 command/benchmark comparing the frozen corrected-legacy/uniform-Bloom baseline against b16/b32/b64 while keeping seed, corpus, format, compression, filters, and logical keys fixed. Record schema file/byte counts and all existing lifecycle/component/CPU/latency/allocation metrics in manifests and CSV.
9. Run formatting, diff checks, vet/build, focused schema and benchmark tests, full tests, race tests, and named lifecycle benchmarks serially where repository tests share fixed paths.
10. Retain Phase 3 manifests, raw benchmark output, variance/gate analysis, and verification/provenance under `artifacts/phase_3/`. All candidates miss the size gate and the empty-key compatibility requirement, so retain legacy as the sole production reader/writer and preserve the full-key implementation only as an internal rejected experiment.

## Tests

- `TestFullKeySchemaValidationAndImmutableNames`: accepts only b16/b32/b64 and binds each encoding to its durable name.
- `TestFullKeySchemaWriterComparisonAndReset`: proves comparer split, complete-key physical common prefix, duplicate comparison, materialization, and block reuse.
- `TestFullKeySchemaRoundTrip`: exact SST round trips for short, primary, secondary, long, shared-prefix, and randomized non-empty keys at every bundle size.
- `TestFullKeySchemaOrderingAndSeek`: exact/gap/before-first/after-last lower bounds match a bytewise oracle.
- `TestFullKeySchemaSplitAndPrefixIteration`: decoded keys preserve `Comparer.Split` and strict logical-prefix scans.
- `TestFullKeySchemaReverseAndSyntheticSuffix`: reverse iteration and suffix replacement preserve the expected logical bytes/order.
- `TestFullKeySchemaConcurrentSeekers`: independent concurrent iterators do not alias mutable seeker state.
- `TestFullKeySchemaCorruption`: truncated/corrupt physical blocks fail with an error and never panic.
- `TestFullKeySchemaCorruptSSTDataBlock`: a structurally readable, checksum-valid SST whose full-key PrefixBytes page metadata is corrupt fails through Pebble's data-block metadata initialization path.
- `TestFullKeySchemaRangeDeletionsLifecycle`: every controlled legacy and full-key writer preserves range-tombstone results across flush, compaction, and reopen against a point-and-iteration oracle.
- `TestExperimentalSchemaSelectionIsNotInProductionOptions`: production options expose only Pebble's legacy schema; internal test configuration retains focused selection evidence.
- `TestProductionEmptyPointMutationsRemainAccepted`: database and batch point sets/deletes retain Bond's synchronous empty-key behavior under the production legacy writer.
- `FuzzFullKeySchemaRoundTripAndSeek`: arbitrary non-empty stored fragments and arbitrary targets preserve bytes, ordering, seeking, and split across all bundle sizes.
- Internal schema selection tests prove all experimental readers are available only behind the repository-internal boundary and unknown names fail there.
- `TestMixedSchemasReopenCompactRollback`: controlled legacy and full-key files survive reopen, checkpoint, compaction, and writer rollback with no logical drift.
- `TestLifecycleFullKeyCandidates`: the bounded lifecycle oracle and manifest capture work for b16/b32/b64 against the frozen baseline.
- `BenchmarkBondLifecycleFullKeySchemas`: emits comparable Phase 2 incumbent and Phase 3 candidate size, component, CPU, latency, and allocation metrics.
