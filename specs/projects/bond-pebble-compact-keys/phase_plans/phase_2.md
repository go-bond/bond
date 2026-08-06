---
status: complete
---

# Phase 2: Freeze the Compression, Filter, and Benchmark Baseline

## Overview

This phase removes Bond's accidental all-keyspace fast-compression override while preserving the intentional low-read-latency value-storage policy. It exposes attributable Pebble compression and table-filter candidates, directly reusing the pinned revision's progressive Bloom and progressive binary-fuse policies. It also adds a deterministic compact-key lifecycle harness, logical oracle, machine-readable manifests, and retained repeated-run evidence so Phase 3 can compare KeySchema candidates against one frozen non-schema configuration without changing the Pebble commit, logical keys, default KeySchema, or `FormatNewest` baseline.

## Steps

1. Re-run the Bond/Pebble drift gates, verify the module and external checkout still identify Pebble commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`, and retain the Phase 1 format-30/default-KeySchema assumptions.
2. Update `options.go` so `spanPolicyFunc` no longer returns `PreferFastCompression=true` for the entire keyspace, while continuing to return `pebble.ValueStorageLowReadLatency` unchanged.
3. Add explicit, named compression and table-filter profile types plus a validated configuration builder. Keep existing `BuildPebbleOptions(PerformanceProfile)` callers source-compatible by delegating to the selected baseline. Apply Pebble's own `DBCompressionBalanced`, `DBCompressionGood`, `DBTableFilterPolicyProgressive`, and `DBTableFilterPolicyBinaryFuseProgressive` definitions rather than copying their level tables.
4. Extend `options_test.go` with profile validation, exact per-level policy assertions, invalid-profile rejection, and an SST-property regression test proving the span policy no longer replaces configured compression with `Fastest` and still emits the low-read-latency value policy.
5. Add `_benchmarks/compactkeys` with deterministic dataset specs/generation for the required primary-key, order, prefix, index-cardinality, partial-index, and 1/3/8-index shapes. Encode realistic unchanged Bond primary and secondary keys, include deterministic lifecycle mutations and misses, and fingerprint the complete logical corpus.
6. Add an independent oracle that verifies exact key/value sets, ordered forward and reverse iteration, bounded scans, point hits/misses, snapshots across updates/deletes, compaction, reopen, and checkpoint restore.
7. Add a lifecycle runner that uses isolated caller-supplied directories, captures load/query/compaction/reopen/checkpoint latency distributions, Pebble metrics, level/file/write-amplification data, SST component/filter/compression/schema properties, filter outcomes for known misses, total database bytes, and full effective Pebble options.
8. Add stable JSON manifest types and environment capture for Bond/Pebble revisions, runtime/platform, seed and generator parameters, warmup/repetition/run identity, active default schema, format version, engine profiles, and machine/storage context. Add a command and named Go benchmarks that emit comparable candidates for `benchstat` and retained artifacts.
9. Add generator, coverage, oracle-drift, manifest round-trip, lifecycle reproducibility, options-profile, and SST-property tests. Format/vet/build, run focused benchmark tests, then run full and race suites serially because repository tests share fixed `test_db` paths.
10. Run the compression candidates first under the uniform Bloom filter, freeze the acceptable compression winner, then compare uniform Bloom, progressive Bloom, and progressive binary fuse under that winner. Retain repeated manifests, raw benchmark output, variance and CPU/latency/size analysis, and the selected non-schema baseline under `specs/projects/bond-pebble-compact-keys/artifacts/phase_2/`.

## Tests

- `TestBuildPebbleOptionsProfiles`: verifies `FormatNewest`, default KeySchema registration, selected compression/filter defaults, resource profiles, and the preserved low-read-latency policy without global fast compression.
- `TestBuildPebbleOptionsExperimentProfiles`: verifies the legacy-configured, balanced, and good compression layouts plus uniform Bloom, progressive Bloom, and progressive binary-fuse policies exactly match the pinned Pebble definitions.
- `TestConfiguredCompressionVisibleInSSTProperties`: proves an explicitly configured non-fast compression profile survives flush/compaction writer construction and is visible in SST properties.
- `TestDatasetGeneratorDeterministic`: verifies identical specs produce byte-identical records, mutations, index ordering, miss probes, and corpus digest.
- `TestDatasetGeneratorCoversShapes`: verifies bounded fixtures cover every required key/order/prefix/cardinality/partial-index and 1/3/8-index shape.
- `TestVariableOrderUsesMultipleEncodedLengths`: proves the variable-order workload produces multiple encoded payload lengths.
- `TestOracleDetectsRecordAndOrderDrift`: verifies changed values, missing entries, and reordered expected scans fail independently.
- `TestStorageMetricsSnapshotUsesPebbleGlobalWriteAmp`: proves reported write amplification uses Pebble's global `Metrics.Total().WriteAmp()` semantics.
- `TestManifestRoundTrip`: verifies every reproducibility and result field survives stable JSON serialization.
- `TestLifecycleOracleAndManifest`: verifies load, snapshot mutations, scans, compaction, reopen, checkpoint restore, metrics, SST properties, and manifest identity on an isolated bounded fixture.
- `BenchmarkBondLifecycle*`: emits named corrected-compression and filter candidates with lifecycle/component/filter/latency metrics for repeated retained runs.
