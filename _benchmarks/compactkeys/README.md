# Compact-key lifecycle baselines

This package generates unchanged logical Bond primary and secondary keys from a recorded seed, verifies them with an independent bytewise oracle, and captures a full Pebble load/mutate/snapshot/flush/compact/query/reopen/checkpoint lifecycle. It is the comparison harness for physical KeySchema experiments; a later candidate should keep the dataset digest and engine settings fixed and change only `writer_schema`/`bundle_size` through `RunRequest.OptionsBuilder`.

Focused Go benchmark output suitable for `benchstat`:

```bash
cd _benchmarks
go test -run '^$' -bench '^BenchmarkBondLifecycleBaselines$' -benchmem -count=5
```

Repeated machine-readable manifests and CSV summary:

```bash
cd _benchmarks
go run ./compactkeys/cmd/compact-key-baseline \
  --seed=20260806 \
  --rows=10000 \
  --warmups=1 \
  --repetitions=3 \
  --output=../specs/projects/bond-pebble-compact-keys/artifacts/phase_2/baselines
```

Phase 3 full-key schema comparisons use the internal rejected-schema package,
keep the frozen Phase 2 policy, and vary only the controlled physical writer
and its immutable bundle size. These commands do not enable a Bond production
reader or writer. Bond production opens reject these names; the harness opens
isolated raw Pebble databases with the internal registry solely to retain the
mixed-schema and rollback evidence:

```bash
cd _benchmarks
go test ./compactkeys -run '^$' -bench '^BenchmarkBondLifecycleFullKeySchemas$' -benchmem -count=6
go run ./compactkeys/cmd/compact-key-schema \
  --seed=20260806 \
  --rows=20000 \
  --warmups=1 \
  --repetitions=3 \
  --output=../specs/projects/bond-pebble-compact-keys/artifacts/phase_3/candidates
```

Compression candidates always use uniform Bloom so compression remains attributable. Filter candidates use the frozen corrected-legacy compression baseline. Point misses use `Get` and require `pebble.ErrNotFound`; separate filter-diagnostic probes use `SeekPrefixGE` with `UseL6Filters=true` because the diagnostic full compaction normally places the representative corpus in L6. All databases and checkpoints live under isolated temporary run roots.

Non-schema baselines use Pebble's comparer-derived legacy writer and exactly one legacy reader. Only explicit compact-key candidates install the internal experimental reader set. Manifest/checkpoint compatibility requirements are derived from encountered SST schema properties (with the active writer as an empty-SST fallback), not every reader registered for a controlled experiment.

Each JSON manifest records the Bond commit/dirty state plus a content fingerprint of the benchmark-relevant source (including key encoding, options, storage compatibility, internal schema experiments, module resolution, and the harness itself), exact Pebble origin hash, Go/runtime and machine/storage context, complete effective Pebble options, structured span/value-storage policy, active writer and sorted schema readers, actual bundle size, `FormatNewest`, logical layout, dataset spec/digest, warmups/repetitions, latency distributions, post-flush and post-compaction Pebble/SST snapshots, compaction deltas, and filter observations. SST physical bytes and non-reconcilable table-property byte counters are labeled independently.
