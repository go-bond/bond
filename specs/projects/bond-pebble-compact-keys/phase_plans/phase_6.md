---
status: complete
---

# Phase 6: Typed Families and Stock-Pebble Routing Feasibility

## Overview

This phase resolves the typed/per-range feasibility question against the exact pinned, unmodified Pebble module. Stock Pebble has one global `Options.KeySchema`; its `SpanPolicy` can force range boundaries but cannot select the schema of an output SST. Bond will therefore retain the production legacy writer and publish a measured per-table-routing no-go rather than claim unsupported behavior. Internal-only `pk-u64`, `pk-u32`, and `pk-bytes` schemas will still prove that typed primary-key-tail decomposition is correct and measurable in isolated one-family Pebble databases. Their results inform a separately scoped upstream proposal without modifying, replacing, or forking Pebble in this project.

## Steps

1. Add `internal/typedschemaexperiment` with immutable names `bond/pk-u64/v1`, `bond/pk-u32/v1`, and `bond/pk-bytes/v1`. Decompose only a secondary key's unambiguous outer Bond framing and final one-field primary-key tail; preserve the exact comparer-defined logical prefix, bytewise key, primary rows, unsupported/malformed forms through an opaque suffix path, and arbitrary seek bounds. Keep construction and writer selection internal to repository tests and benchmarks.
2. Add writer/seeker tests covering primary and secondary layouts, fixed and variable order fields, typed and opaque fallback rows, randomized values, exact/gap/short seeks, logical-prefix equivalence, forward/reverse iteration, independent seekers, range tombstone bounds, flush/reopen, and immutable names. Assert production options still register and select only the legacy schema.
3. Add `TestStockPebbleRangeSchemaCapability` as a stable executable no-go guard. Verify the pinned `pebble.SpanPolicy` exposes no schema selector and that `Options.MakeWriterOptions` resolves the one global `Options.KeySchema`, independent of the configured range policy.
4. Extend the deterministic compact-key harness with isolated family datasets (sequential/random u64, sequential/random u32, and bytes), typed-family candidates, internal options configuration, source fingerprinting, focused lifecycle tests, Go benchmarks, and a manifest/CSV command. Keep logical keys, compression, filters, format, and all other policies fixed within each incumbent/candidate pair.
5. Run retained repeated measurements on this machine. Record exact revisions/configuration, per-family incumbent/candidate size and latency results, variance, parser scope, and the routing conclusion under `artifacts/phase_6`.
6. Publish the stock-Pebble no-go and a bounded upstream proposal. The proposal may add an optional schema name to `SpanPolicy`, resolve it through `Options.KeySchemas` when creating each output writer, and test flush/compaction boundaries; it is explicitly a separate approval/project and does not add a `replace`, edit `/home/peter/Dev/other/pebble`, or activate typed production readers/writers here.
7. Run `gofmt`, focused typed-schema/capability/harness tests, the typed benchmark command, `go vet`, `go build`, example compilation/run, and `git diff --check`. Leave repository-wide tests, race suites, and backup/restore matrices to Phase 7.

## Tests

- `TestTypedFamilyRoundTrip*`: typed primary/index forms and opaque fallback forms decode byte-for-byte across u64, u32, and bytes families.
- `TestTypedFamilyOrderingAndSeek`: exact, gap, before/after, empty, short, truncated, and randomized seek targets match a bytewise oracle and preserve `Comparer.Split` equivalence.
- `TestTypedFamilyDataBlockForwardReverse`: columnar iteration in both directions materializes the original sorted logical keys.
- `TestTypedFamilyConcurrentSeekers`: independently initialized seekers over one immutable block do not share mutable scratch state.
- `TestTypedFamilyRangeBoundsReopen`: range deletion endpoints and surviving records remain correct after flush and reopen in isolated one-family databases.
- `TestTypedSchemaSelectionIsNotInProductionOptions`: production configuration exposes none of the experimental family names and keeps the legacy writer.
- `TestStockPebbleRangeSchemaCapability`: the exact stock API has no range schema selector and writer construction uses the global schema.
- `TestLifecycleTypedFamilyCandidates`: each matching isolated dataset passes the lifecycle oracle for the incumbent and typed writer and records the active schema in SST properties/manifests.
- `BenchmarkBondLifecycleTypedFamilies`: emits comparable size, compaction CPU, and hit/miss latency metrics for each isolated family pair.
