---
status: complete
---

# Functional Spec: Bond Pebble Compact Keys

## 1. Storage Model

1. Bond must continue to open exactly one Pebble database per Bond database.
2. Cross-table and cross-index changes must remain atomically writable through one Pebble batch.
3. Callers write logical table and index operations. No public or internal mutation API may require a physical KeySchema argument on `Set`, `Delete`, or batch mutations.
4. The first compact-key schema changes only SST physical encoding. Decoded user keys, bytewise ordering, comparer `Split` results, range bounds, values, WAL records, and memtable keys must remain equivalent.

## 2. Dependency and Format Baseline

1. Phase 1 must replace the incorrect worktree selection of Pebble `v1.1.5` with exact commit `8fb150d9135d6f94e183a874475e0bd1afb18f63`.
2. Phase 1 must run `go list -m -json github.com/cockroachdb/pebble` after resolution and prove the module origin/hash is the approved commit. Merely observing a generated pseudo-version is insufficient without tying it to that hash.
3. Phase 1 must run `go-outdated`, retain its report, upgrade all direct and indirect dependencies to the latest versions compatible with their existing module paths, run `go mod tidy`, and rerun the audit. Any dependency intentionally held back needs a documented incompatibility and test evidence.
4. The Pebble pin is an explicit exception to automated "latest" recommendations.
5. Bond's default/opened storage format must be `pebble.FormatNewest` from the pinned commit, currently numeric version 30. Upgrade tests must cover an older fixture, the ratchet, reopen, backup/restore, and rejection by binaries that cannot understand the newer format.
6. All Pebble API adaptations must receive a semantic review: filters, compression, value separation, iterator behavior, ingestion, cache accounting, span policy, and open/migration paths.

## 3. Benchmark and Policy Behavior

1. Benchmark generation must be deterministic by seed and cover sequential/random integers, 20/32/64-byte keys, UUID/address-like keys, composite keys, fixed and variable ordering fields, low/high-cardinality indexes, common/random prefixes, partial indexes, and 1/3/8 secondary indexes.
2. Every retained run must record Bond and Pebble revisions, Go/runtime platform, seed, workload parameters, full effective Pebble options, storage format, schema name/bundle size, warmup/repetition counts, and machine/storage context.
3. A logical oracle must compare exact records, ordered index sequences, bounds, forward/reverse scans, snapshots, deletes/updates, reopen, restore, and mixed-schema compaction.
4. Correct the current all-keyspace `PreferFastCompression=true` behavior or deliberately scope it. Compression choices must be visible in resulting SST properties.
5. Compare current, progressive Bloom, and progressive binary-fuse policies independently after compression is fixed. Report filter bytes and false-positive behavior as well as latency.
6. Report total/SST/component bytes, bytes per row/index entry, file counts, L0 overlap, write amplification, compaction bytes/CPU/time, latency distributions, allocations, cache/filter metrics, and recovery/restore time where applicable.
7. Benchmark candidates stop if variance cannot distinguish a 5–10% size change or if a size win violates the accepted CPU/latency budget.

## 4. Global Full-Key Schema

1. Provide immutable candidates `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and `bond/full-key/v1-b64` unless the pinned API forces an equivalent durable naming scheme.
2. Encode the entire logical Bond user key in a `PrefixBytes` physical column while reporting the original comparer-defined logical prefix length.
3. An experimental writer must preserve stored empty keys before it may roll out because Bond's existing mutation API permits them. If the pinned KeySchema API cannot satisfy that invariant, the candidate is rejected and kept outside production options. Seekers must still handle empty/short bounds, truncated Bond-like keys, exact and gap seeks, before/after range seeks, duplicate user keys with different internal suffixes, forward/reverse iteration, strict prefix iteration, synthetic prefixes/suffixes, corruption, independent concurrent seekers, and block reset/reuse.
4. Property tests must prove decoded bytes, ordering, seeks, and `Split` are equivalent to the logical bytewise oracle.
5. Controlled mixed legacy and full-key SST experiments must open, compact, back up, restore, inspect, and roll back to the legacy writer with every experimental reader present.
6. A schema may be activated beyond experiments only when representative results meet an explicitly recorded gate and all existing logical-key contracts. A neutral, losing, or incompatible result is a valid completed outcome; rejected schemas expose no production reader or writer selection.

## 5. Schema Registry and Operational Compatibility

1. One centralized options builder must assemble the comparer, active writer name, all durable schema readers, span policy, filter/compression settings, and `FormatNewest` before every Pebble open.
2. A used schema name is immutable. Its reader remains registered while any live, checkpointed, backed-up, or ingestible SST can name it.
3. Backup/checkpoint metadata must carry a storage-reader epoch and the required schema set, derived from actual table properties when feasible or conservatively from the enabled registry.
4. Restore must validate format and schema-reader requirements before replacing or opening destination data.
5. Normal open, format migration, backup/restore, inspection, benchmarks, and operational utilities must use the centralized builder.
6. Inspection output must report active writer, registered readers, schema files/bytes, and catalog routes without creating unsafe high-cardinality metrics.

## 6. Catalog and Public API

1. Catalog definitions must exist before `bond.Open` so the complete registry and any routes are known at open time.
2. Definitions must include a catalog name/version; stable table name/ID; codec descriptor/version; primary-key layout descriptor; index name/ID, uniqueness, order/key descriptors, and partial-predicate identity/version; logical layout version; and physical schema family.
3. Validation must reject duplicate/reserved identities, missing schemas, unsupported type/schema combinations, ambiguous or overlapping ranges, and incompatible persisted fingerprints.
4. Storage-relevant metadata receives a deterministic fingerprint. Opaque Go callback code is never hashed as if it were a durable semantic descriptor.
5. Catalog definitions compile to bound typed table/index handles. Handles create ordinary logical keys and issue ordinary Pebble batch mutations.
6. Breaking changes to the current Bond API are acceptable. The implementation should prefer one coherent API and migration documentation over mandatory legacy/dynamic API coexistence.
7. Compile-tested examples must demonstrate one database, at least three tables with different key shapes, secondary indexes, typed binding, atomic cross-table writes, and logical queries with no schema argument.

## 7. Typed and Per-Range Feasibility

1. Re-audit stock Pebble at the pinned commit before designing routing. At Phase 0 it has a global active `Options.KeySchema`, a reader registry, and range-aware `SpanPolicy`, but `SpanPolicy` has no schema selector.
2. Typed schema families may include safe, reusable `pk-u64`, `pk-u32`, `pk-bytes`, and opaque/full-key fallbacks. Parsers must be total for the routed key range and preserve bytewise ordering.
3. First evaluate typed schemas globally or in isolated database copies using only stock Pebble. Do not claim per-table writer routing when stock Pebble cannot select a schema per output range.
4. The feasibility phase must end with one of: stock support implemented and measured; a documented no-go with evidence; or a separately approved upstream/fork project. This project does not modify a separate Pebble checkout.
5. If stock support becomes available, routes start per table, coalesce adjacent identical policies, and measure forced SST boundaries/file amplification before considering per-index routing.

## 8. Error and Rollout Contracts

- Invalid catalogs, unknown active/required schemas, incompatible fingerprints, unsupported format versions, and ambiguous routes fail before writes with actionable errors.
- Reader support precedes writer activation. Switching writers never removes readers.
- Corrupt or truncated physical blocks fail safely; they do not panic or silently misdecode.
- Breaking API changes include migration notes and compile-time examples.
- Each experimental phase has an explicit stop gate and preserves the last proven configuration.

## 9. Acceptance Criteria

The project is complete when the exact Pebble pin and latest compatible dependency set are verified; `FormatNewest` migration is proven; compression/filter baselines are attributable; the full-key schema is either accepted with complete correctness/operational evidence or rejected with retained data; registry/backup/tooling paths are centralized; the catalog and examples are implemented; stock typed/per-range feasibility is resolved honestly; and final full, race, focused restore/version, benchmark reproducibility, and documentation audits pass.
