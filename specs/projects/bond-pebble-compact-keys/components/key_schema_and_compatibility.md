---
status: complete
---

# Component: KeySchema and Storage Compatibility

## Purpose and Scope

Evaluate the global complete-key physical schema, durable reader registry, writer selection, mixed-schema lifecycle, backup metadata, and inspection. It preserves logical key bytes and does not parse catalog-specific inner fields. Phase 3 rejected all full-key candidates, so their implementation and registry are retained only for controlled tests and benchmarks; no production reader or writer rollout is active.

## Schema Interface and Invariants

Construct candidates from the finalized comparer and immutable bundle size:

```go
func fullkeyexperiment.New(comparer *pebble.Comparer, bundleSize int) (pebble.KeySchema, error)
```

Only bundle sizes 16, 32, and 64 are accepted for v1. Durable names are `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and `bond/full-key/v1-b64`.

The writer places complete user keys in `PrefixBytes`. Physical common-prefix length is computed across adjacent complete keys, while logical `PrefixLen` always equals `comparer.Split(key)`. The seeker decodes exact bytes, uses bytewise lower-bound ordering, supports short bounds, and maintains independent scratch state per seeker.

Pinned Pebble cannot safely encode a stored empty key through this PrefixBytes design. A fixed marker consumes the iterator's required one-byte allocation spare and fails checkptr validation, while Pebble's invariant validator rejects an empty first decoded key. Because Bond already permits empty point mutations, this is an activation failure: the experiment must not change Bond's API contract or appear in production options.

Schema behavior behind a used name is immutable. Any algorithm, parser, or bundle change receives a new name.

## Registry and Writer Selection

For accepted schemas, create the default schema and every required Bond reader after comparer initialization, validate unique names, and ensure the active writer exists. The rejected Phase 3 schemas are configured only by `internal/fullkeyexperiment` for tests and benchmarks. Production options expose neither their names nor a writer-selection field and register only Pebble's legacy default.

Mixed schemas are normal. Changing the writer affects only future SSTs. Reader deletion is prohibited while live or restorable files may require the name.

## Backup and Inspection

Version backup metadata with `ReaderEpoch`, `FormatMajor`, and a stable sorted `RequiredKeySchema` list. Old metadata means the legacy/default requirement plus its recorded format. Restore checks format and schemas before any destination replacement. Inspection enumerates table properties and aggregates file count/bytes by durable name.

Every open path uses the centralized options builder. Direct ad hoc `pebble.Open` calls are either removed or limited to tests that intentionally verify invalid options.

## Test Plan

- `TestFullKeySchemaRoundTrip`: valid non-empty short, long, primary, secondary, and random experimental keys decode exactly; empty seek targets remain covered.
- `TestFullKeySchemaOrderingAndSeek`: exact/gap/boundary seeks equal a bytewise oracle.
- `TestFullKeySchemaSplit`: decoded and original `Comparer.Split` results match.
- `TestFullKeySchemaPrefixAndReverseIteration`: strict prefix and reverse scans match logical results.
- `TestFullKeySchemaConcurrentSeekers`: independent seekers do not alias mutable state.
- `TestFullKeySchemaCorruption`: truncated/corrupt blocks return errors without panic.
- Fuzz properties for round trip, ordering, seek, and split across every bundle size.
- `TestExperimentalSchemaSelectionIsNotInProductionOptions`: production options expose only the legacy schema while internal configuration validates experimental names.
- `TestProductionEmptyPointMutationsRemainAccepted`: Bond database and batch point mutations retain their pre-experiment synchronous empty-key behavior.
- `TestMixedSchemasReopenCompactRollback`: controlled legacy and v1 files survive compaction and rollback with the internal reader registry.
- `TestBackupRequiredSchemas` and `TestRestoreRejectsMissingSchemaBeforeReplace`.
- `TestAllOpenPathsUseSchemaRegistry`: normal, migration, backup, restore, inspector, and benchmark paths receive the same registry.
