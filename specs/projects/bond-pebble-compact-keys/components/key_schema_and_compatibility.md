---
status: complete
---

# Component: KeySchema and Storage Compatibility

## Purpose and Scope

Implement the global complete-key physical schema, durable reader registry, writer selection, mixed-schema lifecycle, backup metadata, and inspection. It preserves logical key bytes and does not parse catalog-specific inner fields.

## Schema Interface and Invariants

Construct candidates from the finalized comparer and immutable bundle size:

```go
func FullKeySchema(comparer *pebble.Comparer, bundleSize int) (pebble.KeySchema, error)
```

Only bundle sizes 16, 32, and 64 are accepted for v1. Durable names are `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and `bond/full-key/v1-b64`.

The writer places complete user keys in `PrefixBytes`. Physical common-prefix length is computed across adjacent complete keys, while logical `PrefixLen` always equals `comparer.Split(key)`. The seeker decodes exact bytes, uses bytewise lower-bound ordering, supports short bounds, and maintains independent scratch state per seeker.

Schema behavior behind a used name is immutable. Any algorithm, parser, or bundle change receives a new name.

## Registry and Writer Selection

Create the default schema and every Bond schema after comparer initialization. Validate unique names and ensure the active writer exists. Return a fresh Pebble registry map owned by options; callers cannot mutate the compatibility set after open.

Mixed schemas are normal. Changing the writer affects only future SSTs. Reader deletion is prohibited while live or restorable files may require the name.

## Backup and Inspection

Version backup metadata with `ReaderEpoch`, `FormatMajor`, and a stable sorted `RequiredKeySchema` list. Old metadata means the legacy/default requirement plus its recorded format. Restore checks format and schemas before any destination replacement. Inspection enumerates table properties and aggregates file count/bytes by durable name.

Every open path uses the centralized options builder. Direct ad hoc `pebble.Open` calls are either removed or limited to tests that intentionally verify invalid options.

## Test Plan

- `TestFullKeySchemaRoundTrip`: valid, empty, short, long, primary, secondary, and random keys decode exactly.
- `TestFullKeySchemaOrderingAndSeek`: exact/gap/boundary seeks equal a bytewise oracle.
- `TestFullKeySchemaSplit`: decoded and original `Comparer.Split` results match.
- `TestFullKeySchemaPrefixAndReverseIteration`: strict prefix and reverse scans match logical results.
- `TestFullKeySchemaConcurrentSeekers`: independent seekers do not alias mutable state.
- `TestFullKeySchemaCorruption`: truncated/corrupt blocks return errors without panic.
- Fuzz properties for round trip, ordering, seek, and split across every bundle size.
- `TestSchemaRegistryRejectsUnknownWriter` and `TestSchemaRegistryNamesImmutable`.
- `TestMixedSchemasReopenCompactRollback`: legacy and v1 files survive compaction and writer rollback.
- `TestBackupRequiredSchemas` and `TestRestoreRejectsMissingSchemaBeforeReplace`.
- `TestAllOpenPathsUseSchemaRegistry`: normal, migration, backup, restore, inspector, and benchmark paths receive the same registry.
