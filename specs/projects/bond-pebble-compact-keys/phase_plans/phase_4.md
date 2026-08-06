---
status: complete
---

# Phase 4: Centralize Storage Compatibility and Tooling

## Overview

This phase makes the measured Phase 3 no-go operationally explicit. Every production Bond open uses one centralized Pebble preparation path that fixes the comparer, `pebble.FormatNewest`, span policy, legacy/default active writer, and legacy/default-only reader registry. The rejected `bond/full-key/v1-*` implementations remain reachable only from controlled repository tests and benchmarks. Backups and their checkpoints gain optional, backward-compatible storage compatibility metadata; restores validate it before changing the destination. Storage diagnostics expose the active writer, registered readers, encountered SST schema files/bytes, unknown names, and the currently empty pre-catalog route set.

## Steps

1. Add production storage registry construction in `storage_compatibility.go`, deriving a fresh private legacy/default schema from each prepared `DefaultKeyComparer`, sorting reader names, assigning the current reader epoch, and rejecting every caller-supplied definition for that reserved durable name. The resource/profile builder leaves schema fields empty; controlled raw-Pebble harnesses configure their registry explicitly.
2. Add one production Pebble-open helper in `bond.go` and route normal opens and format migration through it. It clones caller options before enforcing `DefaultKeyComparer`, `PebbleDBFormat == pebble.FormatNewest`, `spanPolicyFunc`, and the production registry, so no production caller can activate, replace, or register an internal full-key candidate.
3. Store the effective production storage configuration on `_db` and expose database compatibility/diagnostic methods. Aggregate `DB.SSTables(pebble.WithProperties())` into stable schema file/byte counts, include the active and registered schema names, normalize old property-less legacy tables conservatively, and report unknown encountered names separately.
4. Add `StorageCompatibility` JSON sidecar helpers and validation for metadata version/reader epoch, format major, and sorted required schemas. Replace changed sidecars through a synced same-directory temporary file, atomic rename, and directory sync while eliding unchanged writes; unchanged retries still sync the directory so a previous post-rename sync failure cannot be masked. Older metadata with no storage field maps conservatively to the legacy/default reader and its existing Pebble format.
5. Extend `backup.BackupMeta` with an optional `storage_compatibility` field. After checkpoint creation, derive requirements from live SST properties plus the active writer, write the compatibility sidecar into the checkpoint, and carry the same data in remote and local backup metadata.
6. Reorder `backup.Restore` so it reads and validates the complete restore chain's format/schema requirements before cleaning an interrupted destination, creating directories, writing markers, or downloading files. Recreate the validated compatibility sidecar and retain old-backup support.
7. Add an offline `bond.InspectStorageDirectory` entry point and `bond-cli storage inspect --dir` command. Inspect SST properties across the pinned Pebble supported-format range without opening Pebble or initializing unavailable key seekers, using name-only placeholders to report unknown durable schemas, and emit bounded JSON diagnostics rather than application-controlled metric labels.
8. Keep full-key reader/writer configuration in `internal/fullkeyexperiment`. Convert any Bond production-open experiment to a rejection assertion and retain mixed legacy/full-key write, checkpoint, reopen, compaction, and rollback exercises on raw Pebble only inside controlled tests/benchmarks. Non-schema benchmark baselines install only Pebble's comparer-derived legacy reader; explicit schema candidates alone install experimental readers, and their required schemas come from encountered SST properties.
9. Update README and storage compatibility documentation with the legacy-only Phase 3 decision, reader-before-writer rule, backup/restore metadata, offline inspection command, and safe rollback semantics.
10. Run formatting, vet/build, and focused option/storage/checkpoint/backup/restore/CLI/mixed-schema tests serially. Defer the expensive repository-wide backup/restore and full race matrices to Phase 7 final verification; retain any broad result already completed during this phase without restarting it.

## Tests

- `TestProductionOpenRejectsNonProductionSchemas`: custom options cannot register or activate internal full-key candidates and fail before the database directory is created.
- `TestProductionOptionsAlwaysInstallRegistry`: nil, default, and customized production options all resolve to `FormatNewest`, the Bond comparer, legacy active writer, and exactly one legacy reader.
- `TestProductionPebbleOpenCallsAreCentralized`: production code has one direct `pebble.Open` call, used by normal open and migration after centralized option preparation.
- `TestProductionOpenRejectsNoncanonicalLegacySchema`: a different implementation under the durable legacy schema name is rejected before filesystem mutation.
- `TestProductionSchemasArePrivateWhileAnotherDBIsOpen`: prepared databases have distinct schema objects, and mutating a rejected caller object while another database is live cannot affect it or a future preparation.
- `TestProductionOpenDoesNotRewriteUnchangedStorageCompatibility`: an ordinary reopen leaves byte-identical sidecar metadata on the same file.
- Atomic sidecar tests verify a failure before rename preserves the previous target and a previously truncated target is replaced with valid JSON.
- `TestStorageCompatibilityRetriesDirectorySyncAfterRenameFailure`: a retry of byte-identical post-rename content repeats the previously failed directory sync.
- `TestStorageDiagnosticsReportsSchemasAndBytes`: active/registered/encountered schema names and per-schema file/byte totals are stable and accurate.
- `TestStorageCompatibilityValidation`: current/older epochs and formats are accepted when readable; future epochs, future formats, and missing schemas fail actionably.
- `TestBackupRequiredSchemas`: backup and checkpoint metadata contain the current reader epoch, exact format, and conservative required legacy schema.
- `TestRestoreRejectsInvalidStorageCompatibilityBeforeDestinationMutation`: unknown/missing schemas, unsupported old formats, and contradictory format fields fail before interrupted restore cleanup, marker creation, downloads, or replacement.
- `TestRestoreOldMetadataBackwardCompatible`: backups with the optional compatibility field omitted still restore and open with the legacy reader.
- `TestMixedSchemasReopenCompactRollback`: controlled internal options retain all experimental readers while switching the writer back to legacy across checkpoint/reopen/compaction; production options remain unchanged.
- `TestLifecycleOracleAndManifest`: a non-schema benchmark manifest records exactly one registered and required legacy schema, while candidate tests retain the explicit experimental registry.
- `TestFingerprintSourcesIncludesEveryBenchmarkDependency`: every declared benchmark-relevant Bond source, including `storage_compatibility.go`, independently changes the manifest source fingerprint.
- Storage and CLI tests verify `storage inspect` JSON includes active writer, registered readers, encountered SST usage, and an empty catalog route list, including for an SST whose schema implementation is unavailable to production and for a supported pre-migration Pebble format.
- `TestInspectStorageDirectoryReportsUnknownSchemaNameContainingDiagnosticDelimiter`: escape-aware panic parsing preserves a valid unusual schema name containing Pebble's diagnostic delimiter, quotes, and backslashes.
