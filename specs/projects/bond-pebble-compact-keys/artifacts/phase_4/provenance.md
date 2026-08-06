# Phase 4 provenance and status audit

## Immutable dependency and format gates

| Item | Observed value |
| --- | --- |
| Bond implementation base | `f9aa67c43a31abfb4fa0e39238a44717ecf91e91` |
| Pebble module | `github.com/cockroachdb/pebble v0.0.0-20260707124150-8fb150d9135d` |
| approved Pebble hash | pseudo-version suffix resolves to exact `8fb150d9135d6f94e183a874475e0bd1afb18f63` |
| workspace override | none (`go env GOWORK` empty) |
| module replace | none |
| production format | `PebbleDBFormat == pebble.FormatNewest == 30` |
| production comparer | `leveldb.BytewiseComparator` with Bond `Split` |
| production active writer | `DefaultKeySchema(leveldb.BytewiseComparator,16)` |
| production registered readers | only `DefaultKeySchema(leveldb.BytewiseComparator,16)` |
| storage reader epoch | `1` |

## Production exposure audit

- `options.go`, `bond.go`, `storage_compatibility.go`, `backup/`, and `cmd/` contain no `bond/full-key/v1-*` constructor, writer selector, or production reader registration.
- Each prepared production options instance owns a fresh `colblk.DefaultKeySchema(DefaultKeyComparer(), 16)` and reader map. The resource/profile builder exposes no schema object, and every caller-prepopulated definition under the reserved durable name is rejected before open.
- Rejected full-key names remain in `internal/fullkeyexperiment`, repository tests, the controlled benchmark harness, documentation of the no-go, and a negative restore test only.
- `PebbleOptionsConfig` exposes performance/compression/filter dimensions but no writer-schema field.
- Caller options are cloned; a non-production active writer, additional reader, or any caller-prepopulated reserved legacy name is rejected before filesystem mutation, and the caller's options are not modified.
- The production source audit finds one direct `pebble.Open`, inside `bond.go:openPreparedPebble`. The benchmark harness and controlled tests retain their explicitly isolated raw-Pebble opens.
- `bond.Open`, `MigratePebbleFormatVersion`, and `InspectStorageDirectory` use `productionPebbleOptions`; backup uses `db.Checkpoint`; restore validates metadata without opening the destination.
- Normal open and migration validate an existing compatibility sidecar before Pebble open. Offline inspection reads SST properties directly with the production registry plus name-only placeholders, allowing unknown schema names to be reported without initializing unavailable key seekers.

## Compatibility metadata audit

- New database opens and Bond checkpoints write deterministic `bond/STORAGE_COMPATIBILITY.json` metadata.
- Changed compatibility sidecars use a synced same-directory temporary file, atomic rename, and directory sync. Byte-identical ordinary-open metadata is not rewritten, but its directory is synced again so retry can complete durability after a post-rename sync failure.
- New backup `meta.json` files carry an optional nested `storage_compatibility` object, preserving JSON decoding for older metadata.
- Required schemas conservatively union the enabled reader registry and encountered SST names, sorted and deduplicated. This covers WAL replay and compaction races.
- Metadata with no compatibility object maps to reader epoch 0 plus its existing Pebble format and the legacy/default requirement.
- Restore reads and validates every metadata record in the selected chain before any destination cleanup or creation, then writes the validated final compatibility document.
- Diagnostics aggregate bounded files and SST bytes by durable schema name (Pebble logical sizes live, physical file sizes offline); older property-less tables map conservatively to the legacy/default reader.
- Offline inspection accepts the pinned Pebble range from `FormatMinSupported` through `PebbleDBFormat`, allowing diagnostics before migration while production opens remain pinned to `FormatNewest`.
- Non-schema compact-key baselines call Pebble defaults after the resource builder and therefore record exactly one legacy reader. Explicit schema candidates alone install internal experimental readers, and benchmark compatibility requirements are derived from encountered SST properties rather than the entire controlled reader registry.
- Manifest source fingerprints include `storage_compatibility.go` alongside key encoding, options, internal schema experiments, module resolution, and the benchmark harness; focused mutation coverage proves each declared source input changes the digest.

## Worktree status

Phase 4 changes are intentionally uncommitted and the implementation-plan checkbox and phase-plan `status: draft` remain unchanged pending code review and commit approval. No Pebble source, fork, workspace, or module replacement was modified.
