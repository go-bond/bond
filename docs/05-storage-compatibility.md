# Storage compatibility and inspection

Bond uses one Pebble database and keeps logical keys unchanged. Pebble's physical `KeySchema` is selected when an SST is written; it is never an argument to Bond `Set`, `Delete`, table, index, or batch mutations.

## Current production schema

The Phase 3 `bond/full-key/v1-b16`, `bond/full-key/v1-b32`, and `bond/full-key/v1-b64` experiments did not meet the size gate and cannot represent Bond's supported empty point keys safely. They remain internal test/benchmark implementations only.

Production opens therefore use exactly:

- `pebble.FormatNewest` from the pinned Pebble commit (format 30);
- `DefaultKeyComparer()`;
- the comparer-derived Pebble legacy/default schema as the active writer; and
- that same legacy/default schema as the only registered reader.

Caller-supplied Pebble resource options are cloned, but they may not prepopulate the reserved durable legacy name, select a different writer, or add a schema reader. The lower-level resource/profile builder deliberately leaves schema fields empty. Normal opens and format migration pass through the production preparation path, which creates a fresh private comparer-derived schema and registry for each prepared options instance. No mutable schema object is shared between databases or returned resource options.

## Backup and restore metadata

Every new backup records optional `storage_compatibility` metadata:

```json
{
  "reader_epoch": 1,
  "format_major": 30,
  "required_key_schemas": [
    "DefaultKeySchema(leveldb.BytewiseComparator,16)"
  ]
}
```

The same document is stored as `bond/STORAGE_COMPATIBILITY.json` in the checkpoint. Required schemas include the sorted enabled reader registry plus names encountered in SST properties. This conservative set also covers WAL data that may flush after restore and avoids races with concurrent compaction. Compatibility metadata can evolve as the format or required-reader set changes, so Bond writes and syncs a uniquely created same-directory temporary file, atomically renames it over the old sidecar, and syncs the directory. Byte-identical metadata is not rewritten, but the directory sync is retried so an earlier interrupted publication can finish durably. Local mutable backup metadata, restored format sidecars, and format-migration sidecars use the same replacement discipline; initialization intent files remain no-replace publications.

Use `db.Checkpoint(path)` for operational checkpoints. It writes both the
Pebble format sidecar and storage compatibility sidecar; calling
`db.Backend().Checkpoint` directly only invokes Pebble and cannot add Bond's
compatibility metadata.

Restore downloads backup metadata and validates the complete chain before it cleans an interrupted destination, creates a marker, downloads a file, or writes format metadata. A future format, future reader epoch, or unknown required schema fails with the missing requirement and available reader list. Backups created before this optional field existed remain readable and conservatively require the legacy/default reader.

## Inspection

For a live database, call:

```go
diagnostics, err := db.StorageDiagnostics()
```

For a closed database:

```bash
bond-cli storage inspect --dir /path/to/database
```

Offline inspection does not open the database. It reads SST properties blocks directly and uses name-only placeholders for unavailable columnar schemas, so it can report an unknown durable schema name without initializing or executing that schema's key seeker.

Inspection accepts every Pebble format supported by the pinned binary, from `pebble.FormatMinSupported` through `PebbleDBFormat`. This permits diagnostics before an older database is migrated; production opens and migration targets remain pinned to `FormatNewest`.

The bounded JSON report contains the reader epoch, format, active writer, sorted registered readers, SST file/byte totals by encountered schema, unknown encountered names, and catalog routes. Phase 5 catalogs may describe future physical families, but production writer routing remains unsupported on stock pinned Pebble, so catalog routes stay empty until that feasibility work succeeds. Full stored names belong in diagnostics rather than application-controlled metric labels. See [Declarative catalog and API migration](./06-declarative-catalog.md) for the descriptor-versus-runtime distinction.

## Rollout and rollback rule

A future schema must ship reader support before any writer activation. Rollback means selecting the older writer while retaining every reader required by live, checkpointed, backed-up, or ingestible SSTs. Removing a newer reader or downgrading to a binary without it is not rollback.
