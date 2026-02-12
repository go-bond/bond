# Backup to Object Storage via Pebble Checkpoint

## Overview

The `backup/` sub-package provides full database backup and restore via Pebble's `Checkpoint`. It uploads checkpoints to object storage (S3, GCS, etc.) through a minimal `Bucket` abstraction, supports complete and true incremental (diff-based) backups, and restores by finding the latest complete backup and applying all subsequent incrementals in order.

All code lives in `bond/backup` to keep the bond core free of object storage dependencies.

---

## 1. Design Decisions

### 1.1 Custom Bucket interface

A minimal 5-method `Bucket` interface is defined in `backup/`. Go structural typing means thanos `objstore.Bucket` (or any compatible implementation) can be used directly without importing thanos and its transitive dependencies (prometheus, YAML, etc.).

```go
type Bucket interface {
    Upload(ctx context.Context, name string, r io.Reader) error
    Get(ctx context.Context, name string) (io.ReadCloser, error)
    Iter(ctx context.Context, dir string, f func(name string) error) error
    Exists(ctx context.Context, name string) (bool, error)
    Delete(ctx context.Context, name string) error
}
```

An `InMemBucket` implementation (thread-safe `map[string][]byte`) is included for testing.

### 1.2 Standalone functions

Backup/restore are stateless operations implemented as standalone functions:

- `Backup(ctx, db, bucket, opts) (*BackupMeta, error)`
- `Restore(ctx, bucket, opts) error`
- `ListBackups(ctx, bucket, prefix) ([]BackupInfo, error)`
- `FindRestoreSet(ctx, bucket, prefix, before) ([]BackupInfo, error)`

### 1.3 True incremental backups (diff-based)

- **Complete backup**: Takes a Pebble checkpoint, uploads ALL files.
- **Incremental backup**: Takes a Pebble checkpoint, compares against the previous backup's file list (from its `meta.json`), uploads only **new or changed** files (compared by name + size).
- **Restore**: Finds the latest complete backup, downloads all its files, then applies each subsequent incremental in datetime order (downloading and overwriting/adding files).

This works because:
- Pebble SST files are immutable with unique numbered names (same name = same content).
- MANIFEST, CURRENT, OPTIONS, and WAL files may change between checkpoints and are always uploaded in incrementals.
- On restore, orphaned SST files don't cause issues since Pebble only reads files referenced by the MANIFEST.

### 1.4 meta.json tracks both uploaded files and full checkpoint state

Each backup's `meta.json` contains:
- `files`: the files uploaded in THIS backup (what to download for this backup)
- `checkpoint_files`: ALL files in the checkpoint at this point in time (used by the next incremental to compute its diff)

This way, the next incremental can read the previous backup's `checkpoint_files` to determine what's new/changed without reconstructing state from the full chain.

### 1.5 Bond metadata preservation

Pebble Checkpoint doesn't include `bond/PEBBLE_FORMAT_VERSION`. During backup, the Pebble format version is read via `db.Backend().FormatMajorVersion()` and stored in `meta.json`. During restore, the file is recreated so `bond.Open()` succeeds. The bond data version (stored as a Pebble key) comes for free in the checkpoint.

---

## 2. Backup Layout in Object Store

All keys live under a configurable **prefix** (e.g. `backups/`).

- **Complete backup**
  `{prefix}/{datetime}-complete/{files}`
  Example: `backups/20250212120000-complete/MANIFEST-000001`, `backups/20250212120000-complete/000001.sst`, ...

- **Incremental backup**
  `{prefix}/{datetime}-incremental/{files}`
  Example: `backups/20250212130000-incremental/MANIFEST-000002`, `backups/20250212130000-incremental/000003.sst`, ...

- **Metadata**
  `{prefix}/{datetime}-{type}/meta.json`

**Datetime format**: `20060102150405` (fixed-length, lexicographically sortable, UTC).

---

## 3. File Layout

All new files in `backup/` -- no changes to existing bond files.

```
backup/
  backup.go            -- Bucket interface, BackupOptions, Backup()
  restore.go           -- RestoreOptions, Restore()
  list.go              -- ListBackups(), FindRestoreSet(), BackupInfo
  meta.go              -- BackupMeta, FileInfo, JSON marshal/unmarshal helpers
  naming.go            -- Datetime formatting, backup dir naming helpers
  bucket_inmem.go      -- InMemBucket for testing
  backup_test.go       -- Tests for backup + restore + list + integration
```

---

## 4. API Reference

### BackupOptions

```go
type BackupOptions struct {
    Prefix string      // Object storage prefix (e.g. "backups")
    Type   BackupType  // BackupTypeComplete or BackupTypeIncremental
    At     time.Time   // Override backup timestamp; zero = time.Now().UTC()
}
```

### RestoreOptions

```go
type RestoreOptions struct {
    Prefix     string    // Object storage prefix where backups are stored
    RestoreDir string    // Local directory to restore into (must be empty or non-existent)
    Before     time.Time // Point-in-time cutoff; zero = all backups
}
```

### BackupMeta

```go
type BackupMeta struct {
    Type                BackupType `json:"type"`                  // "complete" or "incremental"
    Datetime            string     `json:"datetime"`              // "20060102150405"
    PebbleFormatVersion uint64     `json:"pebble_format_version"`
    BondDataVersion     uint32     `json:"bond_data_version"`
    Files               []FileInfo `json:"files"`                 // files uploaded in THIS backup
    CheckpointFiles     []FileInfo `json:"checkpoint_files"`      // ALL files in the checkpoint
    CreatedAt           string     `json:"created_at"`            // RFC3339
}
```

### Functions

```go
func Backup(ctx context.Context, db bond.DB, bucket Bucket, opts BackupOptions) (*BackupMeta, error)
func Restore(ctx context.Context, bucket Bucket, opts RestoreOptions) error
func ListBackups(ctx context.Context, bucket Bucket, prefix string) ([]BackupInfo, error)
func FindRestoreSet(ctx context.Context, bucket Bucket, prefix string, before time.Time) ([]BackupInfo, error)
```

---

## 5. Implementation Details

### 5.1 Complete backup flow

1. Determine datetime (`opts.At` or `time.Now().UTC()`)
2. `os.MkdirTemp("", "bond-backup-*")` + `defer os.RemoveAll(tempDir)`
3. `db.Backend().Checkpoint(checkpointDir)` into a subdirectory of tempDir
4. `filepath.WalkDir(checkpointDir, ...)` to collect all checkpoint files
5. Upload ALL files to `{prefix}/{datetime}-complete/{relativePath}`
6. Build `BackupMeta` where `Files == CheckpointFiles` (all files)
7. Upload `meta.json`, return `*BackupMeta`

### 5.2 Incremental backup flow

1. Same steps 1-4 (take a full checkpoint locally)
2. Find the previous backup: `ListBackups` -> take the last one -> `readMeta`
3. Build a set of `{name: size}` from the previous backup's `CheckpointFiles`
4. Diff: for each file in the new checkpoint, if it's not in the previous set OR has a different size, mark as "to upload"
5. Upload only the diff files to `{prefix}/{datetime}-incremental/{relativePath}`
6. Build `BackupMeta` where:
   - `Files` = only the diff files (what was uploaded)
   - `CheckpointFiles` = ALL files in the new checkpoint (for the next incremental's diff)
7. Upload `meta.json`, return `*BackupMeta`

### 5.3 Restore flow

1. Validate `RestoreDir` is empty or doesn't exist
2. `FindRestoreSet(ctx, bucket, opts.Prefix, opts.Before)` -> `[complete, incr1, incr2, ...]`
3. If no backups found, return `ErrNoBackupsFound`
4. Create `RestoreDir` if needed
5. For each backup in order:
   a. `readMeta(ctx, bucket, backup.Prefix)` to get the `Files` list
   b. For each file in `meta.Files`: download from bucket, write to `filepath.Join(opts.RestoreDir, file.Name)`, creating parent dirs as needed
   c. Each incremental overwrites changed files (MANIFEST, CURRENT, etc.) and adds new SSTs
6. Recreate bond metadata from the **last** backup's meta:
   - `os.MkdirAll(filepath.Join(restoreDir, "bond"), 0755)`
   - Write `bond/PEBBLE_FORMAT_VERSION` with the stored `PebbleFormatVersion` via `utils.WriteFileWithSync`
7. Caller then opens with `bond.Open(restoreDir, opts)`

### 5.4 ListBackups and FindRestoreSet

- `ListBackups`: iterates bucket with `Iter(ctx, prefix, ...)`, parses each entry with `parseBackupDir`, returns sorted by datetime ascending.
- `FindRestoreSet`: calls `ListBackups`, filters by `before` time, finds the latest complete backup at or before the cutoff, returns `[complete, incr1, incr2, ...]`.

---

## 6. Testing

All tests use `InMemBucket` and `t.TempDir()`. Tests implemented:

| Test | What it verifies |
|------|-----------------|
| `TestNamingHelpers` | `backupDirName`, `parseBackupDir` round-trips, edge cases |
| `TestInMemBucket` | Upload/Get/Iter/Exists/Delete correctness |
| `TestBackupComplete` | Insert data, backup, verify objects in bucket and meta.json content |
| `TestRestoreComplete` | Backup, restore to new dir, `bond.Open`, compare all key-value pairs |
| `TestBackupIncremental` | Complete + incremental, verify fewer files uploaded in incremental |
| `TestRestoreWithIncrementals` | Complete -> insert -> incr1 -> insert -> incr2 -> restore -> verify ALL data |
| `TestRestoreBeforeTime` | Multiple timed backups, restore to specific point, verify correct data |
| `TestListBackups` | Multiple backups, verify sorted order and correct types |
| `TestFindRestoreSet` | Verify correct [complete + incrementals] chain, and `ErrNoBackupsFound` |
| `TestBackupEmptyDB` | Backup/restore empty database |
| `TestRestoreCreatesMetadata` | Verify `bond/PEBBLE_FORMAT_VERSION` exists and is correct after restore |
| `TestRestoreNonEmptyDir` | Error when restore dir is not empty |
| `TestIncrementalWithoutPreviousBackup` | Error when no previous backup exists |

---

## 7. Key Files Referenced

| File | What is used |
|------|-------------|
| `bond.go:53` | `db.Backend()` -> `*pebble.DB` for `Checkpoint()` and `FormatMajorVersion()` |
| `bond.go:35` | `PebbleFormatFile` constant (`"PEBBLE_FORMAT_VERSION"`) |
| `version.go:12` | `BOND_DB_DATA_VERSION` constant (currently `1`) |
| `utils/file.go:8` | `WriteFileWithSync()` for safe file writes during restore |
| `options.go:18` | `PebbleDBFormat` -- the expected format version value |

---

## 8. Edge Cases

- **Empty DB**: Checkpoint is valid; restore produces a DB with only the bond data version key.
- **Concurrent writes**: Checkpoint is a point-in-time snapshot. All committed writes prior to the checkpoint call are included.
- **Non-empty restore dir**: Returns an error.
- **Incremental with no previous backup**: Returns an error.
- **Bond metadata**: Recreated during restore from `meta.json`'s `pebble_format_version` field.
- **Orphaned SSTs after restore**: Pebble only reads files referenced by the MANIFEST, so leftover SSTs from earlier complete backups are harmless.

---

## 9. Usage Example

```go
import (
    "context"
    "github.com/go-bond/bond"
    "github.com/go-bond/bond/backup"
)

// Complete backup
db, _ := bond.Open("mydb", bond.DefaultOptions())
bucket := // ... your Bucket implementation (S3, GCS, InMemBucket, etc.)

meta, err := backup.Backup(ctx, db, bucket, backup.BackupOptions{
    Prefix: "backups",
    Type:   backup.BackupTypeComplete,
})

// Later: incremental backup
meta, err = backup.Backup(ctx, db, bucket, backup.BackupOptions{
    Prefix: "backups",
    Type:   backup.BackupTypeIncremental,
})

// Restore
err = backup.Restore(ctx, bucket, backup.RestoreOptions{
    Prefix:     "backups",
    RestoreDir: "/path/to/restored",
})
restoredDB, _ := bond.Open("/path/to/restored", bond.DefaultOptions())
```
