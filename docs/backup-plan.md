# Backup to Object Storage via Pebble Checkpoint

## Overview

The `backup/` sub-package provides full database backup and restore via Pebble's `Checkpoint`. It uploads checkpoints to object storage (S3, GCS, Azure, etc.) through the `objstore.Bucket` abstraction from [thanos-io/objstore](https://github.com/thanos-io/objstore), supports complete and true incremental (diff-based) backups, and restores by finding the latest complete backup and applying all subsequent incrementals in order.

All code lives in `bond/backup` to keep the bond core free of object storage dependencies.

---

## 1. Design Decisions

### 1.1 thanos-io/objstore as storage abstraction

The package uses `objstore.Bucket` from [thanos-io/objstore](https://github.com/thanos-io/objstore) directly as the storage interface. This provides production-ready implementations for S3, GCS, Azure, COS, Swift, and filesystem out of the box. The `objstore` dependency was promoted from indirect to direct.

For testing, the package uses `objstore.NewInMemBucket()` — a thread-safe in-memory bucket provided by the objstore library.

### 1.2 Standalone functions

Backup/restore are stateless operations implemented as standalone functions:

- `Backup(ctx, db, bucket, opts) (*BackupMeta, error)`
- `Restore(ctx, bucket, opts) error`
- `ListBackups(ctx, bucket, prefix) ([]BackupInfo, error)`
- `FindRestoreSet(ctx, bucket, prefix, before) ([]BackupInfo, error)`
- `RemoveOrphaned(ctx, bucket, prefix) (int, error)`
- `HasCheckpoint(dir) (bool, error)`
- `RemoveCheckpoint(dir) error`

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

Pebble Checkpoint doesn't include `bond/PEBBLE_FORMAT_VERSION`. During backup, the Pebble format version is read via `db.Backend().FormatMajorVersion()` and stored in `meta.json`. During restore, the file is recreated using `utils.WriteFileWithSync` so `bond.Open()` succeeds. The bond data version (`BOND_DB_DATA_VERSION` constant) is also stored in `meta.json` and comes for free in the checkpoint as a Pebble key.

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

- **Lock file**
  `{prefix}/.lock`

**Datetime format**: `20060102150405` (fixed-length, lexicographically sortable, UTC).

---

## 3. File Layout

All new files in `backup/` -- no changes to existing bond files.

```
backup/
  backup.go            -- BackupOptions, Backup(), computeIncrementalFiles()
  checkpoint.go        -- HasCheckpoint(), RemoveCheckpoint()
  restore.go           -- RestoreOptions, Restore()
  list.go              -- ListBackups(), FindRestoreSet(), BackupInfo, ErrNoBackupsFound
  meta.go              -- BackupMeta, FileInfo, writeMeta(), readMeta(), newBackupMeta()
  naming.go            -- BackupType, datetimeFormat, backupDirName(), backupObjectPrefix(), parseBackupDir()
  progress.go          -- DefaultConcurrency, ProgressEvent, ProgressFunc
  orphan.go            -- isOrphaned(), deleteBackupDir(), removeOrphanedDirs(), RemoveOrphaned()
  lock.go              -- acquireLock(), releaseLock(), startLockRefresh(), ErrBackupInProgress, DefaultLockTTL
  backup_test.go       -- Unit tests for backup, restore, list, naming, orphans, locking, checkpoint, edge cases

tests/
  backup_test.go       -- Integration tests: table-level backup/restore with Bond Table API
```

---

## 4. API Reference

### BackupOptions

```go
type BackupOptions struct {
    Prefix        string        // Object storage prefix (e.g. "backups")
    Type          BackupType    // BackupTypeComplete or BackupTypeIncremental
    At            time.Time     // Override backup timestamp; zero = time.Now().UTC()
    Concurrency   int           // Parallel uploads; <= 0 uses DefaultConcurrency
    OnProgress    ProgressFunc  // Called after each file upload; must be goroutine-safe
    LockTTL       time.Duration // Max age of backup lock before stale; zero = DefaultLockTTL (1h)
    CheckpointDir string        // Directory for Pebble checkpoint; must be on same filesystem as DB. Required.
    RateLimit     float64       // Aggregate upload rate limit in bytes/sec; zero = DefaultRateLimit (100 MB/s); negative disables
}
```

### RestoreOptions

```go
type RestoreOptions struct {
    Prefix      string        // Object storage prefix where backups are stored
    RestoreDir  string        // Local directory to restore into (must be empty or non-existent)
    Before      time.Time     // Point-in-time cutoff; zero = all backups
    Concurrency int           // Parallel downloads per stage; <= 0 uses DefaultConcurrency
    OnProgress  ProgressFunc  // Called after each file download; must be goroutine-safe
    RateLimit   float64       // Aggregate download rate limit in bytes/sec; zero = DefaultRateLimit (100 MB/s); negative disables
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

### FileInfo

```go
type FileInfo struct {
    Name string `json:"name"`
    Size int64  `json:"size"`
}
```

### BackupInfo

```go
type BackupInfo struct {
    Datetime time.Time
    Type     BackupType
    Prefix   string
}
```

### BackupType

```go
type BackupType string

const (
    BackupTypeComplete    BackupType = "complete"
    BackupTypeIncremental BackupType = "incremental"
)
```

### Functions

```go
func Backup(ctx context.Context, db bond.DB, bucket objstore.Bucket, opts BackupOptions) (*BackupMeta, error)
func Restore(ctx context.Context, bucket objstore.Bucket, opts RestoreOptions) error
func ListBackups(ctx context.Context, bucket objstore.Bucket, prefix string) ([]BackupInfo, error)
func FindRestoreSet(ctx context.Context, bucket objstore.Bucket, prefix string, before time.Time) ([]BackupInfo, error)
func RemoveOrphaned(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error)
func HasCheckpoint(dir string) (bool, error)
func RemoveCheckpoint(dir string) error
```

### Constants

```go
const DefaultConcurrency = 4
const DefaultRateLimit float64 = 100 * 1024 * 1024 // 100 MB/s
const DefaultLockTTL = 1 * time.Hour
```

### Sentinel Errors

```go
var ErrNoBackupsFound = fmt.Errorf("no backups found")
var ErrBackupInProgress = fmt.Errorf("another backup is in progress")
```

---

## 5. Implementation Details

### 5.1 Complete backup flow

1. Default `opts.Type` to `BackupTypeComplete` if empty
2. Acquire lock via `acquireLock(ctx, bucket, opts.Prefix, ttl)`; fail fast with `ErrBackupInProgress` if held
3. Start lock refresh goroutine via `startLockRefresh()`; defer `stopRefresh()` and `releaseLock()`
4. Remove orphaned backup dirs via `removeOrphanedDirs(ctx, bucket, opts.Prefix)`
5. Determine datetime (`opts.At` or `time.Now().UTC()`), ensure UTC
6. Compute object prefix via `backupObjectPrefix(opts.Prefix, dt, opts.Type)`
7. Validate `opts.CheckpointDir` is non-empty (required)
8. Remove any stale checkpoint at `opts.CheckpointDir` via `os.RemoveAll` + `defer os.RemoveAll(opts.CheckpointDir)` for cleanup
9. `db.Backend().Checkpoint(opts.CheckpointDir)` into the caller-provided directory
10. `filepath.WalkDir(opts.CheckpointDir, ...)` to collect all checkpoint files as `[]FileInfo{Name, Size}`
11. Upload ALL files in parallel using `errgroup` with configurable concurrency (`opts.Concurrency`, default `DefaultConcurrency`), calling `opts.OnProgress` after each file
12. Read `pebble_format_version` via `db.Backend().FormatMajorVersion()` and `bond_data_version` via `bond.BOND_DB_DATA_VERSION`
13. Build `BackupMeta` via `newBackupMeta()` where `Files == CheckpointFiles` (all files)
14. Upload `meta.json` via `writeMeta()`, return `*BackupMeta`

### 5.2 Incremental backup flow

1. Same steps 1-10 as complete (lock, orphan cleanup, checkpoint dir validation, checkpoint)
2. Call `computeIncrementalFiles()` which:
   a. `ListBackups()` -> take the last one -> `readMeta()`
   b. Build a set of `{name: size}` from the previous backup's `CheckpointFiles`
   c. Diff: for each file in the new checkpoint, if it's not in the previous set OR has a different size, mark as "to upload"
3. If no previous backup exists, return error: `"no previous backup found; cannot create incremental backup"`
4. Upload only the diff files in parallel to `{prefix}/{datetime}-incremental/{relativePath}`
5. Build `BackupMeta` where:
   - `Files` = only the diff files (what was uploaded)
   - `CheckpointFiles` = ALL files in the new checkpoint (for the next incremental's diff)
6. Upload `meta.json`, return `*BackupMeta`

### 5.3 Restore flow

1. Validate `RestoreDir` is specified and is empty or doesn't exist
2. If `opts.Before` is zero, set it to `time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)` (effectively "all backups")
3. `FindRestoreSet(ctx, bucket, opts.Prefix, before)` -> `[complete, incr1, incr2, ...]`
4. If no backups found, return `ErrNoBackupsFound`
5. Create `RestoreDir` via `os.MkdirAll`
6. Pre-read all metas to compute global totals for progress reporting (`totalFiles`, `totalBytes`)
7. Pre-create all needed subdirectories upfront to avoid concurrent `MkdirAll` calls
8. For each backup in order (sequential outer loop preserves incremental override semantics):
   a. Download files in parallel using `errgroup` with configurable concurrency (`opts.Concurrency`, default `DefaultConcurrency`)
   b. For each file in `meta.Files`: download via `bucket.Get()`, read all bytes via `io.ReadAll`, write to `filepath.Join(opts.RestoreDir, file.Name)` via `os.WriteFile`
   c. Call `opts.OnProgress` after each file with cumulative counters across all stages
   d. Each incremental overwrites changed files (MANIFEST, CURRENT, etc.) and adds new SSTs
9. Recreate bond metadata from the **last** backup's meta:
   - `os.MkdirAll(filepath.Join(restoreDir, "bond"), 0755)`
   - Write `bond/PEBBLE_FORMAT_VERSION` with the stored `PebbleFormatVersion` via `utils.WriteFileWithSync`
10. Caller then opens with `bond.Open(restoreDir, opts)`

### 5.4 ListBackups and FindRestoreSet

- `ListBackups`: calls `bucket.Iter(ctx, prefix, ...)`, auto-appends `/` to prefix if missing, parses each entry with `parseBackupDir` (skips non-matching entries), skips orphaned dirs via `isOrphaned()`, returns sorted by datetime ascending.
- `FindRestoreSet`: calls `ListBackups`, filters to backups at or before `before` (UTC), finds the latest complete backup in the filtered set by scanning backwards, returns `[complete, incr1, incr2, ...]`. Returns `ErrNoBackupsFound` if no complete backup exists at or before the cutoff.

### 5.5 Naming helpers

- `backupDirName(t, bt)` -> `"20250212120000-complete"` (datetime format + hyphen + type)
- `backupObjectPrefix(prefix, t, bt)` -> `"backups/20250212120000-complete/"` (with trailing slash)
- `parseBackupDir(name)` -> `(time.Time, BackupType, error)`: strips trailing `/`, takes `path.Base()`, splits on first `-`, parses datetime and validates type

### 5.6 Orphan handling

An orphaned backup is a directory matching `{datetime}-{type}/` that has no `meta.json` file. This can happen if `Backup()` is interrupted after uploading checkpoint files but before writing `meta.json`.

- **Detection**: `isOrphaned(ctx, bucket, backupPrefix)` checks `bucket.Exists()` for `meta.json`.
- **Cleanup**: `deleteBackupDir(ctx, bucket, dirPrefix)` discovers all objects via `bucket.Iter()` with `WithRecursiveIter()`, then deletes each. Guards against concurrent deletion with `bucket.IsObjNotFoundErr()`.
- **`removeOrphanedDirs(ctx, bucket, prefix)`**: Iterates all dirs under prefix, identifies orphans, deletes them. Returns count removed.
- **`RemoveOrphaned(ctx, bucket, prefix)`**: Public API that delegates to `removeOrphanedDirs`.
- **`ListBackups()`**: Skips orphaned dirs automatically via `isOrphaned()` check in the `Iter` callback. This means `FindRestoreSet()` and `Restore()` benefit from orphan skipping with no additional changes.
- **`Backup()`**: Calls `removeOrphanedDirs()` at the start (after acquiring the lock) to clean up orphans before proceeding.
- **`Restore()`**: Does **not** clean up orphans. It simply skips them via `ListBackups()`.

### 5.7 Backup locking

A lock prevents concurrent `Backup()` calls from running against the same prefix. The lock is a JSON file at `{prefix}/.lock` containing `{"created_at":"<RFC3339>"}`.

- **`acquireLock(ctx, bucket, prefix, ttl)`**: Checks if lock exists. If it does and its age < TTL, returns `ErrBackupInProgress`. If age >= TTL (stale) or lock doesn't exist, uploads a new lock.
- **`releaseLock(ctx, bucket, prefix)`**: Deletes the lock. Ignores not-found errors.
- **`startLockRefresh(ctx, bucket, prefix, ttl)`**: Starts a background goroutine that re-uploads the lock every `ttl/2` to prevent long-running backups from having their lock considered stale. Returns a `stop` function.
- **`Backup()` integration**: Acquires lock, starts refresh, defers both `stopRefresh()` and `releaseLock()`, then proceeds with orphan cleanup and backup. The lock is always released on both success and error.
- **`Restore()`**: Does **not** acquire a lock. Restore is read-only with respect to the backup set.

---

## 6. Testing

### 6.1 Unit tests (`backup/backup_test.go`)

All tests use `objstore.NewInMemBucket()` and `t.TempDir()`. Helper functions:
- `openTestDB(t, dir)` — opens a bond DB with `MediumPerformance` options
- `insertTestData(t, db, start, count)` — inserts raw key-value pairs using `bond.KeyEncode`
- `collectAllKVs(t, db)` — iterates all keys and returns `map[string]string`

| Test | What it verifies |
|------|-----------------|
| `TestNamingHelpers` | `backupDirName`, `backupObjectPrefix`, `parseBackupDir` round-trips; path prefix handling; trailing slash handling; invalid input errors |
| `TestBackupComplete` | Insert 10 records, backup, verify meta fields (Type, Datetime, Files == CheckpointFiles, PebbleFormatVersion > 0, BondDataVersion), verify `meta.json` exists in bucket, verify uploaded objects |
| `TestRestoreComplete` | Backup 20 records, restore to new dir, `bond.Open`, compare all key-value pairs match original |
| `TestBackupIncremental` | Complete + insert more + incremental: verify incremental has fewer `Files` than complete, but `CheckpointFiles` >= complete's |
| `TestRestoreWithIncrementals` | Complete -> insert -> incr1 -> insert -> incr2 -> restore -> verify ALL data from all phases |
| `TestRestoreBeforeTime` | 3 timed backups (complete + 2 incrementals), restore with `Before=12:30` (after complete, before first incremental), verify only phase 1 data |
| `TestListBackups` | 3 backups (complete + 2 incrementals), verify sorted order and correct types |
| `TestFindRestoreSet` | Verify correct [complete + incrementals] chain for different cutoff times, and `ErrNoBackupsFound` when cutoff is before all backups |
| `TestBackupEmptyDB` | Backup/restore empty database; verify checkpoint files exist (MANIFEST, CURRENT, etc.); restored DB has only bond data version key |
| `TestRestoreCreatesMetadata` | Verify `bond/PEBBLE_FORMAT_VERSION` file exists after restore and contains correct format version |
| `TestRestoreNonEmptyDir` | Error when restore dir is not empty; error message contains "not empty" |
| `TestIncrementalWithoutPreviousBackup` | Error when attempting incremental with no previous backup; error message contains "no previous backup" |
| `TestBackupProgress` | Verify one `ProgressEvent` per uploaded file, all file names appear, max `FilesDone` == total, max `BytesDone` == total |
| `TestRestoreProgress` | Verify progress events span all backups in restore set, max counters match totals |
| `TestBackupProgressCancellation` | Cancel context in progress callback after 2 files; verify `context.Canceled` error and fewer than total files processed |
| `TestConcurrencyOne` | Backup + restore with `Concurrency: 1`; verify data integrity |
| `TestDefaultConcurrency` | Backup + restore with `Concurrency: 0` (uses `DefaultConcurrency`); verify data integrity |
| `TestListBackups_SkipsOrphaned` | Valid backup + orphaned dir → `ListBackups` returns only valid |
| `TestRemoveOrphaned` | Two orphaned dirs + one valid → `RemoveOrphaned` removes 2, valid remains |
| `TestRemoveOrphaned_NoOrphans` | No orphans → returns 0, no error |
| `TestBackup_CleansOrphanedBeforeIncremental` | Valid complete + orphaned incremental as "latest" → new incremental succeeds, orphan removed |
| `TestRestore_SkipsOrphaned` | Valid complete + orphaned incremental → restore succeeds, data correct, orphan still exists (not cleaned) |
| `TestBackup_LockPreventsConcurrent` | Acquire lock manually, attempt `Backup` → returns `ErrBackupInProgress` |
| `TestBackup_StaleLockIsOverridden` | Upload a lock with old timestamp, `Backup` with short TTL → succeeds (stale lock overridden) |
| `TestBackup_LockReleasedOnError` | Backup that errors (canceled ctx after lock) → lock is released via defer |
| `TestBackup_LockReleasedOnSuccess` | Successful backup → lock object no longer exists afterward |
| `TestHasCheckpoint_NoCheckpoint` | Returns `false` when directory does not exist |
| `TestHasCheckpoint_WithCheckpoint` | Returns `true` after creating a directory |
| `TestRemoveCheckpoint` | Creates dir with files, removes it, verifies gone |
| `TestRemoveCheckpoint_NoCheckpoint` | Idempotent — no error when directory does not exist |
| `TestBackup_CleansStaleCheckpoint` | Stale checkpoint cleaned before backup, checkpoint cleaned after |

### 6.2 Integration tests (`tests/backup_test.go`)

Integration tests in the `bond_tests` package exercise backup/restore through the Bond Table API (not raw key-value). They use two table types:

- `TokenBalance` (TableID `0xC0`) — with fields: ID, AccountID, ContractAddress, AccountAddress, TokenID, Balance
- `Token` (TableID `0xC1`) — with fields: ID, Name

Helper functions:
- `setupTokenBalanceTable(db)` / `setupTokenTable(db)` — create table definitions with primary key functions
- `newAccountAddressIndex()` — creates a secondary index on `AccountAddress` (IndexID `1`)
- `insertTokenBalances(t, ctx, table, start, count)` / `insertTokens(t, ctx, table, start, count)` — insert records via `table.Insert`

| Test | What it verifies |
|------|-----------------|
| `TestBackupRestore_TableData` | Complete backup + restore with 10 TokenBalance records. Verifies via `Scan` (10 records), `GetPoint` (specific record fields), and `Query().Limit(5)` (5 results) |
| `TestBackupRestore_SecondaryIndexes` | Table with secondary index on `AccountAddress`, 20 records (i%5 -> 4 per address). After restore, recreates index and verifies `Query().With(idx, SelectorPoint)` returns 4 records, and `ScanIndex` returns 4 records for a different address |
| `TestBackupRestore_MultipleTables` | Two tables (TokenBalance + Token), 10 records each. After restore, verifies both tables via `Scan` and `GetPoint` |
| `TestBackupRestore_Incremental` | Table with secondary index. Complete backup (10 records) + incremental (10 more). Restore and verify all 20 records via `Scan`, `GetPoint` for both phases, and index query spanning both phases |
| `TestBackupRestore_PointInTime` | 3-phase backup (complete + 2 incrementals, 5 records each). Restore with `Before=12:30` (only complete). Verify 5 records via `Scan`, and `GetPoint` for phase 2 record returns `bond.ErrNotFound` |
| `TestBackupRestore_BatchInsert` | Records inserted via `db.Batch(BatchTypeWriteOnly)` + `batch.Commit(Sync)`. Complete backup + restore. Verify via `Scan`, index query, and `GetPoint` |

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

## 8. Dependencies

The `backup/` package imports:
- `github.com/go-bond/bond` — for `bond.DB`, `bond.BOND_DB_DATA_VERSION`
- `github.com/go-bond/bond/utils` — for `utils.WriteFileWithSync` (in restore)
- `github.com/fujiwara/shapeio` — for per-stream bandwidth rate limiting via token-bucket shaped `io.Reader`
- `github.com/thanos-io/objstore` — for `objstore.Bucket` interface (promoted from indirect to direct dependency)
- `golang.org/x/sync/errgroup` — for parallel uploads/downloads with concurrency limits
- Standard library: `bytes`, `context`, `encoding/json`, `fmt`, `io`, `os`, `path`, `path/filepath`, `sort`, `strings`, `sync/atomic`, `time`

Test-only imports:
- `github.com/stretchr/testify/assert` and `require`

---

## 9. Edge Cases

- **Empty DB**: Checkpoint is valid; restore produces a DB with only the bond data version key.
- **Concurrent writes**: Checkpoint is a point-in-time snapshot. All committed writes prior to the checkpoint call are included.
- **Non-empty restore dir**: Returns an error (`"restore directory %q is not empty"`).
- **Empty RestoreDir option**: Returns an error (`"RestoreDir must be specified"`).
- **Incremental with no previous backup**: Returns an error (`"no previous backup found; cannot create incremental backup"`).
- **Bond metadata**: Recreated during restore from `meta.json`'s `pebble_format_version` field using `utils.WriteFileWithSync`.
- **Orphaned SSTs after restore**: Pebble only reads files referenced by the MANIFEST, so leftover SSTs from earlier complete backups are harmless.
- **Prefix normalization**: `ListBackups` auto-appends `/` to prefix if missing.
- **Missing CheckpointDir**: Returns an error (`"CheckpointDir is required"`). The caller must provide a well-known path, ideally on the same filesystem as the DB to allow Pebble's hard-link optimization.
- **Stale checkpoint**: If `CheckpointDir` already exists (e.g. from a previous crash), `Backup()` removes it before creating a new checkpoint, and always cleans up afterward via `defer`. The `HasCheckpoint()` and `RemoveCheckpoint()` utilities allow callers to detect and clean up stale checkpoints independently.
- **Backup type default**: If `opts.Type` is empty, defaults to `BackupTypeComplete`.
- **Timestamp default**: If `opts.At` is zero, defaults to `time.Now().UTC()`.
- **Before cutoff zero**: If `opts.Before` is zero in `RestoreOptions`, all backups are considered (internally set to year 9999).
- **Orphaned backup directories**: Directories matching the backup pattern but missing `meta.json` are skipped by `ListBackups()` and cleaned up at the start of `Backup()`. `Restore()` simply skips them.
- **Concurrent backups**: A lock file (`{prefix}/.lock`) prevents concurrent `Backup()` calls. Returns `ErrBackupInProgress` if another backup holds an active lock. Stale locks (older than TTL) are overridden.
- **Stale lock recovery**: If a backup process crashes without releasing its lock, the lock will expire after `LockTTL` (default 1 hour), allowing subsequent backups to proceed.
- **Lock refresh**: Long-running backups refresh the lock every `TTL/2` to prevent the lock from becoming stale while the backup is still in progress.

---

## 10. Usage Example

```go
import (
    "context"
    "github.com/go-bond/bond"
    "github.com/go-bond/bond/backup"
    "github.com/thanos-io/objstore"
)

// Complete backup
db, _ := bond.Open("mydb", bond.DefaultOptions())
bucket := // ... your objstore.Bucket implementation (S3, GCS, InMemBucket, etc.)

// CheckpointDir should be on the same filesystem as the DB for hard-link optimization.
checkpointDir := "mydb-checkpoint"

// Clean up any stale checkpoint from a previous crash at startup.
if has, _ := backup.HasCheckpoint(checkpointDir); has {
    _ = backup.RemoveCheckpoint(checkpointDir)
}

meta, err := backup.Backup(ctx, db, bucket, backup.BackupOptions{
    Prefix:        "backups",
    Type:          backup.BackupTypeComplete,
    CheckpointDir: checkpointDir,
})

// Later: incremental backup
meta, err = backup.Backup(ctx, db, bucket, backup.BackupOptions{
    Prefix:        "backups",
    Type:          backup.BackupTypeIncremental,
    CheckpointDir: checkpointDir,
})

// List all backups
backups, err := backup.ListBackups(ctx, bucket, "backups")

// Find restore set for a point in time
restoreSet, err := backup.FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC))

// Restore (latest)
err = backup.Restore(ctx, bucket, backup.RestoreOptions{
    Prefix:     "backups",
    RestoreDir: "/path/to/restored",
})
restoredDB, _ := bond.Open("/path/to/restored", bond.DefaultOptions())

// Restore to a specific point in time
err = backup.Restore(ctx, bucket, backup.RestoreOptions{
    Prefix:     "backups",
    RestoreDir: "/path/to/restored-pit",
    Before:     time.Date(2025, 2, 12, 12, 30, 0, 0, time.UTC),
})
```
