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
- `DeleteBackup(ctx, bucket, backupPrefix) error`
- `RemoveIncompleteBackups(ctx, bucket, prefix) (int, error)`
- `ReadBackupMeta(ctx, bucket, objectPrefix) (*BackupMeta, error)`
- `HasIncompleteRestore(dir) (bool, error)`
- `HasCheckpoint(dir) (bool, error)`
- `RemoveCheckpoint(dir) error`

### 1.3 True incremental backups (diff-based)

- **Complete backup**: Takes a Pebble checkpoint, uploads ALL files.
- **Incremental backup**: Takes a Pebble checkpoint, compares against the previous backup's file list (from its `meta.json`), uploads only **new or changed** files (compared by name + size).
- **Restore**: Finds the latest complete backup, downloads all its files, then applies each subsequent incremental in datetime order (downloading and overwriting/adding files).

This works because:
- Pebble SST files are immutable with unique numbered names (same name = same content).
- MANIFEST, CURRENT, OPTIONS, and WAL files may change between checkpoints and are always uploaded in incrementals.
- On restore, leftover SST files don't cause issues since Pebble only reads files referenced by the MANIFEST.

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
  backup.go              -- BackupOptions, Backup(), isRetriableError(), uploadFileWithRetry(), computeIncrementalFiles()
  backup_checkpoint.go   -- HasCheckpoint(), RemoveCheckpoint()
  backup_incomplete.go   -- isBackupIncomplete(), deleteBackupDir(), removeIncompleteBackupDirs(), DeleteBackup(), RemoveIncompleteBackups()
  backup_list.go         -- BackupInfo, ErrNoBackupsFound, ListBackups(), FindRestoreSet()
  backup_lock.go         -- DefaultLockTTL, ErrBackupInProgress, ErrLockRefreshFailed, lockPayload, acquireLock(), uploadAndVerifyLock(), releaseLock(), startLockRefresh()
  backup_meta.go         -- BackupMeta, FileInfo, newMetaRetryPolicy(), writeMeta(), readMeta(), ReadBackupMeta(), newBackupMeta()
  backup_naming.go       -- BackupType, datetimeFormat, backupDirName(), backupObjectPrefix(), parseBackupDir()
  backup_test.go         -- Unit tests for backup, restore, list, naming, incomplete backups, locking, checkpoint, rate limiting, edge cases
  consts.go              -- DefaultConcurrency, DefaultRateLimit, DefaultMaxUploadRetries, DefaultMaxDownloadRetries, DefaultInitialRetryBackoff, MaxRetryBackoff
  progress.go            -- ProgressEvent, ProgressFunc
  restore.go             -- RestoreOptions, Restore(), downloadFileWithRetry()
  restore_incomplete.go  -- HasIncompleteRestore(), writeRestoreIncompleteMarker(), removeRestoreIncompleteMarker(), cleanRestoreDir()
  stream_ratelimit.go    -- resolvePerStreamRate()

cmd/bond-cli/
  bond-cli.go            -- entry point; registers inspect and backup commands
  backup.go              -- BackupCommand with list, delete, restore subcommands
  backup_list.go         -- backupListCommand(), groupBackups(), grouped display with totals
  backup_delete.go       -- backupDeleteCommand(), selectOlderThan(), selectByDatetime(), warnOrphanedIncrementals()
  backup_restore.go      -- backupRestoreCommand(), progress display, point-in-time support
  bucket.go              -- newBucket(), newS3Bucket(), newGCSBucket(), newFSBucket(), bucketFlags

tests/
  backup_test.go         -- Integration tests: table-level backup/restore with Bond Table API
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
    MaxUploadRetries int        // Retries per file after first failed upload; zero = DefaultMaxUploadRetries. Only transient errors retried.
    InitialRetryBackoff time.Duration // Delay before first retry; zero = DefaultInitialRetryBackoff.
}
```

### RestoreOptions

```go
type RestoreOptions struct {
    Prefix              string        // Object storage prefix where backups are stored
    RestoreDir          string        // Local directory to restore into (must be empty or non-existent)
    Before              time.Time     // Point-in-time cutoff; zero = all backups
    Concurrency         int           // Parallel downloads per stage; <= 0 uses DefaultConcurrency
    OnProgress          ProgressFunc  // Called after each file download; must be goroutine-safe
    RateLimit           float64       // Aggregate download rate limit in bytes/sec; zero = DefaultRateLimit (100 MB/s); negative disables
    MaxDownloadRetries  int           // Retries per file after first failed download; zero = DefaultMaxDownloadRetries. Only transient errors retried.
    InitialRetryBackoff time.Duration // Delay before first retry; zero = DefaultInitialRetryBackoff.
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
func DeleteBackup(ctx context.Context, bucket objstore.Bucket, backupPrefix string) error
func RemoveIncompleteBackups(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error)
func ReadBackupMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string) (*BackupMeta, error)
func HasIncompleteRestore(dir string) (bool, error)
func HasCheckpoint(dir string) (bool, error)
func RemoveCheckpoint(dir string) error
```

### Constants

```go
const DefaultConcurrency = 4
const DefaultRateLimit float64 = 100 * 1024 * 1024 // 100 MB/s
const DefaultLockTTL = 1 * time.Hour
const DefaultMaxUploadRetries = 3
const DefaultMaxDownloadRetries = 3
const DefaultInitialRetryBackoff = 1 * time.Second
const MaxRetryBackoff = 30 * time.Second
```

### Sentinel Errors

```go
var ErrNoBackupsFound = fmt.Errorf("no backups found")
var ErrBackupInProgress = fmt.Errorf("another backup is in progress")
var ErrLockRefreshFailed = fmt.Errorf("lock refresh failed for longer than TTL")
```

---

## 5. Implementation Details

### 5.1 Complete backup flow

1. Default `opts.Type` to `BackupTypeComplete` if empty
2. Acquire lock via `acquireLock(ctx, bucket, opts.Prefix, ttl)`; fail fast with `ErrBackupInProgress` if held
3. Create a cancellable context via `context.WithCancelCause(ctx)`; defer `cancelBackup(nil)`
4. Start lock refresh goroutine via `startLockRefresh(ctx, bucket, prefix, ttl, cancelBackup)` — if lock refresh fails for longer than TTL, the backup context is cancelled with `ErrLockRefreshFailed`
5. Defer `stopRefresh()` and conditional lock release: the lock is **not** released if the context was cancelled due to `ErrLockRefreshFailed` (another process may have already acquired the stale lock); otherwise `releaseLock(context.Background(), bucket, prefix)` is called
6. Remove incomplete backup dirs via `removeIncompleteBackupDirs(ctx, bucket, opts.Prefix)`
7. Determine datetime (`opts.At` or `time.Now().UTC()`), ensure UTC
8. Compute object prefix via `backupObjectPrefix(opts.Prefix, dt, opts.Type)`
9. Validate `opts.CheckpointDir` is non-empty (required)
10. Remove any stale checkpoint at `opts.CheckpointDir` via `os.RemoveAll` + `defer os.RemoveAll(opts.CheckpointDir)` for cleanup
11. `db.Backend().Checkpoint(opts.CheckpointDir)` into the caller-provided directory
12. `filepath.WalkDir(opts.CheckpointDir, ...)` to collect all checkpoint files as `[]FileInfo{Name, Size}`
13. Resolve retry parameters: `MaxUploadRetries` (default `DefaultMaxUploadRetries`), `InitialRetryBackoff` (default `DefaultInitialRetryBackoff`)
14. Upload ALL files in parallel using `errgroup` with configurable concurrency (`opts.Concurrency`, default `DefaultConcurrency`), calling `opts.OnProgress` after each file
15. If `g.Wait()` returns an error and the context was cancelled by lock refresh failure, surface `ErrLockRefreshFailed` instead of generic `context.Canceled`
16. Read `pebble_format_version` via `db.Backend().FormatMajorVersion()` and `bond_data_version` via `bond.BOND_DB_DATA_VERSION`
17. Build `BackupMeta` via `newBackupMeta()` where `Files == CheckpointFiles` (all files)
18. Upload `meta.json` via `writeMeta()` with retry (see §5.10), return `*BackupMeta`

### 5.2 Incremental backup flow

1. Same steps 1-13 as complete (lock, cancellable context, lock refresh, conditional release, incomplete backup cleanup, checkpoint dir validation, checkpoint, retry resolution)
2. Call `computeIncrementalFiles()` which:
   a. `ListBackups()` -> take the last one -> `readMeta()` with retry (see §5.10)
   b. Build a set of `{name: size}` from the previous backup's `CheckpointFiles`
   c. Diff: for each file in the new checkpoint, if it's not in the previous set OR has a different size, mark as "to upload"
3. If no previous backup exists, return error: `"no previous backup found; cannot create incremental backup"`
4. Upload only the diff files in parallel to `{prefix}/{datetime}-incremental/{relativePath}`
5. Build `BackupMeta` where:
   - `Files` = only the diff files (what was uploaded)
   - `CheckpointFiles` = ALL files in the new checkpoint (for the next incremental's diff)
6. Upload `meta.json` with retry (see §5.10), return `*BackupMeta`

### 5.3 Restore flow

1. Validate `RestoreDir` is specified
2. **Check for `.incomplete` marker** (see §5.11): if `RestoreDir` contains a `.incomplete` file from a previously interrupted restore, clean all contents of the directory via `cleanRestoreDir()` so the restore starts fresh
3. Validate `RestoreDir` is empty or doesn't exist (after potential cleanup)
4. If `opts.Before` is zero, set it to `time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)` (effectively "all backups")
5. `FindRestoreSet(ctx, bucket, opts.Prefix, before)` -> `[complete, incr1, incr2, ...]`
6. If no backups found, return `ErrNoBackupsFound`
7. Create `RestoreDir` via `os.MkdirAll`
8. **Write `.incomplete` marker** via `writeRestoreIncompleteMarker()` — marks the restore as in progress
9. Resolve retry parameters: `MaxDownloadRetries` (default `DefaultMaxDownloadRetries`), `InitialRetryBackoff` (default `DefaultInitialRetryBackoff`)
10. Pre-read all metas via `readMeta()` with retry (see §5.10) to compute global totals for progress reporting (`totalFiles`, `totalBytes`)
11. Pre-create all needed subdirectories upfront to avoid concurrent `MkdirAll` calls
12. For each backup in order (sequential outer loop preserves incremental override semantics):
    a. Download files in parallel using `errgroup` with configurable concurrency (`opts.Concurrency`, default `DefaultConcurrency`)
    b. For each file in `meta.Files`: call `downloadFileWithRetry()` which downloads via `bucket.Get()`, streams to `filepath.Join(opts.RestoreDir, file.Name)` via `io.Copy`, retrying transient errors with the failsafe-go retry policy (see §5.9)
    c. Call `opts.OnProgress` after each file with cumulative counters across all stages
    d. Each incremental overwrites changed files (MANIFEST, CURRENT, etc.) and adds new SSTs
13. Recreate bond metadata from the **last** backup's meta:
    - `os.MkdirAll(filepath.Join(restoreDir, "bond"), 0755)`
    - Write `bond/PEBBLE_FORMAT_VERSION` with the stored `PebbleFormatVersion` via `utils.WriteFileWithSync`
14. **Remove `.incomplete` marker** via `removeRestoreIncompleteMarker()` — only removed on full success
15. Caller then opens with `bond.Open(restoreDir, opts)`

### 5.4 ListBackups and FindRestoreSet

- `ListBackups`: calls `bucket.Iter(ctx, prefix, ...)`, auto-appends `/` to prefix if missing, parses each entry with `parseBackupDir` (skips non-matching entries), skips incomplete dirs via `isBackupIncomplete()`, returns sorted by datetime ascending.
- `FindRestoreSet`: calls `ListBackups`, filters to backups at or before `before` (UTC), finds the latest complete backup in the filtered set by scanning backwards, returns `[complete, incr1, incr2, ...]`. Returns `ErrNoBackupsFound` if no complete backup exists at or before the cutoff.

### 5.5 Naming helpers

- `backupDirName(t, bt)` -> `"20250212120000-complete"` (datetime format + hyphen + type)
- `backupObjectPrefix(prefix, t, bt)` -> `"backups/20250212120000-complete/"` (with trailing slash)
- `parseBackupDir(name)` -> `(time.Time, BackupType, error)`: strips trailing `/`, takes `path.Base()`, splits on first `-`, parses datetime and validates type

### 5.6 Incomplete backup handling

An incomplete backup is a directory matching `{datetime}-{type}/` that has no `meta.json` file. This can happen if `Backup()` is interrupted after uploading checkpoint files but before writing `meta.json`.

- **Detection**: `isBackupIncomplete(ctx, bucket, backupPrefix)` checks `bucket.Exists()` for `meta.json`.
- **Cleanup**: `deleteBackupDir(ctx, bucket, dirPrefix)` discovers all objects via `bucket.Iter()` with `WithRecursiveIter()`, then deletes each. Guards against concurrent deletion with `bucket.IsObjNotFoundErr()`.
- **`removeIncompleteBackupDirs(ctx, bucket, prefix)`**: Iterates all dirs under prefix, identifies incomplete backups, deletes them. Returns count removed.
- **`RemoveIncompleteBackups(ctx, bucket, prefix)`**: Public API that delegates to `removeIncompleteBackupDirs`.
- **`ListBackups()`**: Skips incomplete dirs automatically via `isBackupIncomplete()` check in the `Iter` callback. This means `FindRestoreSet()` and `Restore()` benefit from incomplete backup skipping with no additional changes.
- **`Backup()`**: Calls `removeIncompleteBackupDirs()` at the start (after acquiring the lock) to clean up incomplete backups before proceeding.
- **`Restore()`**: Does **not** clean up incomplete backups. It simply skips them via `ListBackups()`.

### 5.7 Backup locking

A lock prevents concurrent `Backup()` calls from running against the same prefix. The lock is a JSON file at `{prefix}/.lock` containing `{"created_at":"<RFC3339>","nonce":"<hex>"}`.

- **Lock payload**: `lockPayload` struct contains `CreatedAt time.Time` and `Nonce string`. The nonce is a 128-bit random hex string generated via `crypto/rand` (with `time.UnixNano()` fallback), used for read-after-write verification.
- **`acquireLock(ctx, bucket, prefix, ttl)`**: Checks if lock exists. If it does and its age < TTL, returns `ErrBackupInProgress`. If age >= TTL (stale), lock doesn't exist, or lock data is corrupt, calls `uploadAndVerifyLock()` to write a new lock with read-after-write verification.
- **`uploadAndVerifyLock(ctx, bucket, lockPath)`**: Uploads a new lock with a unique nonce, waits a random jitter (0 to `defaultLockJitter`, 3s by default), then re-reads the lock to verify the nonce still matches. If another process overwrote the lock in that window, returns `ErrBackupInProgress`. This mitigates the TOCTOU race inherent in object storage (no atomic CAS). The `defaultLockJitter` is a package-level `var` (not `const`) so tests can set it to zero.
- **`releaseLock(ctx, bucket, prefix)`**: Deletes the lock. Ignores not-found errors.
- **`startLockRefresh(ctx, bucket, prefix, ttl, cancelFunc)`**: Starts a background goroutine that re-uploads the lock every `ttl/3` to prevent long-running backups from having their lock considered stale. If the lock cannot be refreshed for longer than the full TTL, it calls `cancelFunc(ErrLockRefreshFailed)` to abort the backup — the stale lock could be acquired by another process, so continuing is unsafe. Returns a `stop` function that terminates the goroutine.
- **`Backup()` integration**: Acquires lock, creates a cancellable context via `context.WithCancelCause(ctx)`, starts refresh with the cancel function, defers `stopRefresh()` and conditional lock release. The lock is **not** released if the context was cancelled due to `ErrLockRefreshFailed` (another process may have already acquired the stale lock); otherwise `releaseLock(context.Background(), bucket, prefix)` is called. After parallel uploads, if the error is caused by lock refresh failure, the root cause (`ErrLockRefreshFailed`) is surfaced instead of a generic `context.Canceled`.
- **`Restore()`**: Does **not** acquire a lock. Restore is read-only with respect to the backup set.

### 5.8 Upload retry

Per-file uploads during backup are retried on **transient** errors to improve robustness against network blips, provider 5xx, throttling, and timeouts. Retries are implemented using `failsafe-go`'s `retrypolicy.RetryPolicy`.

- **Retriable errors** (shared `isRetriableError` predicate): `context.DeadlineExceeded`, and any error that implements `Temporary() bool` and returns `true` (e.g. temporary network failures). `context.Canceled` is never retried.
- **Attempts**: One initial attempt plus up to `MaxUploadRetries` retries per file (default 3 → at most 4 attempts per file).
- **Backoff**: Exponential via `WithBackoff(initialBackoff, MaxRetryBackoff)` (initial delay `InitialRetryBackoff`, default 1s; cap `MaxRetryBackoff`, 30s) with `WithJitterFactor(0.25)` (±25%). The policy uses `WithContext(ctx)` so context cancellation during backoff is handled by failsafe-go.
- **ReturnLastFailure**: The policy uses `ReturnLastFailure()` so that when retries are exhausted the last raw error is returned (not wrapped in `retrypolicy.ExceededError`).
- **Reader reuse**: Each attempt re-opens the file and re-applies rate limiting, since `bucket.Upload` consumes the reader.
- **Options**: `BackupOptions.MaxUploadRetries` and `BackupOptions.InitialRetryBackoff`; zero values use the defaults above.

### 5.9 Download retry

Per-file downloads during restore use the same failsafe-go retry pattern as uploads.

- **Retriable errors**: Same `isRetriableError` predicate as uploads (see §5.8).
- **Attempts**: One initial attempt plus up to `MaxDownloadRetries` retries per file (default 3 → at most 4 attempts per file).
- **Backoff**: Exponential via `WithBackoff(initialBackoff, MaxRetryBackoff)` (initial delay `InitialRetryBackoff`, default 1s; cap `MaxRetryBackoff`, 30s) with `WithJitterFactor(0.25)` (±25%). Context cancellation during backoff is handled by failsafe-go via `WithContext(ctx)`.
- **ReturnLastFailure**: Same as uploads — raw last error returned on exhaustion.
- **Streaming**: Each attempt re-fetches the object via `bucket.Get()` (returns `io.ReadCloser`), opens the local file via `os.OpenFile` (create/truncate), and streams the data via `io.Copy(f, r)`. Rate limiting is applied via `shapeio.NewReaderWithContext` wrapping the `ReadCloser`. On copy error, the partial file is removed via `os.Remove`. This streams directly to disk without buffering the entire object in memory.
- **Options**: `RestoreOptions.MaxDownloadRetries` and `RestoreOptions.InitialRetryBackoff`; zero values use the defaults above.

### 5.10 Meta read/write retry

`readMeta()` and `writeMeta()` perform small but critical bucket operations (reading/writing `meta.json`). Both use the same failsafe-go retry pattern as file uploads and downloads.

- **Shared policy factory**: `newMetaRetryPolicy(maxRetries, initialBackoff)` builds a `retrypolicy.RetryPolicy` with `isRetriableError`, exponential backoff, ±25% jitter, and `ReturnLastFailure()`.
- **`writeMeta`**: Marshals `BackupMeta` to JSON locally, then retries the `bucket.Upload` call. The JSON payload (`bytes.NewReader(data)`) is safe to replay across attempts since it is recreated from the same byte slice.
- **`readMeta`**: Retries the `bucket.Get()` + `io.ReadAll()` pair. Each attempt fetches a fresh reader. `json.Unmarshal` runs outside the retry loop since it is a local operation.
- **Parameter threading**: Both functions accept `maxRetries` and `initialBackoff`. In `Backup()`, these come from `MaxUploadRetries` / `InitialRetryBackoff`; in `Restore()`, from `MaxDownloadRetries` / `InitialRetryBackoff`. Retry params are resolved early in both flows (before any `readMeta` calls) so that `computeIncrementalFiles()` and the pre-read-metas loop can use them.

### 5.11 Restore incomplete marker

A `.incomplete` marker file is placed in the local `RestoreDir` to track whether a restore operation has finished successfully. This is a **local filesystem** mechanism (unlike the backup incomplete detection in §5.6 which operates on object storage).

- **Marker creation**: `writeRestoreIncompleteMarker(dir)` creates an empty `.incomplete` file in `RestoreDir` immediately after the directory is created (step 8 in §5.3), before any file downloads begin.
- **Marker removal**: `removeRestoreIncompleteMarker(dir)` removes the `.incomplete` file only after the entire restore has completed successfully (step 14 in §5.3). If the restore is interrupted (crash, context cancellation, error), the marker remains.
- **Detection on next restore**: `HasIncompleteRestore(dir)` checks for the `.incomplete` file. If found at the start of `Restore()`, it means a previous restore was interrupted and the directory contains partial/corrupt data.
- **Cleanup**: `cleanRestoreDir(dir)` removes all contents of the directory (but not the directory itself) when an incomplete marker is detected. After cleanup, the directory is empty and the restore proceeds from scratch.
- **Non-empty dir without marker**: If `RestoreDir` is non-empty but has no `.incomplete` marker, `Restore()` still returns the existing `"restore directory %q is not empty"` error. The marker only enables automatic cleanup for directories that were previously being restored.
- **Implementation**: All helpers live in `restore_incomplete.go`: `restoreIncompleteFileName` (constant `.incomplete`), `restoreIncompleteFilePath()`, `HasIncompleteRestore()`, `writeRestoreIncompleteMarker()`, `removeRestoreIncompleteMarker()`, `cleanRestoreDir()`.

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
| `TestListBackups_SkipsIncomplete` | Valid backup + incomplete dir → `ListBackups` returns only valid |
| `TestRemoveIncompleteBackups` | Two incomplete dirs + one valid → `RemoveIncompleteBackups` removes 2, valid remains |
| `TestRemoveIncompleteBackups_NoIncomplete` | No incomplete backups → returns 0, no error |
| `TestBackup_CleansIncompleteBeforeIncremental` | Valid complete + incomplete incremental as "latest" → new incremental succeeds, incomplete backup removed |
| `TestRestore_SkipsIncomplete` | Valid complete + incomplete incremental → restore succeeds, data correct, incomplete backup still exists (not cleaned) |
| `TestBackup_LockPreventsConcurrent` | Acquire lock manually, attempt `Backup` → returns `ErrBackupInProgress` |
| `TestBackup_StaleLockIsOverridden` | Upload a lock with old timestamp, `Backup` with short TTL → succeeds (stale lock overridden) |
| `TestBackup_LockReleasedOnError` | Backup that errors (canceled ctx after lock) → lock is released via defer |
| `TestBackup_LockReleasedOnSuccess` | Successful backup → lock object no longer exists afterward |
| `TestHasCheckpoint_NoCheckpoint` | Returns `false` when directory does not exist |
| `TestHasCheckpoint_WithCheckpoint` | Returns `true` after creating a directory |
| `TestRemoveCheckpoint` | Creates dir with files, removes it, verifies gone |
| `TestRemoveCheckpoint_NoCheckpoint` | Idempotent — no error when directory does not exist |
| `TestResolvePerStreamRate` | Per-stream rate calculation: default rate, custom rate, disabled (negative), zero concurrency |
| `TestBackupWithRateLimit` | Backup + restore with explicit rate limit; verify data integrity |
| `TestRestoreWithRateLimit` | Restore with explicit rate limit; verify data integrity |
| `TestBackupNoRateLimit` | Backup + restore with rate limiting disabled (negative); verify data integrity |
| `TestBackup_CleansStaleCheckpoint` | Stale checkpoint cleaned before backup, checkpoint cleaned after |
| `TestBackup_UploadRetry_SuccessAfterRetries` | Bucket fails first 2 attempts per key then succeeds → backup and restore succeed |
| `TestBackup_UploadRetry_PermanentErrorFails` | Bucket always returns non-retriable error → backup fails with that error |
| `TestBackup_UploadRetry_ContextCancelDuringBackoff` | Context cancelled during retry backoff → error is context.Canceled |
| `TestIsRetriableError` | Retriable: DeadlineExceeded, Temporary(); not retriable: nil, Canceled, permanent |
| `TestRestore_IncompleteMarkerRemovedOnSuccess` | Successful restore removes `.incomplete` marker; restored DB is openable |
| `TestRestore_IncompleteMarkerCleansInterruptedRestore` | Simulated interrupted restore (leftover files + `.incomplete` marker) → new restore detects marker, cleans directory, restores successfully with correct data |
| `TestRestore_NonEmptyDirWithoutIncompleteMarkerFails` | Non-empty dir without `.incomplete` marker still returns "not empty" error (preserves existing behavior) |
| `TestRestore_CancelledLeavesIncompleteMarker` | Restore cancelled mid-download → `.incomplete` marker remains in RestoreDir |
| `TestRestore_RetryAfterCancelledRestore` | End-to-end: cancel restore mid-way, then retry to same dir → automatic cleanup and successful restore with data integrity |

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
- `github.com/failsafe-go/failsafe-go` — for declarative retry policies with exponential backoff, jitter, and context-aware cancellation (used in `uploadFileWithRetry` and `downloadFileWithRetry`)
- `github.com/fujiwara/shapeio` — for per-stream bandwidth rate limiting via token-bucket shaped `io.Reader`
- `github.com/thanos-io/objstore` — for `objstore.Bucket` interface (promoted from indirect to direct dependency)
- `golang.org/x/sync/errgroup` — for parallel uploads/downloads with concurrency limits
- Standard library: `bytes`, `context`, `crypto/rand`, `encoding/hex`, `encoding/json`, `errors`, `fmt`, `io`, `math/rand/v2`, `os`, `path`, `path/filepath`, `sort`, `strings`, `sync/atomic`, `time`

The `cmd/bond-cli/` CLI imports (in addition to the `backup/` package):
- `github.com/urfave/cli/v2` — for CLI command structure and flag parsing
- `github.com/dustin/go-humanize` — for human-readable byte sizes in output
- `github.com/mattn/go-isatty` — for detecting non-terminal stdin in `backup delete`
- `github.com/go-kit/log` — required by objstore provider constructors (nop logger)
- `github.com/thanos-io/objstore/providers/s3` — S3 bucket provider
- `github.com/thanos-io/objstore/providers/gcs` — GCS bucket provider
- `github.com/thanos-io/objstore/providers/filesystem` — local filesystem bucket provider

Test-only imports:
- `github.com/stretchr/testify/assert` and `require`

---

## 9. Edge Cases

- **Empty DB**: Checkpoint is valid; restore produces a DB with only the bond data version key.
- **Concurrent writes**: Checkpoint is a point-in-time snapshot. All committed writes prior to the checkpoint call are included.
- **Non-empty restore dir**: Returns an error (`"restore directory %q is not empty"`) unless a `.incomplete` marker is present from a previously interrupted restore, in which case the directory is cleaned automatically and the restore starts fresh.
- **Empty RestoreDir option**: Returns an error (`"RestoreDir must be specified"`).
- **Incremental with no previous backup**: Returns an error (`"no previous backup found; cannot create incremental backup"`).
- **Bond metadata**: Recreated during restore from `meta.json`'s `pebble_format_version` field using `utils.WriteFileWithSync`.
- **Leftover SSTs after restore**: Pebble only reads files referenced by the MANIFEST, so leftover SSTs from earlier complete backups are harmless.
- **Prefix normalization**: `ListBackups` auto-appends `/` to prefix if missing.
- **Missing CheckpointDir**: Returns an error (`"CheckpointDir is required"`). The caller must provide a well-known path, ideally on the same filesystem as the DB to allow Pebble's hard-link optimization.
- **Stale checkpoint**: If `CheckpointDir` already exists (e.g. from a previous crash), `Backup()` removes it before creating a new checkpoint, and always cleans up afterward via `defer`. The `HasCheckpoint()` and `RemoveCheckpoint()` utilities allow callers to detect and clean up stale checkpoints independently.
- **Backup type default**: If `opts.Type` is empty, defaults to `BackupTypeComplete`.
- **Timestamp default**: If `opts.At` is zero, defaults to `time.Now().UTC()`.
- **Before cutoff zero**: If `opts.Before` is zero in `RestoreOptions`, all backups are considered (internally set to year 9999).
- **Incomplete backup directories**: Directories matching the backup pattern but missing `meta.json` are skipped by `ListBackups()` and cleaned up at the start of `Backup()`. `Restore()` simply skips them.
- **Concurrent backups**: A lock file (`{prefix}/.lock`) prevents concurrent `Backup()` calls. Returns `ErrBackupInProgress` if another backup holds an active lock. Stale locks (older than TTL) are overridden.
- **Stale lock recovery**: If a backup process crashes without releasing its lock, the lock will expire after `LockTTL` (default 1 hour), allowing subsequent backups to proceed.
- **Lock refresh**: Long-running backups refresh the lock every `TTL/3` to prevent the lock from becoming stale while the backup is still in progress. If the lock cannot be refreshed for longer than the full TTL, the backup is cancelled with `ErrLockRefreshFailed`.
- **Lock TOCTOU mitigation**: Lock acquisition uses read-after-write verification with a random nonce and jitter delay to mitigate the TOCTOU race inherent in object storage (no atomic CAS). This makes concurrent acquisition collisions extremely unlikely but does not eliminate them entirely.
- **Retries in Backup() / Restore()**: Both `Backup()` and `Restore()` have per-file retries on transient errors using failsafe-go retry policies (see §5.8 and §5.9). Operation-level retries (retry whole Backup or whole Restore on failure) are **not implemented**.
- **Interrupted restore recovery**: A `.incomplete` marker file is written to `RestoreDir` at the start of the restore and removed only on successful completion. If `Restore()` is called on a directory with an existing `.incomplete` marker, all contents are cleaned and the restore starts from scratch. This handles crash recovery, context cancellation, and any other interruption gracefully.
- **Refactor to extract common and utility functions**: Moving shared and utility logic into separate files (e.g. common upload/download helpers, path utilities) — **not implemented**.

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

---

## 11. CLI (`bond-cli backup`)

The `bond-cli` binary provides backup management commands for listing, restoring, and deleting backups in object storage. The CLI is built with `github.com/urfave/cli/v2`.

### 11.1 Entry point

`cmd/bond-cli/bond-cli.go` registers `BackupCommand` alongside the existing `inspect` command:

```go
app := cli.App{
    Name:  "bond-cli",
    Usage: "tools to manage bond db",
    Commands: []*cli.Command{
        inspect.NewInspectCLI(nil),
        BackupCommand,
    },
}
```

### 11.2 Storage backends (`bucket.go`)

All backup subcommands share a common set of `bucketFlags` for configuring the object storage backend. The `--storage` flag selects the backend type, and backend-specific flags provide connection details.

**Shared flags:**
- `--storage` (required): Storage backend type — `s3`, `gcs`, or `fs`
- `--prefix`: Object storage prefix for backups (e.g. `backups/`)

**S3 flags** (env vars supported):
- `--s3-endpoint` (`S3_ENDPOINT`): S3 endpoint URL (required for S3)
- `--s3-bucket` (`S3_BUCKET`): S3 bucket name (required for S3)
- `--s3-access-key` (`AWS_ACCESS_KEY_ID`): S3 access key (required for S3)
- `--s3-secret-key` (`AWS_SECRET_ACCESS_KEY`): S3 secret key (required for S3)
- `--s3-region` (`AWS_REGION`): S3 region
- `--s3-insecure`: Use HTTP instead of HTTPS

**GCS flags:**
- `--gcs-bucket` (`GCS_BUCKET`): GCS bucket name (required for GCS)
- `--gcs-service-account` (`GOOGLE_APPLICATION_CREDENTIALS`): Path to service account JSON key file; falls back to Google Application Default Credentials (ADC) if omitted

**Filesystem flags:**
- `--fs-directory`: Local directory to use as the bucket root (required for fs)

### 11.3 `bond-cli backup list`

Lists all backups grouped by complete + incremental chains.

```
bond-cli backup list --storage s3 --s3-endpoint ... --s3-bucket ... --prefix backups/
```

Output shows each backup group with TYPE, DATETIME, FILES, SIZE, and PREFIX columns, plus per-group and overall totals. Uses `ReadBackupMeta()` to fetch metadata for each backup. Groups are formed by starting a new group at each complete backup; orphaned incrementals before any complete backup are placed in their own group.

### 11.4 `bond-cli backup restore`

Restores a backup set from object storage to a local directory.

```
bond-cli backup restore --storage s3 ... --prefix backups/ --restore-dir /path/to/restored
```

**Flags:**
- `--restore-dir` (required): Local directory to restore into (must be empty or non-existent)
- `--before`: Point-in-time cutoff in RFC3339 format (e.g. `2025-02-12T15:00:00Z`); if omitted, restores the latest
- `--concurrency`: Number of parallel downloads (default: `DefaultConcurrency`, 4)
- `--rate-limit`: Download rate limit in MB/s (0 for default, negative to disable)

Displays inline progress during the restore: `[files/total] bytes done/total  filename`.

### 11.5 `bond-cli backup delete`

Deletes backups from object storage with safety features.

```
bond-cli backup delete --storage s3 ... --prefix backups/ --older-than 720h
bond-cli backup delete --storage s3 ... --prefix backups/ --all --force
bond-cli backup delete --storage s3 ... --prefix backups/ --datetime 20250212120000-complete
```

**Selection flags** (mutually exclusive — exactly one required):
- `--all`: Delete all backups under the prefix
- `--older-than`: Delete backups older than the given duration (e.g. `720h` for 30 days)
- `--datetime`: Delete a specific backup by its datetime-type identifier (e.g. `20250212120000-complete`)

**Safety flags:**
- `--force`: Skip the confirmation prompt
- `--keep-last` (default: `true`): When using `--older-than`, preserves the most recent complete backup chain to ensure at least one restorable backup remains. Use `--keep-last=false` to delete all matching backups.

**Safety features:**
- **Non-terminal stdin detection**: If stdin is not a terminal (e.g. CI, piped input, `/dev/null`) and `--force` is not set, returns an error instructing the user to use `--force`. Detected via `go-isatty`.
- **Orphaned incremental warnings**: When deleting a complete backup, warns if dependent incrementals are not included in the delete set.
- **Cascading incremental selection**: When `--older-than` selects a complete backup, its dependent incrementals (up to the next complete) are automatically included to avoid leaving orphaned incrementals.
- **Pre-deletion summary**: Shows the number of backups, total files, and total size to be deleted, plus the list of backups with type, datetime, and prefix.
- **`--keep-last` guard**: If `--older-than` with `--keep-last` would delete every complete backup, the most recent complete chain is excluded and the user is informed how many backups were preserved.
