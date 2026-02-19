# Backup System Brief

The `backup/` package provides full database backup and restore for Bond via Pebble's `Checkpoint` mechanism. Backups are uploaded to object storage (S3, GCS, Azure, local filesystem) through the `objstore.Bucket` abstraction from [thanos-io/objstore](https://github.com/thanos-io/objstore).

## Backup Types

**Complete backup** — takes a Pebble checkpoint and uploads all files. This is a self-contained snapshot of the entire database at a point in time.

**Incremental backup** — takes a Pebble checkpoint but only uploads files that are new or changed since the previous backup. The diff is computed by comparing file names and sizes against the previous backup's metadata. This works because Pebble SST files are immutable with unique numbered names — same name means same content.

## Restore

Restore finds the latest complete backup and applies all subsequent incrementals in chronological order. Each incremental overwrites changed metadata files (MANIFEST, CURRENT, etc.) and adds new SST files. Leftover SSTs from earlier backups are harmless since Pebble only reads files referenced by the active MANIFEST.

Point-in-time restore is supported via the `Before` option, which limits the restore set to backups at or before the given timestamp. **Chain integrity** is enforced: if any intermediate backup in the chain has been removed, `FindRestoreSet` returns `ErrIncompleteRestoreChain` so restore does not proceed with a broken chain.

## Object Storage Layout

```
{prefix}/{datetime}-complete-{seq}/          Files + meta.json   (seq = 00000 for complete)
{prefix}/{datetime}-incremental-{seq}/      Files + meta.json  (seq = 00001, 00002, ... per chain)
{prefix}/.lock                               Concurrency lock
```

- **Datetime**: `YYYYMMDDHHMMSS` format (UTC, lexicographically sortable).
- **Sequence**: Per-chain sequence number, zero-padded to 5 digits. Complete backups use `00000`; incrementals use the previous backup's seq + 1. The sequence resets with each new complete backup. Used to detect gaps (e.g. a deleted incremental) before restore.

## Metadata

Each backup writes a `meta.json` containing:

- **`files`** — files uploaded in this backup (what to download during restore).
- **`checkpoint_files`** — all files in the checkpoint at this point (used by the next incremental to compute its diff).
- Pebble format version and Bond data version, used to recreate bond metadata on restore.

## Concurrency & Safety

- **Locking** — a JSON lock file (`{prefix}/.lock`) prevents concurrent backups against the same prefix. Stale locks (older than a configurable TTL, default 1 hour) are automatically overridden. Long-running backups refresh the lock periodically. Restore does not acquire a lock.
- **Parallel I/O** — file uploads and downloads run in parallel via `errgroup` with configurable concurrency (default 4 streams).
- **Rate limiting** — aggregate bandwidth is capped (default 100 MB/s) and distributed across parallel streams.
- **Retry** — transient errors (timeouts, temporary network failures) trigger per-file retries with exponential backoff and jitter via `failsafe-go`.

## Incomplete Operation Recovery

- **Backup** — a backup directory without `meta.json` is considered incomplete. `Backup()` cleans up incomplete directories before starting. `ListBackups()` skips them automatically.
- **Restore** — a `.incomplete` marker file is written to the restore directory before downloading and removed only on success. If `Restore()` finds this marker from a prior interrupted run, it cleans the directory and starts fresh.

## CLI

The `bond-cli backup` command provides subcommands for managing backups:

| Command | Purpose |
|---------|---------|
| `backup list` | Lists all backups grouped by complete + incremental chains, showing type, seq, datetime, file counts, sizes, and prefix. |
| `backup restore` | Restores a backup set to a local directory with progress display. Supports `--before` for point-in-time restore. |
| `backup delete` | Deletes backups by age (`--older-than`), specific datetime, or all. Includes safety guards: confirmation prompts, `--keep-last` to preserve at least one restorable chain, and orphaned incremental warnings. |

Storage backend is selected via `--storage` (s3, gcs, fs) with backend-specific flags for connection details.

## API at a Glance

```go
backup.Backup(ctx, db, bucket, BackupOptions{...})    // Create a complete or incremental backup
backup.Restore(ctx, bucket, RestoreOptions{...})       // Restore to a local directory
backup.ListBackups(ctx, bucket, prefix)                // List all valid backups
backup.FindRestoreSet(ctx, bucket, prefix, before)     // Resolve the backup chain needed for restore
backup.DeleteBackup(ctx, bucket, backupPrefix)         // Delete a single backup
backup.RemoveIncompleteBackups(ctx, bucket, prefix)    // Clean up incomplete backups
```
