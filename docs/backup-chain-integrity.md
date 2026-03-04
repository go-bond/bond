# Backup Chain Integrity: Incremental Backup Baseline Mismatch

## Problem

The current incremental backup implementation does not verify that the database being backed up is the same instance that produced the previous backup in the target bucket. This creates a silent data-integrity risk in two broad scenarios:

1. **Multiple backup destinations** — the same database is backed up to different buckets or prefixes, and an incremental is later made against a stale chain.
2. **Database instance swap** — a different copy of the database (e.g. after an A/B deployment switch, a restore to a different node, or a failover) starts producing incremental backups into a chain that was started by the original instance.

The root cause is the same in both cases: `computeIncrementalFiles` diffs the current checkpoint against the last backup in the bucket without verifying that the database actually evolved from that backup's state.

### Scenario 1: Multiple Backup Destinations

A node backs up to two different buckets:

```
t0: Complete backup to Bucket-A   → chain A starts
t1: Incremental backup to Bucket-A → OK, diff against A's chain
t2: Complete backup to Bucket-B   → chain B starts, DB continues to evolve
t3: Incremental backup to Bucket-B → OK, diff against B's chain
t4: More writes to the DB
t5: Incremental backup to Bucket-A → BUG: diff computed against A's last backup (t1),
                                      but the DB has diverged through B's chain and new writes
```

At **t5**, `computeIncrementalFiles` reads the `CheckpointFiles` from the last backup in Bucket-A (created at t1) and diffs against the current checkpoint. The diff will capture files that changed since t1, but the intermediate state (t2–t4) was never part of Bucket-A's chain.

### Scenario 2: A/B Deployment / Instance Swap

In an A/B deployment, two database instances (DB-A and DB-B) take turns being active. Backups go to the same bucket:

```
t0: DB-A is active → Complete backup       → chain starts with DB-A's state
t1: DB-A is active → Incremental backup    → OK, same instance
t2: DB-B starts from the backup at t1, receives its own writes from that point
t3: DB-A is active → Incremental backup    → OK, chain advances to DB-A's t3 state
t4: Traffic switches to DB-B
t5: DB-B is active → Incremental backup    → BUG: diff computed against DB-A's backup at t3,
                                               but DB-B branched off at t1 and never had t3's state
```

DB-B was started from the state at t1 and has been receiving its own writes since t2, while DB-A continued independently and produced another backup at t3. When traffic switches to DB-B at t4, the chain's latest entry is DB-A's backup at t3 — a state that DB-B never went through. DB-B's checkpoint at t5 contains SSTs that reflect its own compaction and write history since t1, not t3. The incremental diff against t3 produces a meaningless set of "changed" files — SSTs may be missing because DB-B compacted them differently, and some may appear unchanged by name/size but belong to an entirely different compaction lineage.

Note that if DB-A had not made the backup at t3, DB-B's first incremental would diff against the backup at t1 — the state DB-B was restored from — and the chain would be valid.

### Why This Corrupts Restores

In both scenarios the backup chain looks structurally valid (sequence numbers are contiguous, all files exist), but the resulting restore is a frankenstein:

- The complete backup has state from the original instance/chain.
- The incremental has a diff computed against a baseline the current database never actually evolved from.
- The final MANIFEST may reference SSTs that only existed in the original instance's compaction history. If those SSTs were never uploaded (because they appeared "unchanged" by name/size), the restored database is missing data or outright corrupt.

### Why Sequence Numbers Don't Catch This

The existing chain validation checks sequence number contiguity (`0, 1, 2, ...`), which detects deleted intermediate backups. However, it cannot detect that the database instance changed between two incremental backups — the sequence numbers are still contiguous, but the logical chain is broken because the underlying database is different.

## Proposed Solution: Backup UUID

Introduce a random **backup UUID** that marks which backup a database last produced. The UUID is:

1. **Generated** on every backup (both complete and incremental) — a new random string (e.g. 128-bit hex via `crypto/rand`).
2. **Stored in `meta.json`** — added as a new `uuid` field in `BackupMeta`.
3. **`meta.json` saved locally** — after a successful backup, a copy of `meta.json` is written to the database directory (e.g. `bond/meta.json`). During restore, the last backup's `meta.json` is also placed in the database directory alongside the other restored files.

This reuses the existing `meta.json` format with no new file types. The local copy serves as the database's record of which backup it last produced.

The validation rule is simple: **the `uuid` in the local `meta.json` must match the `uuid` of the last backup in the bucket**. If they match, this database produced that backup and is safe to continue the chain. If they don't match, a different instance (or a different chain) produced the last backup, and an incremental is refused.

#### Why `meta.json` Is Written After Success (Crash Safety)

Storing the UUID as a Pebble key would require writing it *before* the checkpoint (so it gets captured in the checkpoint files). This creates a crash-safety problem: if the app crashes after writing the key but before the backup completes, the database is left with a UUID that matches no backup in the bucket, and every subsequent incremental is refused. A deferred rollback can handle graceful failures, but cannot survive a process crash or power loss.

Writing the local `meta.json` *after* the backup succeeds avoids this entirely. The file is only updated once everything (checkpoint, upload, bucket `meta.json` write) has completed successfully. If the backup fails or the process crashes at any point before that, the local file still reflects the previous backup, and the next incremental will proceed correctly.

#### Why Every Backup Needs a New UUID

If only complete backups generated UUIDs, the A/B scenario would not be caught. Both DB-A and DB-B would share the UUID from the original complete backup, so DB-B's incremental after DB-A's additional incrementals would pass the check despite the baseline mismatch. By generating a new UUID on every backup, each incremental advances the UUID, and any instance that didn't produce the latest backup will have a stale UUID.

Walking through Scenario 2 with per-backup UUIDs:

```
t0: DB-A complete backup    → generates UUID-0, meta.json written to DB-A dir and bucket
t1: DB-A incremental backup → generates UUID-1, meta.json written to DB-A dir and bucket
t2: DB-B restored from t1   → restore places meta.json (with UUID-1) in DB-B dir
t3: DB-A incremental backup → generates UUID-2, meta.json written to DB-A dir and bucket
                               bucket's last backup now has UUID-2
t4: Traffic switches to DB-B
t5: DB-B incremental backup → reads UUID-1 from local meta.json, reads UUID-2 from bucket
                               → MISMATCH → refused
```

### Complete Backup Flow (Modified)

1. Generate a new random backup UUID.
2. Proceed with checkpoint and upload as before.
3. Include the UUID in the bucket's `meta.json`.
4. **After success**: write a copy of `meta.json` to the database directory (e.g. `bond/meta.json`).

Because complete backups always start a new chain, they unconditionally overwrite the local `meta.json`. A complete backup is always allowed regardless of what UUID (if any) was previously stored.

### Incremental Backup Flow (Modified)

1. Read the `uuid` from the **last backup in the bucket** (from its `meta.json`).
2. Read the `uuid` from the **local `meta.json`** in the database directory.
3. **If they don't match** — refuse the incremental backup with a clear error, e.g.:

   ```
   backup UUID mismatch: database has UUID "abc123" but the latest backup
   in the bucket has UUID "def456"; a complete backup is required to
   start a new chain
   ```

4. **If they match** — generate a new UUID, proceed with the incremental backup as usual; include the new UUID in the bucket's `meta.json`.
5. **After success**: write a copy of the new `meta.json` to the database directory.

### Crash Safety

The local `meta.json` is written only after the backup has fully succeeded (all files uploaded, bucket `meta.json` written). This means:

- **Crash during backup**: The local `meta.json` still reflects the previous backup. The next incremental sees the old UUID, which matches the last *successful* backup in the bucket. The chain continues correctly.
- **Crash after success but before local `meta.json` write**: The local file has the previous UUID, but the bucket's last backup has the new UUID. The next incremental sees a mismatch and refuses. This is a false positive — the backup actually succeeded — but it is safe: a complete backup will recover the chain. This window is extremely narrow (a single file write after everything else has completed).

### Restore Flow (Modified)

After downloading all backup files, restore writes a copy of the **last** backup's `meta.json` to the restored database directory (e.g. `bond/meta.json`). This follows the same pattern as the existing `bond/PEBBLE_FORMAT_VERSION` file. The restored database then has the correct UUID, so subsequent incremental backups against the same bucket will succeed without requiring a new complete backup.

### Data Model Changes

**`BackupMeta`** — add a new field:

```go
type BackupMeta struct {
    // ... existing fields ...
    UUID string `json:"uuid"` // random identifier tying this backup to its chain
}
```

No new file formats. The local `meta.json` in the database directory uses the same `BackupMeta` structure as the bucket copy.

### Backward Compatibility

- Backups created before this change will have no `uuid` in their `meta.json` (field will be absent, unmarshals as empty string).
- When an incremental backup encounters a previous backup with an empty `uuid`, it should treat it as **no chain validation available** and proceed (with an optional warning log). This avoids breaking existing backup chains.
- A database with no local `meta.json` (created before this change) is also treated as no validation available.
- Alternatively, the first incremental against a legacy chain can adopt the database's UUID and write it to its own `meta.json`, effectively starting chain validation from that point forward.

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| Fresh database, no local `meta.json` | Complete backup generates UUID and writes local `meta.json`; incremental without it skips validation or requires a complete backup first |
| Database restored from backup | Restore places `meta.json` in the DB directory; incrementals to the same bucket work without a new complete backup |
| Database restored, incremental to a *different* bucket | Local UUID won't match the other bucket's chain → error, requires complete backup |
| A/B deployment: switch active instance | New instance has a different local `meta.json` (from whichever backup it was restored from) → if UUID doesn't match the chain's latest, incremental is refused |
| A/B deployment: switch back to original instance | If the original DB still has the matching UUID in its local `meta.json`, incremental succeeds; otherwise complete backup required |
| Multiple prefixes in the same bucket | Each prefix has its own chain; the DB stores only one local `meta.json`, so incremental backups can only continue one chain at a time |
| Legacy backups without `uuid` | Incremental proceeds without validation (backward compatible) |
| Legacy database without local `meta.json` | Incremental proceeds without validation (backward compatible) |

## Benefits

- **Prevents silent corruption**: An incremental backup against the wrong baseline is caught immediately with a clear error message.
- **Low overhead**: One local file write per backup, one local file read per incremental backup.
- **Crash safe**: The local `meta.json` is written only after the backup fully succeeds — no rollback logic needed.
- **No new file formats**: Reuses the existing `BackupMeta` / `meta.json` structure for both the bucket and the local copy.
- **Backward compatible**: Old backups and databases without UUIDs or local `meta.json` continue to work.
