package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func openTestDB(t *testing.T, dir string) bond.DB {
	t.Helper()
	opts := bond.DefaultOptions(bond.MediumPerformance)
	db, err := bond.Open(dir, opts)
	require.NoError(t, err)
	return db
}

func insertTestData(t *testing.T, db bond.DB, start, count int) {
	t.Helper()
	for i := start; i < start+count; i++ {
		key := bond.KeyEncode(bond.Key{
			TableID:    0xC0,
			IndexID:    0x00,
			Index:      []byte{},
			IndexOrder: []byte{},
			PrimaryKey: []byte(fmt.Sprintf("key_%06d", i)),
		})
		value := []byte(fmt.Sprintf("value_%06d", i))
		require.NoError(t, db.Set(key, value, bond.Sync))
	}
}

func collectAllKVs(t *testing.T, db bond.DB) map[string]string {
	t.Helper()
	result := make(map[string]string)
	itr := db.Iter(&bond.IterOptions{})
	defer itr.Close()
	for itr.First(); itr.Valid(); itr.Next() {
		k := make([]byte, len(itr.Key()))
		copy(k, itr.Key())
		v := make([]byte, len(itr.Value()))
		copy(v, itr.Value())
		result[string(k)] = string(v)
	}
	return result
}

func TestNamingHelpers(t *testing.T) {
	ts := time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC)

	name := backupDirName(ts, BackupTypeComplete)
	assert.Equal(t, "20250212120000-complete", name)

	name = backupDirName(ts, BackupTypeIncremental)
	assert.Equal(t, "20250212120000-incremental", name)

	prefix := backupObjectPrefix("backups", ts, BackupTypeComplete)
	assert.Equal(t, "backups/20250212120000-complete/", prefix)

	// Round-trip
	parsedTime, parsedType, err := parseBackupDir("20250212120000-complete")
	require.NoError(t, err)
	assert.Equal(t, ts, parsedTime)
	assert.Equal(t, BackupTypeComplete, parsedType)

	parsedTime, parsedType, err = parseBackupDir("20250212120000-incremental/")
	require.NoError(t, err)
	assert.Equal(t, ts, parsedTime)
	assert.Equal(t, BackupTypeIncremental, parsedType)

	// With path prefix
	parsedTime, parsedType, err = parseBackupDir("backups/20250212120000-complete/")
	require.NoError(t, err)
	assert.Equal(t, ts, parsedTime)
	assert.Equal(t, BackupTypeComplete, parsedType)

	// Invalid
	_, _, err = parseBackupDir("invalid")
	assert.Error(t, err)

	_, _, err = parseBackupDir("20250212120000-unknown")
	assert.Error(t, err)
}

func TestBackupComplete(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 10)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NotNil(t, meta)

	assert.Equal(t, BackupTypeComplete, meta.Type)
	assert.Equal(t, "20250212120000", meta.Datetime)
	assert.Greater(t, len(meta.Files), 0)
	assert.Equal(t, meta.Files, meta.CheckpointFiles)
	assert.Greater(t, meta.PebbleFormatVersion, uint64(0))
	assert.Equal(t, uint32(bond.BOND_DB_DATA_VERSION), meta.BondDataVersion)

	// Verify meta.json exists in the bucket.
	ok, err := bucket.Exists(ctx, "backups/20250212120000-complete/meta.json")
	require.NoError(t, err)
	assert.True(t, ok)

	// Verify uploaded files match meta.
	objMap := bucket.Objects()
	objs := make([]string, 0, len(objMap))
	for k := range objMap {
		objs = append(objs, k)
	}
	sort.Strings(objs)
	assert.Greater(t, len(objs), 1) // at least meta.json + checkpoint files
}

func TestRestoreComplete(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 20)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	db.Close()

	// Restore to a new directory.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// Open the restored DB.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestBackupIncremental(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 10)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup.
	completeMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Insert more data.
	insertTestData(t, db, 10, 10)

	// Incremental backup.
	incrMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// The incremental should have fewer uploaded files than the complete.
	assert.Less(t, len(incrMeta.Files), len(completeMeta.Files))
	// But CheckpointFiles should have all files at this point.
	assert.GreaterOrEqual(t, len(incrMeta.CheckpointFiles), len(completeMeta.CheckpointFiles))
}

func TestRestoreWithIncrementals(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Phase 1: Insert initial data, complete backup.
	insertTestData(t, db, 0, 10)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 2: Insert more data, incremental backup.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 3: Insert even more data, another incremental.
	insertTestData(t, db, 20, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	originalKVs := collectAllKVs(t, db)
	db.Close()

	// Restore.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestRestoreBeforeTime(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Phase 1: Initial data + complete backup at T=12:00.
	insertTestData(t, db, 0, 10)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	phase1KVs := collectAllKVs(t, db)

	// Phase 2: More data + incremental at T=13:00.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 3: Even more data + incremental at T=14:00.
	insertTestData(t, db, 20, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	db.Close()

	// Restore to just after the complete backup (before any incrementals).
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
		Before:     time.Date(2025, 2, 12, 12, 30, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, phase1KVs, restoredKVs)
}

func TestListBackups(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	times := []time.Time{
		time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
	}
	types := []BackupType{BackupTypeComplete, BackupTypeIncremental, BackupTypeIncremental}

	for i, ts := range times {
		_, err := Backup(ctx, db, bucket, BackupOptions{
			Prefix: "backups",
			Type:   types[i],
			At:     ts,
		})
		require.NoError(t, err)
	}

	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	require.Len(t, backups, 3)

	for i, b := range backups {
		assert.Equal(t, times[i], b.Datetime)
		assert.Equal(t, types[i], b.Type)
	}
}

func TestFindRestoreSet(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete at T=12:00
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Incremental at T=13:00
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Incremental at T=14:00
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Find restore set up to T=14:00.
	set, err := FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Len(t, set, 3)
	assert.Equal(t, BackupTypeComplete, set[0].Type)
	assert.Equal(t, BackupTypeIncremental, set[1].Type)
	assert.Equal(t, BackupTypeIncremental, set[2].Type)

	// Find restore set up to T=12:30 — only the complete.
	set, err = FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 12, 30, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Len(t, set, 1)
	assert.Equal(t, BackupTypeComplete, set[0].Type)

	// Find restore set before any backup.
	_, err = FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 11, 0, 0, 0, time.UTC))
	assert.ErrorIs(t, err, ErrNoBackupsFound)
}

func TestBackupEmptyDB(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	assert.Greater(t, len(meta.Files), 0) // even empty DB has MANIFEST, CURRENT, etc.
	db.Close()

	// Restore empty DB.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	kvs := collectAllKVs(t, db2)
	// Empty DB still has bond data version key.
	assert.Len(t, kvs, 1)
}

func TestRestoreCreatesMetadata(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// Verify PEBBLE_FORMAT_VERSION file exists and has correct content.
	versionPath := filepath.Join(restoreDir, "bond", "PEBBLE_FORMAT_VERSION")
	data, err := os.ReadFile(versionPath)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%d", meta.PebbleFormatVersion), string(data))
}

func TestRestoreNonEmptyDir(t *testing.T) {
	dir := t.TempDir()

	// Create a file in the directory so it's not empty.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "existing.txt"), []byte("data"), 0644))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	err := Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: dir,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not empty")
}

func TestIncrementalWithoutPreviousBackup(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no previous backup")
}

func TestBackupProgress(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 20)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	var mu sync.Mutex
	var events []ProgressEvent

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		OnProgress: func(event ProgressEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})
	require.NoError(t, err)

	// Should have one event per uploaded file.
	require.Len(t, events, len(meta.Files))

	// Final event should have FilesDone == FilesTotal and BytesDone == BytesTotal.
	last := events[len(events)-1]
	assert.Equal(t, len(meta.Files), last.FilesTotal)

	// Find the event with max FilesDone (could be any due to concurrency).
	var maxFilesDone int
	var maxBytesDone int64
	filesSeen := make(map[string]struct{})
	for _, e := range events {
		filesSeen[e.File] = struct{}{}
		if e.FilesDone > maxFilesDone {
			maxFilesDone = e.FilesDone
		}
		if e.BytesDone > maxBytesDone {
			maxBytesDone = e.BytesDone
		}
	}
	assert.Equal(t, len(meta.Files), maxFilesDone)
	assert.Equal(t, last.BytesTotal, maxBytesDone)

	// All file names should appear in events.
	for _, fi := range meta.Files {
		_, ok := filesSeen[fi.Name]
		assert.True(t, ok, "missing progress event for file %s", fi.Name)
	}
}

func TestRestoreProgress(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Phase 1: Complete backup.
	insertTestData(t, db, 0, 10)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 2: Incremental backup.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix: "backups",
		Type:   BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	db.Close()

	var mu sync.Mutex
	var events []ProgressEvent

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
		OnProgress: func(event ProgressEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})
	require.NoError(t, err)

	require.Greater(t, len(events), 0)

	// The totals should span all backups in the restore set.
	var maxFilesDone int
	var maxBytesDone int64
	for _, e := range events {
		if e.FilesDone > maxFilesDone {
			maxFilesDone = e.FilesDone
		}
		if e.BytesDone > maxBytesDone {
			maxBytesDone = e.BytesDone
		}
	}
	last := events[len(events)-1]
	assert.Equal(t, last.FilesTotal, maxFilesDone)
	assert.Equal(t, last.BytesTotal, maxBytesDone)

	// Verify restored DB is valid.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()
}

func TestBackupProgressCancellation(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 50)

	bucket := objstore.NewInMemBucket()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount atomic.Int64

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:      "backups",
		Type:        BackupTypeComplete,
		At:          time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency: 1, // sequential so cancellation is deterministic
		OnProgress: func(event ProgressEvent) {
			if callCount.Add(1) == 2 {
				cancel()
			}
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, int(callCount.Load()), 50)
}

func TestConcurrencyOne(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 20)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:      "backups",
		Type:        BackupTypeComplete,
		At:          time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency: 1,
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:      "backups",
		RestoreDir:  restoreDir,
		Concurrency: 1,
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestDefaultConcurrency(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 10)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Concurrency: 0 should use DefaultConcurrency without panic.
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:      "backups",
		Type:        BackupTypeComplete,
		At:          time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency: 0,
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:      "backups",
		RestoreDir:  restoreDir,
		Concurrency: 0,
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}
