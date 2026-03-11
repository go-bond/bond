package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestMain(m *testing.M) {
	// Disable lock jitter in tests to avoid unnecessary delays.
	defaultLockJitter = 0
	os.Exit(m.Run())
}

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

	name := backupDirName(ts, BackupTypeComplete, 0)
	assert.Equal(t, "20250212120000-complete-00000", name)

	name = backupDirName(ts, BackupTypeIncremental, 1)
	assert.Equal(t, "20250212120000-incremental-00001", name)

	prefix := backupObjectPrefix("backups", ts, BackupTypeComplete, 0)
	assert.Equal(t, "backups/20250212120000-complete-00000/", prefix)

	// Round-trip
	parsedTime, parsedType, parsedSeq, err := parseBackupDir("20250212120000-complete-00000")
	require.NoError(t, err)
	assert.Equal(t, ts, parsedTime)
	assert.Equal(t, BackupTypeComplete, parsedType)
	assert.Equal(t, 0, parsedSeq)

	parsedTime, parsedType, parsedSeq, err = parseBackupDir("20250212120000-incremental-00001/")
	require.NoError(t, err)
	assert.Equal(t, ts, parsedTime)
	assert.Equal(t, BackupTypeIncremental, parsedType)
	assert.Equal(t, 1, parsedSeq)

	// With path prefix
	parsedTime, parsedType, parsedSeq, err = parseBackupDir("backups/20250212120000-complete-00000/")
	require.NoError(t, err)
	assert.Equal(t, ts, parsedTime)
	assert.Equal(t, BackupTypeComplete, parsedType)
	assert.Equal(t, 0, parsedSeq)

	// Invalid
	_, _, _, err = parseBackupDir("invalid")
	assert.Error(t, err)

	_, _, _, err = parseBackupDir("20250212120000-unknown-00000")
	assert.Error(t, err)

	_, _, _, err = parseBackupDir("20250212120000-complete")
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, meta)

	assert.Equal(t, BackupTypeComplete, meta.Type)
	assert.Equal(t, time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC), meta.Datetime)
	assert.Greater(t, len(meta.Files), 0)
	assert.Equal(t, meta.Files, meta.CheckpointFiles)
	assert.Greater(t, meta.PebbleFormatVersion, uint64(0))
	assert.Equal(t, uint32(bond.BOND_DB_DATA_VERSION), meta.BondDataVersion)

	// Verify meta.json exists in the bucket.
	ok, err := bucket.Exists(ctx, "backups/20250212120000-complete-00000/meta.json")
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Insert more data.
	insertTestData(t, db, 10, 10)

	// Incremental backup.
	incrMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 2: Insert more data, incremental backup.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 3: Insert even more data, another incremental.
	insertTestData(t, db, 20, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	phase1KVs := collectAllKVs(t, db)

	// Phase 2: More data + incremental at T=13:00.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 3: Even more data + incremental at T=14:00.
	insertTestData(t, db, 20, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
			Prefix:        "backups",
			Type:          types[i],
			At:            ts,
			CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental at T=13:00
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental at T=14:00
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 2: Incremental backup.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency:   1, // sequential so cancellation is deterministic
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency:   1,
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency:   0,
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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

// createIncompleteDir uploads files into a backup-like directory but without meta.json.
func createIncompleteDir(t *testing.T, bucket *objstore.InMemBucket, prefix string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, bucket.Upload(ctx, prefix+"000001.sst", bytes.NewReader([]byte("fake-sst"))))
	require.NoError(t, bucket.Upload(ctx, prefix+"MANIFEST-000001", bytes.NewReader([]byte("fake-manifest"))))
}

func TestListBackups_SkipsIncomplete(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Create a valid backup.
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Create an incomplete directory (no meta.json).
	createIncompleteDir(t, bucket, "backups/20250212130000-incremental-00001/")

	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, BackupTypeComplete, backups[0].Type)
}

func TestRemoveIncompleteBackups(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Create a valid backup.
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Create two incomplete directories.
	createIncompleteDir(t, bucket, "backups/20250212130000-incremental-00001/")
	createIncompleteDir(t, bucket, "backups/20250212140000-complete-00000/")

	removed, err := RemoveIncompleteBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	assert.Equal(t, 2, removed)

	// Valid backup should still be there.
	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, BackupTypeComplete, backups[0].Type)

	// Incomplete backup files should be gone.
	exists, err := bucket.Exists(ctx, "backups/20250212130000-incremental-00001/000001.sst")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestRemoveIncompleteBackups_NoIncomplete(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	removed, err := RemoveIncompleteBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	assert.Equal(t, 0, removed)
}

func TestBackup_CleansIncompleteBeforeIncremental(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 10)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup at T=12:00.
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Create an incomplete incremental that would be "latest" by datetime.
	createIncompleteDir(t, bucket, "backups/20250212130000-incremental-00001/")

	// Insert more data, take a new incremental at T=14:00.
	insertTestData(t, db, 10, 10)
	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, meta)

	// Incomplete backup should be cleaned up.
	exists, err := bucket.Exists(ctx, "backups/20250212130000-incremental-00001/000001.sst")
	require.NoError(t, err)
	assert.False(t, exists)

	// Should have exactly 2 valid backups.
	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	assert.Len(t, backups, 2)
}

func TestRestore_SkipsIncomplete(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 10)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup at T=12:00.
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	// Create an incomplete incremental.
	createIncompleteDir(t, bucket, "backups/20250212130000-incremental-00001/")

	// Restore should succeed, using only the valid complete backup.
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

	// Incomplete backup should still exist (restore does not clean up).
	exists, err := bucket.Exists(ctx, "backups/20250212130000-incremental-00001/000001.sst")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestBackup_LockPreventsConcurrent(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Manually acquire a lock.
	err := acquireLock(ctx, bucket, "backups", DefaultLockTTL)
	require.NoError(t, err)

	// Attempt a backup — should fail with ErrBackupInProgress.
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	assert.ErrorIs(t, err, ErrBackupInProgress)
}

func TestBackup_StaleLockIsOverridden(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Upload a lock with an old timestamp.
	oldPayload := []byte(`{"created_at":"2020-01-01T00:00:00Z"}`)
	require.NoError(t, bucket.Upload(ctx, "backups/.lock", bytes.NewReader(oldPayload)))

	// Backup with a short TTL should succeed (stale lock overridden).
	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		LockTTL:       1 * time.Millisecond,
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestBackup_LockReleasedOnError(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after first file — upload of remaining files should fail.
	var called atomic.Int64
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		Concurrency:   1,
		CheckpointDir: filepath.Join(dir, "checkpoint"),
		OnProgress: func(event ProgressEvent) {
			if called.Add(1) == 1 {
				cancel()
			}
		},
	})
	require.Error(t, err)

	// Lock should be released via defer.
	exists, err := bucket.Exists(context.Background(), "backups/.lock")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestBackup_LockReleasedOnSuccess(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Lock should not exist after successful backup.
	exists, err := bucket.Exists(ctx, "backups/.lock")
	require.NoError(t, err)
	assert.False(t, exists)
}

// --- Checkpoint utility tests ---

func TestHasCheckpoint_NoCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nonexistent")

	has, err := HasCheckpoint(dir)
	require.NoError(t, err)
	assert.False(t, has)
}

func TestHasCheckpoint_WithCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, os.Mkdir(dir, 0755))

	has, err := HasCheckpoint(dir)
	require.NoError(t, err)
	assert.True(t, has)
}

func TestRemoveCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, os.Mkdir(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file.sst"), []byte("data"), 0644))

	err := RemoveCheckpoint(dir)
	require.NoError(t, err)

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
}

func TestRemoveCheckpoint_NoCheckpoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nonexistent")

	err := RemoveCheckpoint(dir)
	require.NoError(t, err)
}

func TestResolvePerStreamRate(t *testing.T) {
	// Zero rateBPS -> default / concurrency
	rate := resolvePerStreamRate(0, DefaultMaxUploadBPS, 4)
	assert.Equal(t, float64(DefaultMaxUploadBPS)/4, rate)

	// Positive rateBPS -> rateBPS / concurrency
	rate = resolvePerStreamRate(50*1024*1024, DefaultMaxUploadBPS, 5)
	assert.Equal(t, float64(50*1024*1024)/5, rate)

	// Negative rateBPS -> disabled (0)
	rate = resolvePerStreamRate(-1, DefaultMaxUploadBPS, 4)
	assert.Equal(t, float64(0), rate)

	// Zero concurrency -> DefaultConcurrency
	rate = resolvePerStreamRate(80*1024*1024, DefaultMaxDownloadBPS, 0)
	assert.Equal(t, float64(80*1024*1024)/float64(DefaultConcurrency), rate)

	// Both zero -> default / DefaultConcurrency
	rate = resolvePerStreamRate(0, DefaultMaxDownloadBPS, 0)
	assert.Equal(t, float64(DefaultMaxDownloadBPS)/float64(DefaultConcurrency), rate)
}

func TestBackupWithRateLimit(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 20)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
		MaxUploadBPS:  50 * 1024 * 1024, // 50 MB/s
	})
	require.NoError(t, err)
	db.Close()

	// Restore and verify data integrity.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:         "backups",
		RestoreDir:     restoreDir,
		MaxDownloadBPS: -1, // disable for speed
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestRestoreWithRateLimit(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 20)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
		MaxUploadBPS:  -1, // disable for speed
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:         "backups",
		RestoreDir:     restoreDir,
		MaxDownloadBPS: 50 * 1024 * 1024, // 50 MB/s
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestBackupNoRateLimit(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 20)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
		MaxUploadBPS:  -1, // disabled
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:         "backups",
		RestoreDir:     restoreDir,
		MaxDownloadBPS: -1, // disabled
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestBackup_CleansStaleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	checkpointDir := filepath.Join(dir, "checkpoint")

	// Simulate a stale checkpoint from a previous crash.
	require.NoError(t, os.Mkdir(checkpointDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "stale.sst"), []byte("stale"), 0644))

	has, err := HasCheckpoint(checkpointDir)
	require.NoError(t, err)
	assert.True(t, has)

	// Backup should clean the stale checkpoint before proceeding.
	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: checkpointDir,
	})
	require.NoError(t, err)
	require.NotNil(t, meta)

	// After backup, the checkpoint dir should be cleaned up.
	has, err = HasCheckpoint(checkpointDir)
	require.NoError(t, err)
	assert.False(t, has)
}

// --- Upload retry tests ---

// temporaryError is an error that implements Temporary() for retry tests.
type temporaryError struct{ msg string }

func (e *temporaryError) Error() string   { return e.msg }
func (e *temporaryError) Temporary() bool { return true }

// uploadFailingBucket wraps InMemBucket and fails the first N upload attempts per key.
type uploadFailingBucket struct {
	*objstore.InMemBucket
	mu           sync.Mutex
	attempts     map[string]int // key -> number of Upload attempts so far
	failAttempts int           // fail first N attempts per key with retryable error
	alwaysFail   bool          // if true, always return permanent error (no delegation)
}

// isBackupCheckpointFile returns true for keys that are checkpoint file uploads (not lock or meta.json).
// Only those uploads use the retry path in Backup(); lock and writeMeta use the bucket directly.
func isBackupCheckpointFile(name string) bool {
	if strings.HasSuffix(name, "/meta.json") {
		return false
	}
	return strings.Contains(name, "-complete-") || strings.Contains(name, "-incremental-")
}

func (b *uploadFailingBucket) Upload(ctx context.Context, name string, r io.Reader, _ ...objstore.ObjectUploadOption) error {
	// Only fail checkpoint file uploads; let lock and meta uploads succeed so Backup() can proceed.
	if !isBackupCheckpointFile(name) {
		return b.InMemBucket.Upload(ctx, name, r)
	}
	b.mu.Lock()
	n := b.attempts[name]
	b.attempts[name]++
	failAttempts := b.failAttempts
	alwaysFail := b.alwaysFail
	b.mu.Unlock()

	if alwaysFail {
		return fmt.Errorf("access denied")
	}
	if n < failAttempts {
		return &temporaryError{msg: "transient upload failure"}
	}
	return b.InMemBucket.Upload(ctx, name, r)
}

func TestBackup_UploadRetry_SuccessAfterRetries(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	// Fail first 2 attempts per key, then succeed. Default MaxUploadRetries is 3, so we have enough.
	bucket := &uploadFailingBucket{
		InMemBucket:   objstore.NewInMemBucket(),
		attempts:      make(map[string]int),
		failAttempts:  2,
	}

	ctx := context.Background()
	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, meta)

	// Verify backup is complete and restorable.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{Prefix: "backups", RestoreDir: restoreDir})
	require.NoError(t, err)
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()
	restored := collectAllKVs(t, db2)
	original := collectAllKVs(t, db)
	assert.Equal(t, original, restored)
}

func TestBackup_UploadRetry_PermanentErrorFails(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := &uploadFailingBucket{
		InMemBucket: objstore.NewInMemBucket(),
		attempts:    make(map[string]int),
		alwaysFail:  true,
	}

	ctx := context.Background()
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload ")
	assert.Contains(t, err.Error(), "access denied")
}

func TestBackup_UploadRetry_ContextCancelDuringBackoff(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	// Fail first attempt for every key so we enter backoff. Use long backoff so cancel happens during wait.
	bucket := &uploadFailingBucket{
		InMemBucket:   objstore.NewInMemBucket(),
		attempts:      make(map[string]int),
		failAttempts:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel shortly after backup starts so we hit backoff then context done.
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:              "backups",
		Type:                BackupTypeComplete,
		At:                  time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir:       filepath.Join(dir, "checkpoint"),
		InitialRetryBackoff: 200 * time.Millisecond, // long enough that cancel fires during backoff
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context cancellation: %v", err)
}

func TestIsRetriableError(t *testing.T) {
	// Not retryable: nil, Canceled
	assert.False(t, isRetryableError(nil))
	assert.False(t, isRetryableError(context.Canceled))

	// Retryable: DeadlineExceeded, Temporary()
	assert.True(t, isRetryableError(context.DeadlineExceeded))
	assert.True(t, isRetryableError(&temporaryError{msg: "x"}))

	// Not retryable: permanent error
	assert.False(t, isRetryableError(fmt.Errorf("access denied")))
}

// --- Restore .incomplete marker tests ---

func TestRestore_IncompleteMarkerRemovedOnSuccess(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 10)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// .incomplete marker should NOT exist after a successful restore.
	incomplete, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.False(t, incomplete)

	// DB should be openable.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()
}

func TestRestore_IncompleteMarkerCleansInterruptedRestore(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 20)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")

	// Simulate an interrupted restore: create the directory with leftover
	// files and a .incomplete marker.
	require.NoError(t, os.MkdirAll(restoreDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(restoreDir, "leftover.sst"), []byte("partial"), 0644))
	require.NoError(t, writeRestoreIncompleteMarker(restoreDir))

	// Restore should detect the .incomplete marker, clean the directory, and succeed.
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// .incomplete marker should be gone.
	incomplete, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.False(t, incomplete)

	// Leftover file should be gone.
	_, err = os.Stat(filepath.Join(restoreDir, "leftover.sst"))
	assert.True(t, os.IsNotExist(err))

	// Restored DB should be valid.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

func TestRestore_NonEmptyDirWithoutIncompleteMarkerFails(t *testing.T) {
	dir := t.TempDir()

	// Create a file but NO .incomplete marker — this should fail as before.
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

func TestRestore_CancelledLeavesIncompleteMarker(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 50)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")

	// Cancel after first file download.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount atomic.Int64
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:      "backups",
		RestoreDir:  restoreDir,
		Concurrency: 1,
		OnProgress: func(event ProgressEvent) {
			if callCount.Add(1) == 1 {
				cancel()
			}
		},
	})
	require.Error(t, err)

	// .incomplete marker should still be present after a failed restore.
	incomplete, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.True(t, incomplete)
}

func TestRestore_RetryAfterCancelledRestore(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 50)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")

	// First restore: cancel mid-way.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	var callCount atomic.Int64
	err = Restore(ctx1, bucket, RestoreOptions{
		Prefix:      "backups",
		RestoreDir:  restoreDir,
		Concurrency: 1,
		OnProgress: func(event ProgressEvent) {
			if callCount.Add(1) == 1 {
				cancel1()
			}
		},
	})
	require.Error(t, err)
	incomplete, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.True(t, incomplete)

	// Second restore: should detect .incomplete, clean up, and succeed.
	err = Restore(context.Background(), bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)
	incomplete2, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.False(t, incomplete2)

	// Verify data integrity.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}

// --- Download retry tests ---

// downloadFailingBucket wraps InMemBucket and fails the first N Get() attempts per key.
type downloadFailingBucket struct {
	*objstore.InMemBucket
	mu           sync.Mutex
	attempts     map[string]int
	failAttempts int  // fail first N Get() attempts per key with retryable error
	alwaysFail   bool // if true, always return permanent error
}

// isRestoreDataFile returns true for keys that are data file downloads (not meta.json).
func isRestoreDataFile(name string) bool {
	if strings.HasSuffix(name, "/meta.json") {
		return false
	}
	return strings.Contains(name, "-complete-") || strings.Contains(name, "-incremental-")
}

func (b *downloadFailingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if !isRestoreDataFile(name) {
		return b.InMemBucket.Get(ctx, name)
	}
	b.mu.Lock()
	n := b.attempts[name]
	b.attempts[name]++
	failAttempts := b.failAttempts
	alwaysFail := b.alwaysFail
	b.mu.Unlock()

	if alwaysFail {
		return nil, fmt.Errorf("access denied")
	}
	if n < failAttempts {
		return nil, &temporaryError{msg: "transient download failure"}
	}
	return b.InMemBucket.Get(ctx, name)
}

func TestRestore_DownloadRetry_SuccessAfterRetries(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 10)
	originalKVs := collectAllKVs(t, db)

	inner := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, inner, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	bucket := &downloadFailingBucket{
		InMemBucket:  inner,
		attempts:     make(map[string]int),
		failAttempts: 2,
	}

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

func TestRestore_DownloadRetry_PermanentErrorFails(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	inner := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, inner, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	bucket := &downloadFailingBucket{
		InMemBucket: inner,
		attempts:    make(map[string]int),
		alwaysFail:  true,
	}

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download ")
	assert.Contains(t, err.Error(), "access denied")
}

func TestRestore_DownloadRetry_ContextCancelDuringBackoff(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	inner := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, inner, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	bucket := &downloadFailingBucket{
		InMemBucket:  inner,
		attempts:     make(map[string]int),
		failAttempts: 1,
	}

	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx2, bucket, RestoreOptions{
		Prefix:              "backups",
		RestoreDir:          restoreDir,
		InitialRetryBackoff: 200 * time.Millisecond,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context cancellation: %v", err)
}

// --- Lock refresh failure test ---

// lockRefreshFailBucket wraps InMemBucket and fails lock upload after initial acquisition.
// Non-lock uploads are artificially delayed so the backup cannot finish before the
// lock refresh goroutine detects the failure and cancels the context.
type lockRefreshFailBucket struct {
	*objstore.InMemBucket
	mu              sync.Mutex
	lockUploadCount int
}

func (b *lockRefreshFailBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/.lock") {
		b.mu.Lock()
		b.lockUploadCount++
		count := b.lockUploadCount
		b.mu.Unlock()

		// Allow the first two lock uploads (acquire + verify write), fail all subsequent (refresh).
		if count > 2 {
			return fmt.Errorf("simulated lock refresh failure")
		}
	} else {
		// Delay non-lock uploads so the backup takes longer than the lock TTL,
		// giving the refresh goroutine time to detect failure and cancel.
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.InMemBucket.Upload(ctx, name, r, opts...)
}

func TestBackup_LockRefreshFailure(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 50)

	bucket := &lockRefreshFailBucket{
		InMemBucket: objstore.NewInMemBucket(),
	}

	ctx := context.Background()
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
		LockTTL:       10 * time.Millisecond,
		Concurrency:   1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLockRefreshFailed), "expected ErrLockRefreshFailed, got: %v", err)

	// Lock should NOT be released (another process may own it).
	exists, err := bucket.Exists(context.Background(), "backups/.lock")
	require.NoError(t, err)
	assert.True(t, exists, "lock should remain because refresh failed")
}

// --- DeleteBackup tests ---

func TestDeleteBackup(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Verify backup exists.
	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	require.Len(t, backups, 1)

	err = DeleteBackup(ctx, bucket, backups[0].Prefix)
	require.NoError(t, err)

	// All objects under the backup prefix should be gone.
	remaining, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	assert.Len(t, remaining, 0)
}

func TestDeleteBackup_AlreadyDeleted(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Deleting a nonexistent prefix should not error.
	err := DeleteBackup(ctx, bucket, "backups/20250212120000-complete-00000/")
	require.NoError(t, err)
}

// --- Multiple complete backups restore test ---

func TestRestoreWithMultipleCompleteBackups(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Phase 1: Initial data + complete backup at T=12:00.
	insertTestData(t, db, 0, 10)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 2: More data + incremental at T=13:00.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 3: More data + second complete backup at T=14:00.
	insertTestData(t, db, 20, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 4: More data + incremental at T=15:00.
	insertTestData(t, db, 30, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 15, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	originalKVs := collectAllKVs(t, db)
	db.Close()

	// FindRestoreSet should pick [complete2, incr2], not [complete1, incr1, complete2, incr2].
	set, err := FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 15, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Len(t, set, 2)
	assert.Equal(t, BackupTypeComplete, set[0].Type)
	assert.Equal(t, time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC), set[0].Datetime)
	assert.Equal(t, BackupTypeIncremental, set[1].Type)

	// Restore and verify data integrity.
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

// --- Corrupt lock test ---

func TestBackup_CorruptLockIsOverridden(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Upload corrupt (non-JSON) lock data.
	require.NoError(t, bucket.Upload(ctx, "backups/.lock", bytes.NewReader([]byte("not-json"))))

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	require.NotNil(t, meta)
}

// --- Input validation tests ---

func TestBackup_EmptyCheckpointDir(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: "",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CheckpointDir is required")
}

func TestRestore_EmptyRestoreDir(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	err := Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: "",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RestoreDir must be specified")
}

// --- ReadBackupMeta tests ---

func TestReadBackupMeta(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	backupMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Read the meta using the public API.
	readResult, err := ReadBackupMeta(ctx, bucket, "backups/20250212120000-complete-00000/")
	require.NoError(t, err)

	assert.Equal(t, backupMeta.Type, readResult.Type)
	assert.Equal(t, backupMeta.Datetime, readResult.Datetime)
	assert.Equal(t, backupMeta.PebbleFormatVersion, readResult.PebbleFormatVersion)
	assert.Equal(t, backupMeta.BondDataVersion, readResult.BondDataVersion)
	assert.Equal(t, backupMeta.Files, readResult.Files)
	assert.Equal(t, backupMeta.CheckpointFiles, readResult.CheckpointFiles)
}

func TestReadBackupMeta_NotFound(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := ReadBackupMeta(ctx, bucket, "backups/nonexistent/")
	require.Error(t, err)
}

// --- Default values tests ---

func TestBackup_DefaultTimestamp(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	before := time.Now().UTC()
	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	after := time.Now().UTC()

	// Verify the datetime is within the test window.
	assert.False(t, meta.Datetime.Before(before.Truncate(time.Second)), "meta datetime %v should not be before %v", meta.Datetime, before)
	assert.False(t, meta.Datetime.After(after.Add(time.Second)), "meta datetime %v should not be after %v", meta.Datetime, after)
}

func TestBackup_DefaultType(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          "",
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	assert.Equal(t, BackupTypeComplete, meta.Type)
}

// --- Edge case tests ---

func TestListBackups_Empty(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	assert.Empty(t, backups)
}

func TestFindRestoreSet_ExactBoundary(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete at T=12:00.
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental at T=13:00.
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental at T=14:00.
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Before=T=13:00 exactly should include T=12 complete + T=13 incremental.
	set, err := FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Len(t, set, 2)
	assert.Equal(t, BackupTypeComplete, set[0].Type)
	assert.Equal(t, time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC), set[0].Datetime)
	assert.Equal(t, BackupTypeIncremental, set[1].Type)
	assert.Equal(t, time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC), set[1].Datetime)
}

func TestFindRestoreSet_MissingIncremental(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete at T=12:00 (seq=0).
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental at T=13:00 (seq=1).
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental at T=14:00 (seq=2).
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Delete the middle incremental (seq=1) to create a gap.
	err = DeleteBackup(ctx, bucket, "backups/20250212130000-incremental-00001/")
	require.NoError(t, err)

	// FindRestoreSet should detect the gap and return ErrIncompleteRestoreChain.
	_, err = FindRestoreSet(ctx, bucket, "backups", time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC))
	assert.ErrorIs(t, err, ErrIncompleteRestoreChain)
}

func TestHasCheckpoint_FileNotDir(t *testing.T) {
	p := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, os.WriteFile(p, []byte("not a directory"), 0644))

	has, err := HasCheckpoint(p)
	require.NoError(t, err)
	assert.False(t, has, "HasCheckpoint should return false for a regular file")
}

func TestRestore_CreatesNestedDir(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 5)
	originalKVs := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	// Restore to a deeply nested directory that doesn't exist yet.
	restoreDir := filepath.Join(dir, "a", "b", "c", "restored")
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

// --- Incremental diff correctness test ---

func TestBackup_IncrementalDiffCorrectness(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 10)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup.
	completeMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Build a set of complete backup's checkpoint files.
	completeFileSet := make(map[string]int64, len(completeMeta.CheckpointFiles))
	for _, f := range completeMeta.CheckpointFiles {
		completeFileSet[f.Name] = f.Size
	}

	// Insert more data to force new/changed SST files.
	insertTestData(t, db, 10, 100)

	// Incremental backup.
	incrMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Every file in the incremental's Files list should be either:
	// (a) not present in the complete's CheckpointFiles, OR
	// (b) present but with a different size.
	for _, f := range incrMeta.Files {
		prevSize, existed := completeFileSet[f.Name]
		if existed {
			assert.NotEqual(t, prevSize, f.Size,
				"incremental file %s has same size as complete (%d), should have been excluded", f.Name, f.Size)
		}
	}

	// CheckpointFiles should contain all files at this checkpoint, not just the diff.
	assert.GreaterOrEqual(t, len(incrMeta.CheckpointFiles), len(incrMeta.Files),
		"CheckpointFiles should have at least as many files as Files (diff)")
}

func TestValidateFileName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "simple file", input: "000001.sst", wantErr: false},
		{name: "nested path", input: "subdir/file.sst", wantErr: false},
		{name: "deeply nested", input: "a/b/c/file.sst", wantErr: false},
		{name: "empty", input: "", wantErr: true},
		{name: "absolute unix", input: "/etc/passwd", wantErr: true},
		{name: "dotdot only", input: "..", wantErr: true},
		{name: "dotdot prefix", input: "../etc/passwd", wantErr: true},
		{name: "double dotdot", input: "../../etc/passwd", wantErr: true},
		{name: "dotdot mid-path", input: "subdir/../../etc/passwd", wantErr: true},
		{name: "dotdot at end", input: "subdir/..", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFileName(tt.input)
			if tt.wantErr {
				assert.Error(t, err, "expected error for %q", tt.input)
			} else {
				assert.NoError(t, err, "unexpected error for %q", tt.input)
			}
		})
	}
}

func TestValidateMetaFileNames_RejectsTamperedMeta(t *testing.T) {
	meta := &BackupMeta{
		Files: []FileInfo{
			{Name: "000001.sst", Size: 100},
			{Name: "../../etc/passwd", Size: 50},
		},
	}
	err := validateMetaFileNames(meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path traversal")
}

func TestValidateMetaFileNames_RejectsInCheckpointFiles(t *testing.T) {
	meta := &BackupMeta{
		Files: []FileInfo{{Name: "000001.sst", Size: 100}},
		CheckpointFiles: []FileInfo{
			{Name: "000001.sst", Size: 100},
			{Name: "../escape.txt", Size: 10},
		},
	}
	err := validateMetaFileNames(meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path traversal")
}

// --- UUID chain integrity tests ---

func TestBackupCompleteGeneratesUUID(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, meta.UUID)

	// Verify local bond/meta.json exists with matching UUID.
	localMeta, err := readLocalMeta(db.Dir())
	require.NoError(t, err)
	require.NotNil(t, localMeta)
	assert.Equal(t, meta.UUID, localMeta.UUID)
}

func TestBackupIncrementalValidatesUUID(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	completeMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	insertTestData(t, db, 5, 5)

	incrMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental should have a different UUID than the complete.
	assert.NotEqual(t, completeMeta.UUID, incrMeta.UUID)
	assert.NotEmpty(t, incrMeta.UUID)

	// Local meta.json should be updated with the incremental UUID.
	localMeta, err := readLocalMeta(db.Dir())
	require.NoError(t, err)
	require.NotNil(t, localMeta)
	assert.Equal(t, incrMeta.UUID, localMeta.UUID)
}

func TestBackupIncrementalFailsUUIDMismatch(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Tamper with local meta.json UUID.
	localMeta, err := readLocalMeta(db.Dir())
	require.NoError(t, err)
	localMeta.UUID = "tampered-uuid"
	err = writeLocalMeta(db.Dir(), localMeta)
	require.NoError(t, err)

	insertTestData(t, db, 5, 5)

	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrChainBroken)
}

func TestBackupIncrementalFailsNoLocalMeta(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Delete the local meta.json.
	err = os.Remove(filepath.Join(db.Dir(), "bond", "meta.json"))
	require.NoError(t, err)

	insertTestData(t, db, 5, 5)

	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrChainBroken)
}

func TestBackupIncrementalFailsLegacyBucketMeta(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))
	defer db.Close()

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Rewrite bucket meta.json to remove UUID (simulate legacy backup).
	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	require.Len(t, backups, 1)

	bucketMeta, err := ReadBackupMeta(ctx, bucket, backups[0].Prefix)
	require.NoError(t, err)
	bucketMeta.UUID = "" // remove UUID
	data, err := json.MarshalIndent(bucketMeta, "", "  ")
	require.NoError(t, err)
	metaPath := backups[0].Prefix + "meta.json"
	require.NoError(t, bucket.Upload(ctx, metaPath, bytes.NewReader(data)))

	insertTestData(t, db, 5, 5)

	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrChainBroken)
}

func TestRestoreWritesLocalMeta(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 5)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// Verify bond/meta.json exists with correct UUID.
	localMeta, err := readLocalMeta(restoreDir)
	require.NoError(t, err)
	require.NotNil(t, localMeta)
	assert.Equal(t, meta.UUID, localMeta.UUID)
}

func TestRestoreThenIncrementalSameBucket(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 10)

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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

	// Open the restored DB and do an incremental backup to the same bucket.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	insertTestData(t, db2, 10, 5)

	_, err = Backup(ctx, db2, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint2"),
	})
	require.NoError(t, err)
}

func TestRestoreThenIncrementalDifferentBucket(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	insertTestData(t, db, 0, 10)

	bucketA := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup to bucket A.
	_, err := Backup(ctx, db, bucketA, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	// Restore from bucket A.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucketA, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// Open restored DB and try incremental to bucket B (different bucket).
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	bucketB := objstore.NewInMemBucket()

	// First, do a complete backup to bucket B so there's a previous backup.
	_, err = Backup(ctx, db2, bucketB, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint2"),
	})
	require.NoError(t, err)

	// Now the local meta has the UUID from the bucket B complete backup.
	// Do another complete to bucket A to change bucket A's latest UUID.
	db3 := openTestDB(t, filepath.Join(dir, "db3"))
	defer db3.Close()
	insertTestData(t, db3, 0, 5)

	_, err = Backup(ctx, db3, bucketA, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 3, 1, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint3"),
	})
	require.NoError(t, err)

	// Now db2's local UUID (from bucket B complete) doesn't match bucket A's latest.
	insertTestData(t, db2, 10, 5)

	_, err = Backup(ctx, db2, bucketA, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 3, 1, 15, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint4"),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrChainBroken)
}

// --- Optimized incremental restore tests ---

// countingBucket wraps InMemBucket and counts per-key data file downloads.
type countingBucket struct {
	*objstore.InMemBucket
	mu        sync.Mutex
	downloads map[string]int
}

func newCountingBucket(inner *objstore.InMemBucket) *countingBucket {
	return &countingBucket{
		InMemBucket: inner,
		downloads:   make(map[string]int),
	}
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if isRestoreDataFile(name) {
		b.mu.Lock()
		b.downloads[name]++
		b.mu.Unlock()
	}
	return b.InMemBucket.Get(ctx, name)
}

func (b *countingBucket) totalDataDownloads() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, n := range b.downloads {
		total += n
	}
	return total
}

func TestOptimizedRestore_MatchesLegacy(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Phase 1: 100 KVs, complete backup.
	insertTestData(t, db, 0, 100)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 2: 100 more KVs, incremental.
	insertTestData(t, db, 100, 100)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 3: 100 more KVs, incremental.
	insertTestData(t, db, 200, 100)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	originalKVs := collectAllKVs(t, db)
	db.Close()

	// Restore and verify.
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

func TestOptimizedRestore_NoRedundantDownloads(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	inner := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup.
	insertTestData(t, db, 0, 50)
	_, err := Backup(ctx, db, inner, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental backup.
	insertTestData(t, db, 50, 50)
	lastMeta, err := Backup(ctx, db, inner, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	cb := newCountingBucket(inner)

	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, cb, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	// Each file should be downloaded exactly once.
	cb.mu.Lock()
	for key, count := range cb.downloads {
		assert.Equal(t, 1, count, "file %s downloaded %d times", key, count)
	}
	cb.mu.Unlock()

	// Total downloads should equal the checkpoint file count.
	assert.Equal(t, len(lastMeta.CheckpointFiles), cb.totalDataDownloads())

	// Verify data integrity.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()
}

func TestOptimizedRestore_SingleComplete(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	insertTestData(t, db, 0, 50)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	originalKVs := collectAllKVs(t, db)
	db.Close()

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

func TestOptimizedRestore_PointInTime(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Phase 1: Complete at T=12:00.
	insertTestData(t, db, 0, 50)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Phase 2: Incremental at T=13:00.
	insertTestData(t, db, 50, 50)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	phase2KVs := collectAllKVs(t, db)

	// Phase 3: Incremental at T=14:00.
	insertTestData(t, db, 100, 50)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	// Restore with Before=13:30, should include complete + first incremental.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
		Before:     time.Date(2025, 2, 12, 13, 30, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, phase2KVs, restoredKVs)
}

func TestOptimizedRestore_CorruptChain(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup.
	insertTestData(t, db, 0, 10)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental backup.
	insertTestData(t, db, 10, 10)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)
	db.Close()

	// Tamper with the last backup's meta.json: add a phantom file to CheckpointFiles.
	backups, err := ListBackups(ctx, bucket, "backups")
	require.NoError(t, err)
	lastBackup := backups[len(backups)-1]

	meta, err := ReadBackupMeta(ctx, bucket, lastBackup.Prefix)
	require.NoError(t, err)
	meta.CheckpointFiles = append(meta.CheckpointFiles, FileInfo{Name: "phantom.sst", Size: 999})

	tamperedData, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, bucket.Upload(ctx, lastBackup.Prefix+"meta.json", bytes.NewReader(tamperedData)))

	// Restore should fail with corrupt chain error.
	restoreDir := filepath.Join(dir, "restored")
	err = Restore(ctx, bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt backup chain")
	assert.Contains(t, err.Error(), "phantom.sst")
}

func TestResolveFileSources_BackwardPriority(t *testing.T) {
	backups := []backupWithMeta{
		{
			prefix: "backup0/",
			meta: &BackupMeta{
				Files: []FileInfo{
					{Name: "MANIFEST", Size: 100},
					{Name: "000001.sst", Size: 200},
				},
			},
		},
		{
			prefix: "backup1/",
			meta: &BackupMeta{
				Files: []FileInfo{
					{Name: "MANIFEST", Size: 150},
					{Name: "000002.sst", Size: 300},
				},
			},
		},
	}

	required := []FileInfo{
		{Name: "MANIFEST", Size: 150},
		{Name: "000001.sst", Size: 200},
		{Name: "000002.sst", Size: 300},
	}

	resolved, err := resolveFileSources(required, backups)
	require.NoError(t, err)
	require.Len(t, resolved, 3)

	// Build lookup by file name.
	byName := make(map[string]fileSource)
	for _, rs := range resolved {
		byName[rs.File.Name] = rs
	}

	// MANIFEST should come from backup1 (most recent).
	assert.Equal(t, "backup1/", byName["MANIFEST"].BackupPrefix)
	// 000001.sst only in backup0.
	assert.Equal(t, "backup0/", byName["000001.sst"].BackupPrefix)
	// 000002.sst only in backup1.
	assert.Equal(t, "backup1/", byName["000002.sst"].BackupPrefix)
}

func TestResolveFileSources_AllFromComplete(t *testing.T) {
	backups := []backupWithMeta{
		{
			prefix: "complete/",
			meta: &BackupMeta{
				Files: []FileInfo{
					{Name: "MANIFEST", Size: 100},
					{Name: "000001.sst", Size: 200},
					{Name: "CURRENT", Size: 10},
				},
			},
		},
	}

	required := []FileInfo{
		{Name: "MANIFEST", Size: 100},
		{Name: "000001.sst", Size: 200},
		{Name: "CURRENT", Size: 10},
	}

	resolved, err := resolveFileSources(required, backups)
	require.NoError(t, err)
	require.Len(t, resolved, 3)

	for _, rs := range resolved {
		assert.Equal(t, "complete/", rs.BackupPrefix, "file %s should resolve to complete backup", rs.File.Name)
	}
}

func TestResolveFileSources_SizeMismatch(t *testing.T) {
	backups := []backupWithMeta{
		{
			prefix: "backup0/",
			meta: &BackupMeta{
				Files: []FileInfo{
					{Name: "000001.sst", Size: 999}, // wrong size
				},
			},
		},
	}

	required := []FileInfo{
		{Name: "000001.sst", Size: 200},
	}

	_, err := resolveFileSources(required, backups)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt backup chain")
	assert.Contains(t, err.Error(), "000001.sst")
}

func TestOptimizedRestore_Progress(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete backup.
	insertTestData(t, db, 0, 20)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	// Incremental backup.
	insertTestData(t, db, 20, 20)
	lastMeta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
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

	// FilesTotal should equal len(CheckpointFiles).
	require.Greater(t, len(events), 0)
	assert.Equal(t, len(lastMeta.CheckpointFiles), events[0].FilesTotal)

	// One event per file.
	assert.Equal(t, len(lastMeta.CheckpointFiles), len(events))

	// All checkpoint files should appear in events.
	filesSeen := make(map[string]struct{})
	for _, e := range events {
		filesSeen[e.File] = struct{}{}
	}
	for _, fi := range lastMeta.CheckpointFiles {
		_, ok := filesSeen[fi.Name]
		assert.True(t, ok, "missing progress event for file %s", fi.Name)
	}
}

func TestOptimizedRestore_InterruptAndRetry(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(dir, "db"))

	bucket := objstore.NewInMemBucket()
	ctx := context.Background()

	// Complete + incremental.
	insertTestData(t, db, 0, 50)
	_, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeComplete,
		At:            time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	insertTestData(t, db, 50, 50)
	_, err = Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "backups",
		Type:          BackupTypeIncremental,
		At:            time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
		CheckpointDir: filepath.Join(dir, "checkpoint"),
	})
	require.NoError(t, err)

	originalKVs := collectAllKVs(t, db)
	db.Close()

	restoreDir := filepath.Join(dir, "restored")

	// First restore: cancel after first file.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	var callCount atomic.Int64
	err = Restore(ctx1, bucket, RestoreOptions{
		Prefix:      "backups",
		RestoreDir:  restoreDir,
		Concurrency: 1,
		OnProgress: func(event ProgressEvent) {
			if callCount.Add(1) == 1 {
				cancel1()
			}
		},
	})
	require.Error(t, err)

	// .incomplete marker should be present.
	incomplete, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.True(t, incomplete)

	// Second restore: should clean up and succeed.
	err = Restore(context.Background(), bucket, RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	})
	require.NoError(t, err)

	incomplete2, err := HasIncompleteRestore(restoreDir)
	require.NoError(t, err)
	assert.False(t, incomplete2)

	// Verify data integrity.
	db2 := openTestDB(t, restoreDir)
	defer db2.Close()

	restoredKVs := collectAllKVs(t, db2)
	assert.Equal(t, originalKVs, restoredKVs)
}
