package bond_tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// Token is a simple struct for multi-table tests. The identically named type in
// bond/types_test.go lives in package bond and is inaccessible from bond_tests.
type Token struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
}

const (
	TokenBalanceTableID   bond.TableID = 0xC0
	TokenTableID          bond.TableID = 0xC1
	AccountAddressIndexID bond.IndexID = 1
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func openTestDB(t *testing.T, dir string) bond.DB {
	t.Helper()
	opts := bond.DefaultOptions(bond.MediumPerformance)
	db, err := bond.Open(dir, opts)
	require.NoError(t, err)
	return db
}

func setupTokenBalanceTable(db bond.DB) bond.Table[*TokenBalance] {
	return bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
}

func setupTokenTable(db bond.DB) bond.Table[*Token] {
	return bond.NewTable[*Token](bond.TableOptions[*Token]{
		DB:        db,
		TableID:   TokenTableID,
		TableName: "token",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tk *Token) []byte {
			return builder.AddUint64Field(tk.ID).Bytes()
		},
	})
}

func newAccountAddressIndex() *bond.Index[*TokenBalance] {
	return bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
		IndexID:   AccountAddressIndexID,
		IndexName: "account_address_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
	})
}

func insertTokenBalances(t *testing.T, ctx context.Context, table bond.Table[*TokenBalance], start, count int) []*TokenBalance {
	t.Helper()
	records := make([]*TokenBalance, 0, count)
	for i := start; i < start+count; i++ {
		records = append(records, &TokenBalance{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: fmt.Sprintf("0xcontract_%d", i),
			AccountAddress:  fmt.Sprintf("0xaccount_%d", i%5),
			TokenID:         uint32(i * 10),
			Balance:         uint64(1000 + i),
		})
	}
	require.NoError(t, table.Insert(ctx, records))
	return records
}

func insertTokens(t *testing.T, ctx context.Context, table bond.Table[*Token], start, count int) []*Token {
	t.Helper()
	records := make([]*Token, 0, count)
	for i := start; i < start+count; i++ {
		records = append(records, &Token{
			ID:   uint64(i),
			Name: fmt.Sprintf("token_%d", i),
		})
	}
	require.NoError(t, table.Insert(ctx, records))
	return records
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestBackupRestore_TableData(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Source DB: create table, insert 10 records.
	srcDB := openTestDB(t, filepath.Join(dir, "src"))
	table := setupTokenBalanceTable(srcDB)
	insertTokenBalances(t, ctx, table, 0, 10)

	// Backup (complete) to in-memory bucket.
	bucket := objstore.NewInMemBucket()
	_, err := backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, srcDB.Close())

	// Restore to a new directory.
	restoreDir := filepath.Join(dir, "restored")
	require.NoError(t, backup.Restore(ctx, bucket, backup.RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	}))

	// Open restored DB and recreate table definition.
	restoredDB := openTestDB(t, restoreDir)
	defer restoredDB.Close()
	restoredTable := setupTokenBalanceTable(restoredDB)

	// Verify via Scan — 10 records.
	var results []*TokenBalance
	require.NoError(t, restoredTable.Scan(ctx, &results, false))
	assert.Len(t, results, 10)

	// Verify via GetPoint — specific record.
	rec, err := restoredTable.GetPoint(ctx, &TokenBalance{ID: 5})
	require.NoError(t, err)
	assert.Equal(t, uint64(5), rec.ID)
	assert.Equal(t, "0xcontract_5", rec.ContractAddress)
	assert.Equal(t, "0xaccount_0", rec.AccountAddress)
	assert.Equal(t, uint64(1005), rec.Balance)

	// Verify via Query with Limit.
	var queryResults []*TokenBalance
	require.NoError(t, restoredTable.Query().Limit(5).Execute(ctx, &queryResults))
	assert.Len(t, queryResults, 5)
}

func TestBackupRestore_SecondaryIndexes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Source DB: table + secondary index, insert 20 records.
	srcDB := openTestDB(t, filepath.Join(dir, "src"))
	table := setupTokenBalanceTable(srcDB)
	idx := newAccountAddressIndex()
	require.NoError(t, table.AddIndex([]*bond.Index[*TokenBalance]{idx}))
	insertTokenBalances(t, ctx, table, 0, 20) // i%5 → 4 records per address

	// Backup.
	bucket := objstore.NewInMemBucket()
	_, err := backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, srcDB.Close())

	// Restore.
	restoreDir := filepath.Join(dir, "restored")
	require.NoError(t, backup.Restore(ctx, bucket, backup.RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	}))

	// Open restored DB, recreate table + same secondary index.
	restoredDB := openTestDB(t, restoreDir)
	defer restoredDB.Close()
	restoredTable := setupTokenBalanceTable(restoredDB)
	restoredIdx := newAccountAddressIndex()
	require.NoError(t, restoredTable.AddIndex([]*bond.Index[*TokenBalance]{restoredIdx}))

	// Query via index — expect 4 records for "0xaccount_0".
	var queryResults []*TokenBalance
	require.NoError(t, restoredTable.Query().
		With(restoredIdx, bond.NewSelectorPoint(&TokenBalance{AccountAddress: "0xaccount_0"})).
		Execute(ctx, &queryResults))
	assert.Len(t, queryResults, 4)
	for _, r := range queryResults {
		assert.Equal(t, "0xaccount_0", r.AccountAddress)
	}

	// ScanIndex — expect 4 records for "0xaccount_1".
	var scanResults []*TokenBalance
	require.NoError(t, restoredTable.ScanIndex(ctx, restoredIdx,
		bond.NewSelectorPoint(&TokenBalance{AccountAddress: "0xaccount_1"}),
		&scanResults, false))
	assert.Len(t, scanResults, 4)
	for _, r := range scanResults {
		assert.Equal(t, "0xaccount_1", r.AccountAddress)
	}
}

func TestBackupRestore_MultipleTables(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Source DB: two tables.
	srcDB := openTestDB(t, filepath.Join(dir, "src"))
	tbTable := setupTokenBalanceTable(srcDB)
	tkTable := setupTokenTable(srcDB)

	insertTokenBalances(t, ctx, tbTable, 0, 10)
	insertTokens(t, ctx, tkTable, 0, 10)

	// Backup.
	bucket := objstore.NewInMemBucket()
	_, err := backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, srcDB.Close())

	// Restore.
	restoreDir := filepath.Join(dir, "restored")
	require.NoError(t, backup.Restore(ctx, bucket, backup.RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	}))

	// Open restored DB, recreate both tables.
	restoredDB := openTestDB(t, restoreDir)
	defer restoredDB.Close()
	restoredTBTable := setupTokenBalanceTable(restoredDB)
	restoredTKTable := setupTokenTable(restoredDB)

	// Scan TokenBalance → 10 records.
	var tbResults []*TokenBalance
	require.NoError(t, restoredTBTable.Scan(ctx, &tbResults, false))
	assert.Len(t, tbResults, 10)

	tb, err := restoredTBTable.GetPoint(ctx, &TokenBalance{ID: 3})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), tb.ID)
	assert.Equal(t, "0xcontract_3", tb.ContractAddress)

	// Scan Token → 10 records.
	var tkResults []*Token
	require.NoError(t, restoredTKTable.Scan(ctx, &tkResults, false))
	assert.Len(t, tkResults, 10)

	tk, err := restoredTKTable.GetPoint(ctx, &Token{ID: 3})
	require.NoError(t, err)
	assert.Equal(t, "token_3", tk.Name)
}

func TestBackupRestore_Incremental(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Source DB: table + index.
	srcDB := openTestDB(t, filepath.Join(dir, "src"))
	table := setupTokenBalanceTable(srcDB)
	idx := newAccountAddressIndex()
	require.NoError(t, table.AddIndex([]*bond.Index[*TokenBalance]{idx}))

	// Phase 1: insert 0..9, complete backup.
	insertTokenBalances(t, ctx, table, 0, 10)
	bucket := objstore.NewInMemBucket()
	_, err := backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 2: insert 10..19, incremental backup.
	insertTokenBalances(t, ctx, table, 10, 10)
	_, err = backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, srcDB.Close())

	// Restore (applies complete + incremental).
	restoreDir := filepath.Join(dir, "restored")
	require.NoError(t, backup.Restore(ctx, bucket, backup.RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	}))

	// Open restored DB, recreate table + index.
	restoredDB := openTestDB(t, restoreDir)
	defer restoredDB.Close()
	restoredTable := setupTokenBalanceTable(restoredDB)
	restoredIdx := newAccountAddressIndex()
	require.NoError(t, restoredTable.AddIndex([]*bond.Index[*TokenBalance]{restoredIdx}))

	// Scan → 20 records.
	var results []*TokenBalance
	require.NoError(t, restoredTable.Scan(ctx, &results, false))
	assert.Len(t, results, 20)

	// Phase 1 data correct.
	rec, err := restoredTable.GetPoint(ctx, &TokenBalance{ID: 5})
	require.NoError(t, err)
	assert.Equal(t, uint64(5), rec.ID)
	assert.Equal(t, "0xcontract_5", rec.ContractAddress)

	// Phase 2 data correct.
	rec, err = restoredTable.GetPoint(ctx, &TokenBalance{ID: 15})
	require.NoError(t, err)
	assert.Equal(t, uint64(15), rec.ID)
	assert.Equal(t, "0xcontract_15", rec.ContractAddress)

	// Index query spanning both phases: "0xaccount_0" has IDs 0,5,10,15 → 4 records.
	var idxResults []*TokenBalance
	require.NoError(t, restoredTable.Query().
		With(restoredIdx, bond.NewSelectorPoint(&TokenBalance{AccountAddress: "0xaccount_0"})).
		Execute(ctx, &idxResults))
	assert.Len(t, idxResults, 4)
}

func TestBackupRestore_PointInTime(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	srcDB := openTestDB(t, filepath.Join(dir, "src"))
	table := setupTokenBalanceTable(srcDB)

	bucket := objstore.NewInMemBucket()

	// Phase 1: insert 0..4, complete backup at T=12:00.
	insertTokenBalances(t, ctx, table, 0, 5)
	_, err := backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 2: insert 5..9, incremental at T=13:00.
	insertTokenBalances(t, ctx, table, 5, 5)
	_, err = backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 13, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Phase 3: insert 10..14, incremental at T=14:00.
	insertTokenBalances(t, ctx, table, 10, 5)
	_, err = backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeIncremental,
		At:     time.Date(2025, 2, 12, 14, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, srcDB.Close())

	// Restore with Before=T=12:30 → only the complete backup.
	restoreDir := filepath.Join(dir, "restored")
	require.NoError(t, backup.Restore(ctx, bucket, backup.RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
		Before:     time.Date(2025, 2, 12, 12, 30, 0, 0, time.UTC),
	}))

	restoredDB := openTestDB(t, restoreDir)
	defer restoredDB.Close()
	restoredTable := setupTokenBalanceTable(restoredDB)

	// Scan → 5 records (IDs 0..4 only).
	var results []*TokenBalance
	require.NoError(t, restoredTable.Scan(ctx, &results, false))
	assert.Len(t, results, 5)

	// Record from phase 2 should not exist.
	_, err = restoredTable.GetPoint(ctx, &TokenBalance{ID: 7})
	assert.ErrorIs(t, err, bond.ErrNotFound)
}

func TestBackupRestore_BatchInsert(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	srcDB := openTestDB(t, filepath.Join(dir, "src"))
	table := setupTokenBalanceTable(srcDB)
	idx := newAccountAddressIndex()
	require.NoError(t, table.AddIndex([]*bond.Index[*TokenBalance]{idx}))

	// Insert via batch.
	records := make([]*TokenBalance, 0, 10)
	for i := 0; i < 10; i++ {
		records = append(records, &TokenBalance{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: fmt.Sprintf("0xcontract_%d", i),
			AccountAddress:  fmt.Sprintf("0xaccount_%d", i%5),
			TokenID:         uint32(i * 10),
			Balance:         uint64(1000 + i),
		})
	}

	batch := srcDB.Batch(bond.BatchTypeWriteOnly)
	require.NoError(t, table.Insert(ctx, records, batch))
	require.NoError(t, batch.Commit(bond.Sync))
	require.NoError(t, batch.Close())

	// Backup.
	bucket := objstore.NewInMemBucket()
	_, err := backup.Backup(ctx, srcDB, bucket, backup.BackupOptions{
		Prefix: "backups",
		Type:   backup.BackupTypeComplete,
		At:     time.Date(2025, 2, 12, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, srcDB.Close())

	// Restore.
	restoreDir := filepath.Join(dir, "restored")
	require.NoError(t, backup.Restore(ctx, bucket, backup.RestoreOptions{
		Prefix:     "backups",
		RestoreDir: restoreDir,
	}))

	// Open restored DB, recreate table + index.
	restoredDB := openTestDB(t, restoreDir)
	defer restoredDB.Close()
	restoredTable := setupTokenBalanceTable(restoredDB)
	restoredIdx := newAccountAddressIndex()
	require.NoError(t, restoredTable.AddIndex([]*bond.Index[*TokenBalance]{restoredIdx}))

	// Scan → 10 records.
	var scanResults []*TokenBalance
	require.NoError(t, restoredTable.Scan(ctx, &scanResults, false))
	assert.Len(t, scanResults, 10)

	// Index query.
	var idxResults []*TokenBalance
	require.NoError(t, restoredTable.Query().
		With(restoredIdx, bond.NewSelectorPoint(&TokenBalance{AccountAddress: "0xaccount_0"})).
		Execute(ctx, &idxResults))
	assert.Len(t, idxResults, 2) // IDs 0 and 5

	// GetPoint.
	rec, err := restoredTable.GetPoint(ctx, &TokenBalance{ID: 3})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), rec.ID)
	assert.Equal(t, "0xcontract_3", rec.ContractAddress)
	assert.Equal(t, uint64(1003), rec.Balance)
}
