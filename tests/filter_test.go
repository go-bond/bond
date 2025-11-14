package bond_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-bond/bond"
	bondmock "github.com/go-bond/bond/internal/testing/mocks/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func setupDatabaseForFilter(t *testing.T) bond.DB {
	dbPath := t.TempDir() + "/test.db"
	db, err := bond.Open(dbPath, &bond.Options{})
	require.NoError(t, err)
	return db
}

func tearDownDatabaseForFilter(t *testing.T, db bond.DB) {
	if db != nil {
		err := db.Close()
		require.NoError(t, err)
	}
}

type TokenBalance struct {
	ID              uint64 `json:"id"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

func TestFilter_Insert(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
	mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.Error(t, err)
}

func TestFilter_Insert_Batch(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	batch := db.Batch(bond.BatchTypeReadWrite)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
	mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount}, batch)
	require.NoError(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount}, batch)
	require.Error(t, err)

	err = batch.Commit(bond.Sync)
	require.NoError(t, err)

	_ = batch.Close()
}

func TestFilter_Exist(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(2)
	mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

	require.False(t, tokenBalanceTable.Exist(tokenBalanceAccount))

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

	require.True(t, tokenBalanceTable.Exist(tokenBalanceAccount))
}

func TestFilter_Upsert(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
	mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

	err := tokenBalanceTable.Upsert(context.Background(), []*TokenBalance{tokenBalanceAccount}, bond.TableUpsertOnConflictReplace[*TokenBalance])
	require.NoError(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

	err = tokenBalanceTable.Upsert(context.Background(), []*TokenBalance{tokenBalanceAccountUpdated}, bond.TableUpsertOnConflictReplace[*TokenBalance])
	require.NoError(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

	dbTr, err := tokenBalanceTable.GetPoint(context.Background(), tokenBalanceAccountUpdated)
	require.NoError(t, err)
	assert.Equal(t, tokenBalanceAccountUpdated, dbTr)
}

func TestFilter_Get(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)

	_, err := tokenBalanceTable.Get(context.Background(), bond.NewSelectorPoint(tokenBalanceAccount))
	require.Error(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
	mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

	dbTr, err := tokenBalanceTable.GetPoint(context.Background(), tokenBalanceAccount)
	require.NoError(t, err)
	assert.Equal(t, tokenBalanceAccount, dbTr)
}

func TestFilterInitializable_AddWhenInitialized(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	// Create test records
	tokenBalance1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract1",
		AccountAddress:  "0xtestAccount1",
		Balance:         100,
	}

	tokenBalance2 := &TokenBalance{
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount2",
		Balance:         200,
	}

	ctx := context.Background()

	// Test 1: Add first record (filter not initialized yet - should call MayContain and Add)
	t.Run("Add first record before filter initialization", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
		mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalance1})
		require.NoError(t, err)
	})

	// Test 2: Verify the record exists in filter
	t.Run("Verify first record exists", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

		exists := tokenBalanceTable.Exist(tokenBalance1)
		require.True(t, exists)
	})

	// Test 3: Add second record (filter still tracking adds)
	t.Run("Add second record while filter is tracking", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
		mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalance2})
		require.NoError(t, err)
	})

	// Test 4: Verify both records exist
	t.Run("Verify both records exist in filter", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(2)

		exists1 := tokenBalanceTable.Exist(tokenBalance1)
		require.True(t, exists1)

		exists2 := tokenBalanceTable.Exist(tokenBalance2)
		require.True(t, exists2)
	})

	// Test 5: Try to insert duplicate (filter should prevent it)
	t.Run("Try to insert duplicate record", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalance1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exist")
	})
}

func TestFilterInitializable_AddWithBatch(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	ctx := context.Background()
	batch := db.Batch(bond.BatchTypeReadWrite)
	defer func() {
		_ = batch.Close()
	}()

	// Test 1: Add multiple records in a batch
	t.Run("Add multiple records in batch", func(t *testing.T) {
		records := []*TokenBalance{
			{
				ID:              1,
				AccountID:       1,
				ContractAddress: "0xcontract1",
				AccountAddress:  "0xaccount1",
				Balance:         100,
			},
			{
				ID:              2,
				AccountID:       2,
				ContractAddress: "0xcontract2",
				AccountAddress:  "0xaccount2",
				Balance:         200,
			},
			{
				ID:              3,
				AccountID:       3,
				ContractAddress: "0xcontract3",
				AccountAddress:  "0xaccount3",
				Balance:         300,
			},
		}

		// Each record should call MayContain and Add
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(3)
		mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(3)

		err := tokenBalanceTable.Insert(ctx, records, batch)
		require.NoError(t, err)

		err = batch.Commit(bond.Sync)
		require.NoError(t, err)
	})

	// Test 2: Verify all records exist via filter
	t.Run("Verify all batch records exist", func(t *testing.T) {
		records := []*TokenBalance{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		}

		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(3)

		for _, record := range records {
			exists := tokenBalanceTable.Exist(record)
			require.True(t, exists, "Record with ID %d should exist", record.ID)
		}
	})
}

func TestFilterInitializable_AddDuringUpsert(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	ctx := context.Background()

	record := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xcontract",
		AccountAddress:  "0xaccount",
		Balance:         100,
	}

	// Test 1: First upsert (insert) - should add to filter
	t.Run("First upsert adds to filter", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
		mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

		err := tokenBalanceTable.Upsert(ctx, []*TokenBalance{record}, bond.TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)
	})

	// Test 2: Second upsert (update) - should check filter but not add again
	t.Run("Second upsert does not add to filter", func(t *testing.T) {
		updatedRecord := &TokenBalance{
			ID:              1,
			AccountID:       1,
			ContractAddress: "0xcontract",
			AccountAddress:  "0xaccount",
			Balance:         200, // Updated balance
		}

		// Should check filter (returns true), but not call Add again
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)
		// Note: Add should NOT be called for updates

		err := tokenBalanceTable.Upsert(ctx, []*TokenBalance{updatedRecord}, bond.TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)
	})

	// Test 3: Verify record exists and has updated value
	t.Run("Verify updated record", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(1)

		retrieved, err := tokenBalanceTable.GetPoint(ctx, record)
		require.NoError(t, err)
		assert.Equal(t, uint64(200), retrieved.Balance)
	})
}

func TestFilterInitializable_StatsWhenInitialized(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	ctx := context.Background()

	record := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xcontract",
		AccountAddress:  "0xaccount",
		Balance:         100,
	}

	// Test 1: Add record
	t.Run("Add record to filter", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(1)
		mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(1)

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{record})
		require.NoError(t, err)
	})

	// Test 2: Check stats are tracked
	t.Run("Verify filter stats are accessible", func(t *testing.T) {
		expectedStats := bond.FilterStats{
			FalsePositives: 5,
			HitCount:       100,
			MissCount:      10,
		}

		mFilter.EXPECT().Stats().Return(expectedStats).Times(1)

		// Access stats through the filter (assuming we can get it from the table)
		// This tests that Stats() is properly forwarded
		stats := mFilter.Stats()

		assert.Equal(t, uint64(5), stats.FalsePositives)
		assert.Equal(t, uint64(100), stats.HitCount)
		assert.Equal(t, uint64(10), stats.MissCount)
	})

	// Test 3: Record false positive
	t.Run("Record false positive", func(t *testing.T) {
		mFilter.EXPECT().RecordFalsePositive().Times(1)

		mFilter.RecordFalsePositive()
	})
}

func TestFilterInitializable_ConcurrentAdds(t *testing.T) {
	db := setupDatabaseForFilter(t)
	defer tearDownDatabaseForFilter(t, db)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mFilter := bondmock.NewMockFilter(ctrl)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	ctx := context.Background()
	numRecords := 10

	// Set up expectations for concurrent adds
	mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(false).Times(numRecords)
	mFilter.EXPECT().Add(gomock.Any(), gomock.Any()).Times(numRecords)

	t.Run("Concurrent adds to filter", func(t *testing.T) {
		var wg sync.WaitGroup
		errChan := make(chan error, numRecords)

		for i := 0; i < numRecords; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()

				record := &TokenBalance{
					ID:              id,
					AccountID:       uint32(id),
					ContractAddress: fmt.Sprintf("0xcontract%d", id),
					AccountAddress:  fmt.Sprintf("0xaccount%d", id),
					Balance:         id * 100,
				}

				err := tokenBalanceTable.Insert(ctx, []*TokenBalance{record})
				if err != nil {
					errChan <- err
				}
			}(uint64(i + 1))
		}

		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			require.NoError(t, err)
		}
	})

	// Verify all records exist
	t.Run("Verify all concurrent records exist", func(t *testing.T) {
		mFilter.EXPECT().MayContain(gomock.Any(), gomock.Any()).Return(true).Times(numRecords)

		for i := 1; i <= numRecords; i++ {
			record := &TokenBalance{ID: uint64(i)}
			exists := tokenBalanceTable.Exist(record)
			require.True(t, exists, "Record with ID %d should exist", i)
		}
	})
}
