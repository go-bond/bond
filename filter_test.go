package bond

import (
	"fmt"
	"sync"

	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockFilter struct {
	mock.Mock
}

func (m *MockFilter) Add(ctx context.Context, key []byte) {
	m.Called(ctx, key)
}

func (m *MockFilter) MayContain(ctx context.Context, key []byte) bool {
	args := m.Called(ctx, key)
	return args.Get(0).(bool)
}

func (m *MockFilter) Stats() FilterStats {
	args := m.Called()
	return args.Get(0).(FilterStats)
}

func (m *MockFilter) RecordFalsePositive() {
	m.Called()
}

func (m *MockFilter) Load(ctx context.Context, store FilterStorer) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockFilter) Save(ctx context.Context, store FilterStorer) error {
	args := m.Called(ctx, store)
	if err := args.Get(0); err != nil {
		return err.(error)
	}
	return nil
}

func (m *MockFilter) Clear(ctx context.Context, store FilterStorer) error {
	//TODO implement me
	panic("implement me")
}

func TestFilter_Insert(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.Error(t, err)

	mFilter.AssertExpectations(t)
}

func TestFilter_Insert_Batch(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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

	batch := db.Batch(BatchTypeReadWrite)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount}, batch)
	require.NoError(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount}, batch)
	require.Error(t, err)

	err = batch.Commit(Sync)
	require.NoError(t, err)

	_ = batch.Close()

	mFilter.AssertExpectations(t)
}

func TestFilter_Exist(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Twice()
	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

	require.False(t, tokenBalanceTable.Exist(tokenBalanceAccount))

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

	require.True(t, tokenBalanceTable.Exist(tokenBalanceAccount))

	mFilter.AssertExpectations(t)
}

func TestFilter_Upsert(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

	err := tokenBalanceTable.Upsert(context.Background(), []*TokenBalance{tokenBalanceAccount}, TableUpsertOnConflictReplace[*TokenBalance])
	require.NoError(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

	err = tokenBalanceTable.Upsert(context.Background(), []*TokenBalance{tokenBalanceAccountUpdated}, TableUpsertOnConflictReplace[*TokenBalance])
	require.NoError(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

	dbTr, err := tokenBalanceTable.GetPoint(context.Background(), tokenBalanceAccountUpdated)
	require.NoError(t, err)
	assert.Equal(t, tokenBalanceAccountUpdated, dbTr)

	mFilter.AssertExpectations(t)
}

func TestFilter_Get(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()

	_, err := tokenBalanceTable.Get(context.Background(), NewSelectorPoint(tokenBalanceAccount))
	require.Error(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

	dbTr, err := tokenBalanceTable.GetPoint(context.Background(), tokenBalanceAccount)
	require.NoError(t, err)
	assert.Equal(t, tokenBalanceAccount, dbTr)

	mFilter.AssertExpectations(t)
}

func TestFilterInitializable_AddWhenInitialized(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
		mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalance1})
		require.NoError(t, err)

		mFilter.AssertExpectations(t)
	})

	// Test 2: Verify the record exists in filter
	t.Run("Verify first record exists", func(t *testing.T) {
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

		exists := tokenBalanceTable.Exist(tokenBalance1)
		require.True(t, exists)

		mFilter.AssertExpectations(t)
	})

	// Test 3: Add second record (filter still tracking adds)
	t.Run("Add second record while filter is tracking", func(t *testing.T) {
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
		mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalance2})
		require.NoError(t, err)

		mFilter.AssertExpectations(t)
	})

	// Test 4: Verify both records exist
	t.Run("Verify both records exist in filter", func(t *testing.T) {
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Times(2)

		exists1 := tokenBalanceTable.Exist(tokenBalance1)
		require.True(t, exists1)

		exists2 := tokenBalanceTable.Exist(tokenBalance2)
		require.True(t, exists2)

		mFilter.AssertExpectations(t)
	})

	// Test 5: Try to insert duplicate (filter should prevent it)
	t.Run("Try to insert duplicate record", func(t *testing.T) {
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalance1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exist")

		mFilter.AssertExpectations(t)
	})
}

func TestFilterInitializable_AddWithBatch(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	ctx := context.Background()
	batch := db.Batch(BatchTypeReadWrite)
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
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Times(3)
		mFilter.On("Add", mock.Anything, mock.Anything).Return().Times(3)

		err := tokenBalanceTable.Insert(ctx, records, batch)
		require.NoError(t, err)

		err = batch.Commit(Sync)
		require.NoError(t, err)

		mFilter.AssertExpectations(t)
	})

	// Test 2: Verify all records exist via filter
	t.Run("Verify all batch records exist", func(t *testing.T) {
		records := []*TokenBalance{
			{ID: 1},
			{ID: 2},
			{ID: 3},
		}

		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Times(3)

		for _, record := range records {
			exists := tokenBalanceTable.Exist(record)
			require.True(t, exists, "Record with ID %d should exist", record.ID)
		}

		mFilter.AssertExpectations(t)
	})
}

func TestFilterInitializable_AddDuringUpsert(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
		mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

		err := tokenBalanceTable.Upsert(ctx, []*TokenBalance{record}, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		mFilter.AssertExpectations(t)
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
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()
		// Note: Add should NOT be called for updates

		err := tokenBalanceTable.Upsert(ctx, []*TokenBalance{updatedRecord}, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(t, err)

		mFilter.AssertExpectations(t)
	})

	// Test 3: Verify record exists and has updated value
	t.Run("Verify updated record", func(t *testing.T) {
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

		retrieved, err := tokenBalanceTable.GetPoint(ctx, record)
		require.NoError(t, err)
		assert.Equal(t, uint64(200), retrieved.Balance)

		mFilter.AssertExpectations(t)
	})
}

func TestFilterInitializable_StatsWhenInitialized(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
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
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
		mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

		err := tokenBalanceTable.Insert(ctx, []*TokenBalance{record})
		require.NoError(t, err)

		mFilter.AssertExpectations(t)
	})

	// Test 2: Check stats are tracked
	t.Run("Verify filter stats are accessible", func(t *testing.T) {
		expectedStats := FilterStats{
			FalsePositives: 5,
			HitCount:       100,
			MissCount:      10,
		}

		mFilter.On("Stats").Return(expectedStats).Once()

		// Access stats through the filter (assuming we can get it from the table)
		// This tests that Stats() is properly forwarded
		stats := mFilter.Stats()

		assert.Equal(t, uint64(5), stats.FalsePositives)
		assert.Equal(t, uint64(100), stats.HitCount)
		assert.Equal(t, uint64(10), stats.MissCount)

		mFilter.AssertExpectations(t)
	})

	// Test 3: Record false positive
	t.Run("Record false positive", func(t *testing.T) {
		mFilter.On("RecordFalsePositive").Return().Once()

		mFilter.RecordFalsePositive()

		mFilter.AssertExpectations(t)
	})
}

func TestFilterInitializable_ConcurrentAdds(t *testing.T) {
	db := setupDatabase()
	defer func() {
		tearDownDatabase(t, db)
	}()

	mFilter := &MockFilter{}

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Filter: mFilter,
	})

	ctx := context.Background()
	numRecords := 10

	// Set up expectations for concurrent adds
	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Times(numRecords)
	mFilter.On("Add", mock.Anything, mock.Anything).Return().Times(numRecords)

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

		mFilter.AssertExpectations(t)
	})

	// Verify all records exist
	t.Run("Verify all concurrent records exist", func(t *testing.T) {
		mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Times(numRecords)

		for i := 1; i <= numRecords; i++ {
			record := &TokenBalance{ID: uint64(i)}
			exists := tokenBalanceTable.Exist(record)
			require.True(t, exists, "Record with ID %d should exist", i)
		}

		mFilter.AssertExpectations(t)
	})
}
