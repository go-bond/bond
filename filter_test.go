package bond

// import (
// 	"context"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// 	"github.com/stretchr/testify/require"
// )

// type MockFilter struct {
// 	mock.Mock
// }

// func (m *MockFilter) Add(ctx context.Context, key []byte) {
// 	m.Called(ctx, key)
// }

// func (m *MockFilter) MayContain(ctx context.Context, key []byte) bool {
// 	args := m.Called(ctx, key)
// 	return args.Get(0).(bool)
// }

// func (m *MockFilter) Load(ctx context.Context, store FilterStorer) error {
// 	//TODO implement me
// 	panic("implement me")
// }

// func (m *MockFilter) Save(ctx context.Context, store FilterStorer) error {
// 	args := m.Called(ctx, store)
// 	if err := args.Get(0); err != nil {
// 		return err.(error)
// 	}
// 	return nil
// }

// func (m *MockFilter) Clear(ctx context.Context, store FilterStorer) error {
// 	//TODO implement me
// 	panic("implement me")
// }

// func TestFilter_Insert(t *testing.T) {
// 	db := setupDatabase()
// 	defer func() {
// 		tearDownDatabase(db)
// 	}()

// 	mFilter := &MockFilter{}

// 	const (
// 		TokenBalanceTableID = TableID(1)
// 	)

// 	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
// 		DB:        db,
// 		TableID:   TokenBalanceTableID,
// 		TableName: "token_balance",
// 		TablePrimaryKeyFunc: func(builder *KeyBuilder, tb *TokenBalance) []byte {
// 			return builder.AddUint64Field(tb.ID).Bytes()
// 		},
// 		Filter: mFilter,
// 	})

// 	tokenBalanceAccount := &TokenBalance{
// 		ID:              1,
// 		AccountID:       1,
// 		ContractAddress: "0xtestContract",
// 		AccountAddress:  "0xtestAccount",
// 		Balance:         5,
// 	}

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
// 	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

// 	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
// 	require.NoError(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

// 	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
// 	require.Error(t, err)

// 	mFilter.AssertExpectations(t)
// }

// func TestFilter_Insert_Batch(t *testing.T) {
// 	db := setupDatabase()
// 	defer func() {
// 		tearDownDatabase(db)
// 	}()

// 	mFilter := &MockFilter{}

// 	const (
// 		TokenBalanceTableID = TableID(1)
// 	)

// 	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
// 		DB:        db,
// 		TableID:   TokenBalanceTableID,
// 		TableName: "token_balance",
// 		TablePrimaryKeyFunc: func(builder *KeyBuilder, tb *TokenBalance) []byte {
// 			return builder.AddUint64Field(tb.ID).Bytes()
// 		},
// 		Filter: mFilter,
// 	})

// 	tokenBalanceAccount := &TokenBalance{
// 		ID:              1,
// 		AccountID:       1,
// 		ContractAddress: "0xtestContract",
// 		AccountAddress:  "0xtestAccount",
// 		Balance:         5,
// 	}

// 	batch := db.Batch()

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
// 	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

// 	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount}, batch)
// 	require.NoError(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

// 	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount}, batch)
// 	require.Error(t, err)

// 	err = batch.Commit(Sync)
// 	require.NoError(t, err)

// 	_ = batch.Close()

// 	mFilter.AssertExpectations(t)
// }

// func TestFilter_Exist(t *testing.T) {
// 	db := setupDatabase()
// 	defer func() {
// 		tearDownDatabase(db)
// 	}()

// 	mFilter := &MockFilter{}

// 	const (
// 		TokenBalanceTableID = TableID(1)
// 	)

// 	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
// 		DB:        db,
// 		TableID:   TokenBalanceTableID,
// 		TableName: "token_balance",
// 		TablePrimaryKeyFunc: func(builder *KeyBuilder, tb *TokenBalance) []byte {
// 			return builder.AddUint64Field(tb.ID).Bytes()
// 		},
// 		Filter: mFilter,
// 	})

// 	tokenBalanceAccount := &TokenBalance{
// 		ID:              1,
// 		AccountID:       1,
// 		ContractAddress: "0xtestContract",
// 		AccountAddress:  "0xtestAccount",
// 		Balance:         5,
// 	}

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Twice()
// 	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

// 	require.False(t, tokenBalanceTable.Exist(tokenBalanceAccount))

// 	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
// 	require.NoError(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

// 	require.True(t, tokenBalanceTable.Exist(tokenBalanceAccount))

// 	mFilter.AssertExpectations(t)
// }

// func TestFilter_Upsert(t *testing.T) {
// 	db := setupDatabase()
// 	defer func() {
// 		tearDownDatabase(db)
// 	}()

// 	mFilter := &MockFilter{}

// 	const (
// 		TokenBalanceTableID = TableID(1)
// 	)

// 	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
// 		DB:        db,
// 		TableID:   TokenBalanceTableID,
// 		TableName: "token_balance",
// 		TablePrimaryKeyFunc: func(builder *KeyBuilder, tb *TokenBalance) []byte {
// 			return builder.AddUint64Field(tb.ID).Bytes()
// 		},
// 		Filter: mFilter,
// 	})

// 	tokenBalanceAccount := &TokenBalance{
// 		ID:              1,
// 		AccountID:       1,
// 		ContractAddress: "0xtestContract",
// 		AccountAddress:  "0xtestAccount",
// 		Balance:         5,
// 	}

// 	tokenBalanceAccountUpdated := &TokenBalance{
// 		ID:              1,
// 		AccountID:       1,
// 		ContractAddress: "0xtestContract",
// 		AccountAddress:  "0xtestAccount",
// 		Balance:         7,
// 	}

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
// 	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

// 	err := tokenBalanceTable.Upsert(context.Background(), []*TokenBalance{tokenBalanceAccount}, TableUpsertOnConflictReplace[*TokenBalance])
// 	require.NoError(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

// 	err = tokenBalanceTable.Upsert(context.Background(), []*TokenBalance{tokenBalanceAccountUpdated}, TableUpsertOnConflictReplace[*TokenBalance])
// 	require.NoError(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

// 	dbTr, err := tokenBalanceTable.Get(tokenBalanceAccountUpdated)
// 	require.NoError(t, err)
// 	assert.Equal(t, tokenBalanceAccountUpdated, dbTr)

// 	mFilter.AssertExpectations(t)
// }

// func TestFilter_Get(t *testing.T) {
// 	db := setupDatabase()
// 	defer func() {
// 		tearDownDatabase(db)
// 	}()

// 	mFilter := &MockFilter{}

// 	const (
// 		TokenBalanceTableID = TableID(1)
// 	)

// 	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
// 		DB:        db,
// 		TableID:   TokenBalanceTableID,
// 		TableName: "token_balance",
// 		TablePrimaryKeyFunc: func(builder *KeyBuilder, tb *TokenBalance) []byte {
// 			return builder.AddUint64Field(tb.ID).Bytes()
// 		},
// 		Filter: mFilter,
// 	})

// 	tokenBalanceAccount := &TokenBalance{
// 		ID:              1,
// 		AccountID:       1,
// 		ContractAddress: "0xtestContract",
// 		AccountAddress:  "0xtestAccount",
// 		Balance:         5,
// 	}

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()

// 	_, err := tokenBalanceTable.Get(tokenBalanceAccount)
// 	require.Error(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(false).Once()
// 	mFilter.On("Add", mock.Anything, mock.Anything).Return().Once()

// 	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
// 	require.NoError(t, err)

// 	mFilter.On("MayContain", mock.Anything, mock.Anything).Return(true).Once()

// 	dbTr, err := tokenBalanceTable.Get(tokenBalanceAccount)
// 	require.NoError(t, err)
// 	assert.Equal(t, tokenBalanceAccount, dbTr)

// 	mFilter.AssertExpectations(t)
// }
