package bond

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/serializers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_NewTable(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)
	assert.Equal(t, TokenBalanceTableID, tokenBalanceTable.ID())
}

func TestBondTable_Interfaces(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	tableReadInterface := TableReader[*TokenBalance](tokenBalanceTable)
	require.NotNil(t, tableReadInterface)

	tableWriteInterface := TableWriter[*TokenBalance](tokenBalanceTable)
	require.NotNil(t, tableWriteInterface)

	tableReadWriteInterface := Table[*TokenBalance](tokenBalanceTable)
	require.NotNil(t, tableReadWriteInterface)
}

func TestBondTable_PrimaryIndex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	primaryIndex := tokenBalanceTable.PrimaryIndex()
	require.NotNil(t, primaryIndex)
	assert.Equal(t, PrimaryIndexID, primaryIndex.IndexID)
	assert.Equal(t, PrimaryIndexName, primaryIndex.IndexName)
}

func TestBondTable_SecondaryIndexes(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = IndexID(iota)
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex(IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	_ = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	secondaryIndexes := tokenBalanceTable.SecondaryIndexes()
	require.NotNil(t, secondaryIndexes)
	require.Equal(t, 1, len(secondaryIndexes))

	assert.Equal(t, TokenBalanceAccountAddressIndexID, secondaryIndexes[0].IndexID)
	assert.Equal(t, "account_address_idx", secondaryIndexes[0].IndexName)
}

func TestDuplicateIndexID(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	var (
		TokenBalanceAccountAddressIndex = NewIndex(IndexOptions[*TokenBalance]{
			IndexID:   PrimaryIndexID + 1,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAddressIndex2 = NewIndex(IndexOptions[*TokenBalance]{
			IndexID:   PrimaryIndexID + 1,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAddressIndex2,
	})
	require.Error(t, err)
}

func TestBondTable_Serializer(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)
	assert.NotNil(t, tokenBalanceTable.Serializer())
}

func TestBondTable_SerializerOption(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	require.NotNil(t, tokenBalanceTable)
	assert.NotNil(t, tokenBalanceTable.Serializer())
}

func TestBondTable_Get(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	require.NotNil(t, tokenBalanceTable)

	// token balances to insert
	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	// get token balance
	tokenBalance, err := tokenBalanceTable.GetPoint(context.Background(), &TokenBalance{ID: tokenBalanceAccount1.ID})
	require.NoError(t, err)
	assert.Equal(t, tokenBalanceAccount1, tokenBalance)

	// get token balance with non-existing id
	tokenBalance, err = tokenBalanceTable.GetPoint(context.Background(), &TokenBalance{ID: 2})
	require.Error(t, err)
	assert.Nil(t, tokenBalance)
}

func TestBondTable_Get_Range(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	require.NotNil(t, tokenBalanceTable)

	// token balances to insert
	expectedTokenBalances := []*TokenBalance{
		{
			ID:              1,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
		{
			ID:              2,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
	}

	err := tokenBalanceTable.Insert(context.Background(), expectedTokenBalances)
	require.NoError(t, err)

	// get token balances with range
	tokenBalances, err := tokenBalanceTable.Get(context.Background(), NewSelectorRange(&TokenBalance{ID: 1}, &TokenBalance{ID: 2}))
	require.NoError(t, err)
	require.Equal(t, len(expectedTokenBalances), len(tokenBalances))
	assert.Equal(t, expectedTokenBalances, tokenBalances)

	// get token balance with non-existing id range
	tokenBalances, err = tokenBalanceTable.Get(context.Background(), NewSelectorRange(&TokenBalance{ID: 3}, &TokenBalance{ID: 4}))
	require.NoError(t, err)
	assert.Equal(t, 0, len(tokenBalances))
}

func TestBondTable_Get_Points(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	require.NotNil(t, tokenBalanceTable)

	// token balances to insert
	insertTokenBalances := []*TokenBalance{
		{
			ID:              1,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
		{
			ID:              3,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
	}

	err := tokenBalanceTable.Insert(context.Background(), insertTokenBalances)
	require.NoError(t, err)

	// get token balances with points
	tokenBalances, err := tokenBalanceTable.Get(context.Background(), NewSelectorPoints(&TokenBalance{ID: 1}, &TokenBalance{ID: 3}))
	require.NoError(t, err)
	require.Equal(t, len(insertTokenBalances), len(tokenBalances))
	require.Equal(t, insertTokenBalances, tokenBalances)

	// get token balance with non-existing points
	tokenBalances, err = tokenBalanceTable.Get(context.Background(), NewSelectorPoints(&TokenBalance{ID: 2}))
	require.NoError(t, err)
	require.Equal(t, 1, len(tokenBalances))
	require.Nil(t, tokenBalances[0])
}

func TestBondTable_Get_Ranges(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	require.NotNil(t, tokenBalanceTable)

	// token balances to insert
	insertTokenBalances := []*TokenBalance{
		{
			ID:              1,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
		{
			ID:              2,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
		{
			ID:              3,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         7,
		},
	}

	err := tokenBalanceTable.Insert(context.Background(), insertTokenBalances)
	require.NoError(t, err)

	expectedTokenBalances := []*TokenBalance{
		insertTokenBalances[0],
		insertTokenBalances[2],
	}

	// get token balances with range
	tokenBalances, err := tokenBalanceTable.Get(context.Background(), NewSelectorRanges([]*TokenBalance{{ID: 0}, {ID: 1}}, []*TokenBalance{{ID: 3}, {ID: math.MaxUint64}}))
	require.NoError(t, err)
	require.Equal(t, len(expectedTokenBalances), len(tokenBalances))
	assert.Equal(t, expectedTokenBalances, tokenBalances)

	// get token balance with non-existing id range
	tokenBalances, err = tokenBalanceTable.Get(context.Background(), NewSelectorRanges([]*TokenBalance{{ID: 5}, {ID: 5}}))
	require.NoError(t, err)
	assert.Equal(t, 0, len(tokenBalances))
}

func TestBondTable_Insert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount1, &tokenBalanceAccount1FromDB)
	}
}

func TestBondTable_Insert_Context_Canceled(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tokenBalanceTable.Insert(ctx, []*TokenBalance{tokenBalanceAccount1})
	require.Error(t, err)
}

func TestBondTable_Insert_When_Exist(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	err = tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.Error(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount1, &tokenBalanceAccount1FromDB)
	}
}

func TestBondTable_Update(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
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

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	err = tokenBalanceTable.Update(context.Background(), []*TokenBalance{tokenBalanceAccountUpdated})
	require.NoError(t, err)

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccountUpdated, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()
}

func TestBondTable_Update_Context_Canceled(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
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

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = tokenBalanceTable.Update(ctx, []*TokenBalance{tokenBalanceAccountUpdated})
	require.Error(t, err)

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()
}

func TestBondTable_Upsert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccount2 := &TokenBalance{
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         15,
	}

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	retTrsUpserted, err := tokenBalanceTable.Upsert(
		context.Background(),
		[]*TokenBalance{tokenBalanceAccountUpdated, tokenBalanceAccount2},
		TableUpsertOnConflictReplace[*TokenBalance])
	require.NoError(t, err)

	require.Equal(t, 2, len(retTrsUpserted))
	assert.Equal(t, tokenBalanceAccountUpdated, retTrsUpserted[0])
	assert.Equal(t, tokenBalanceAccount2, retTrsUpserted[1])

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccountFromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccountFromDB)
		if err != nil {
			// panic(err)
			break
		}
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}
	require.NoError(t, err)

	_ = it.Close()

	require.Equal(t, 2, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccountUpdated, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount2, tokenBalances[1])
}

func TestBondTable_Upsert_Context_Canceled(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccount2 := &TokenBalance{
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         15,
	}

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	retTrsUpserted, err := tokenBalanceTable.Upsert(
		ctx,
		[]*TokenBalance{tokenBalanceAccountUpdated, tokenBalanceAccount2},
		TableUpsertOnConflictReplace[*TokenBalance])
	require.Error(t, err)

	require.Equal(t, 0, len(retTrsUpserted))

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccountFromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccountFromDB)
		if err != nil {
			break
		}
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}
	require.NoError(t, err)

	_ = it.Close()

	require.Equal(t, 1, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccount, tokenBalances[0])
}

func TestBondTable_Upsert_OnConflict(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccount2 := &TokenBalance{
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         15,
	}

	tokenBalanceAccountUpdate := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	expectedTokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         12,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	onConflictAddBalance := func(oldTb, newTb *TokenBalance) *TokenBalance {
		return &TokenBalance{
			ID:              oldTb.ID,
			AccountID:       oldTb.AccountID,
			ContractAddress: oldTb.ContractAddress,
			AccountAddress:  oldTb.AccountAddress,
			TokenID:         oldTb.TokenID,
			Balance:         oldTb.Balance + newTb.Balance,
		}
	}

	retTrsUpserted, err := tokenBalanceTable.Upsert(
		context.Background(),
		[]*TokenBalance{tokenBalanceAccountUpdate, tokenBalanceAccount2}, onConflictAddBalance)
	require.NoError(t, err)

	require.Equal(t, 2, len(retTrsUpserted))
	assert.Equal(t, expectedTokenBalanceAccountUpdated, retTrsUpserted[0])
	assert.Equal(t, tokenBalanceAccount2, retTrsUpserted[1])

	it, err = db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccountFromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccountFromDB)
		if err != nil {
			break
		}
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}
	require.NoError(t, err)

	_ = it.Close()

	require.Equal(t, 2, len(tokenBalances))
	assert.Equal(t, expectedTokenBalanceAccountUpdated, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount2, tokenBalances[1])
}

func TestBondTable_Upsert_OnConflict_Two_Updates_Same_Row(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalanceAccount2 := &TokenBalance{
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         15,
	}

	tokenBalanceAccountUpdate := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccount1FromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccount1FromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalanceAccount, &tokenBalanceAccount1FromDB)
	}

	_ = it.Close()

	onConflictAddBalance := func(oldTb, newTb *TokenBalance) *TokenBalance {
		return &TokenBalance{
			ID:              oldTb.ID,
			AccountID:       oldTb.AccountID,
			ContractAddress: oldTb.ContractAddress,
			AccountAddress:  oldTb.AccountAddress,
			TokenID:         oldTb.TokenID,
			Balance:         oldTb.Balance + newTb.Balance,
		}
	}

	retTrsUpserted, err := tokenBalanceTable.Upsert(
		context.Background(),
		[]*TokenBalance{tokenBalanceAccountUpdate, tokenBalanceAccountUpdate, tokenBalanceAccount2}, onConflictAddBalance)
	require.ErrorContains(t, err, "duplicate record found")
	require.Equal(t, 0, len(retTrsUpserted))
}

func TestBondTable_Update_No_Such_Entry(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccountUpdated := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	err := tokenBalanceTable.Update(context.Background(), []*TokenBalance{tokenBalanceAccountUpdated})
	require.Error(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)
	defer it.Close()
	assert.False(t, it.First())
}

func TestBondTable_Delete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	err = tokenBalanceTable.Delete(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)
	defer it.Close()
	assert.False(t, it.First())
}

func TestBondTable_Exist(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	ifExist := tokenBalanceTable.Exist(&TokenBalance{ID: 1})
	assert.False(t, ifExist)

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	ifExist = tokenBalanceTable.Exist(&TokenBalance{ID: 1})
	assert.True(t, ifExist)
}

func TestBondTable_Scan(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.Scan(context.Background(), &tokenBalances, false)
	require.NoError(t, err)
	require.Equal(t, len(tokenBalances), 3)

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])

	tokenBalances = nil
	err = tokenBalanceTable.Scan(context.Background(), &tokenBalances, true)
	require.NoError(t, err)
	require.Equal(t, len(tokenBalances), 3)

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[0])
}

func TestBondTable_Scan_Context_Canceled(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.Scan(ctx, &tokenBalances, false)
	require.Error(t, err)
}

func TestBondTable_ScanIndex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex(IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	_ = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.ScanIndex(context.Background(), TokenBalanceAccountAddressIndex,
		NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}), &tokenBalances, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
}

func TestBond_Batch(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	err := tokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	batch := db.Batch(BatchTypeReadWrite)

	exist := tokenBalanceTable.Exist(&TokenBalance{ID: 1}, batch)
	require.True(t, exist)

	tokenBalance, err := tokenBalanceTable.GetPoint(context.Background(), &TokenBalance{ID: 1}, batch)
	require.NoError(t, err)
	require.NotNil(t, tokenBalance)

	tokenBalance.Balance += 20

	err = tokenBalanceTable.Update(context.Background(), []*TokenBalance{tokenBalance}, batch)
	require.NoError(t, err)

	err = batch.Commit(Sync)
	require.NoError(t, err)

	it, err := db.Backend().NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(TokenBalanceTableID)},
		UpperBound: []byte{byte(TokenBalanceTableID + 1)},
	})
	require.NoError(t, err)

	for it.First(); it.Valid(); it.Next() {
		rawData := it.Value()

		var tokenBalanceAccountFromDB TokenBalance
		err = db.Serializer().Deserialize(rawData, &tokenBalanceAccountFromDB)
		require.NoError(t, err)
		assert.Equal(t, tokenBalance, &tokenBalanceAccountFromDB)
	}
	it.Close()
}

func TestBondTable_Case_TokenHistory_Basic(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenHistoryTableID TableID = 0xD0
	)

	tokenHistoryTable := NewTable(TableOptions[*TokenHistory]{
		DB:        db,
		TableID:   TokenHistoryTableID,
		TableName: "token_history",
		TablePrimaryKeyFunc: func(keyBuilder KeyBuilder, th *TokenHistory) []byte {
			return keyBuilder.
				AddUint64Field(th.BlockNumber).
				AddUint64Field(uint64(th.TxnIndex)).
				AddUint64Field(uint64(th.TxnLogIndex)).
				Bytes()
		},
	})

	require.NotNil(t, tokenHistoryTable)
	require.Equal(t, TokenHistoryTableID, tokenHistoryTable.ID())

	// add indexes
	TokenHistoryByAccountIndex := NewIndex(IndexOptions[*TokenHistory]{
		IndexID:   PrimaryIndexID + 1,
		IndexName: "token_history_by_account_idx",
		IndexKeyFunc: func(builder KeyBuilder, th *TokenHistory) []byte {
			// return builder.AddStringField(tb.AccountAddress).Bytes()
			return builder.AddStringField(th.FromAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, th *TokenHistory) IndexOrder {
			return o.
				OrderUint64(th.BlockNumber, IndexOrderTypeDESC).
				OrderUint64(uint64(th.TxnIndex), IndexOrderTypeASC).
				OrderUint64(uint64(th.TxnLogIndex), IndexOrderTypeASC)
		},
		// IndexOrderFunc: IndexOrderDefault[*TokenHistory],
	})

	// add indexes

	TokenHistoryByAccountToIndex := NewIndex(IndexOptions[*TokenHistory]{
		IndexID:   PrimaryIndexID + 2,
		IndexName: "token_history_by_account_to_idx",
		IndexKeyFunc: func(builder KeyBuilder, th *TokenHistory) []byte {
			// return builder.AddStringField(tb.AccountAddress).Bytes()
			return builder.AddStringField(th.ToAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, th *TokenHistory) IndexOrder {
			return o.
				OrderUint64(th.BlockNumber, IndexOrderTypeDESC).
				OrderUint64(uint64(th.TxnIndex), IndexOrderTypeASC).
				OrderUint64(uint64(th.TxnLogIndex), IndexOrderTypeASC)
		},
	})
	// _ = TokenHistoryByAccountToIndex

	_ = tokenHistoryTable.AddIndex([]*Index[*TokenHistory]{
		TokenHistoryByAccountIndex, // from ..
		TokenHistoryByAccountToIndex,
	})

	secondaryIndexes := tokenHistoryTable.SecondaryIndexes()
	require.NotNil(t, secondaryIndexes)
	// require.Equal(t, 2, len(secondaryIndexes))

	// add some records

	tokenHistory1 := &TokenHistory{
		BlockNumber:     1,
		TxnIndex:        1,
		TxnLogIndex:     1,
		FromAddress:     "0xabc1",
		ToAddress:       "0xdef1",
		ContractAddress: "0x1231",
		TokenIDs:        []uint64{1},
		Amounts:         []uint64{100},
		TS:              time.Now(),
	}

	tokenHistory2 := &TokenHistory{
		BlockNumber:     1,
		TxnIndex:        2,
		TxnLogIndex:     1,
		FromAddress:     "0xabc2",
		ToAddress:       "0xdef2",
		ContractAddress: "0x1232",
		TokenIDs:        []uint64{1},
		Amounts:         []uint64{200},
		TS:              time.Now(),
	}

	err := tokenHistoryTable.Insert(context.Background(), []*TokenHistory{tokenHistory1, tokenHistory2})
	require.NoError(t, err)

	// test scan index

	{
		var tokenHistories []*TokenHistory
		err = tokenHistoryTable.Scan(context.Background(), &tokenHistories, false)
		require.NoError(t, err)
		// spew.Dump(tokenHistories)
	}

	t.Log("ScanIndex")
	{
		var tokenHistories []*TokenHistory

		// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
		// of DESC for block number.
		selector := NewSelectorRange(
			&TokenHistory{FromAddress: "0xabc1", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
			&TokenHistory{FromAddress: "0xabc1", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
		)
		err = tokenHistoryTable.ScanIndex(context.Background(), TokenHistoryByAccountIndex, selector, &tokenHistories, false)
		require.NoError(t, err)
		// spew.Dump(tokenHistories)
	}

	t.Log("GetPoint")
	{
		tokenHistory, err := tokenHistoryTable.GetPoint(context.Background(), &TokenHistory{BlockNumber: 1, TxnIndex: 1, TxnLogIndex: 1}, nil)
		require.NoError(t, err)
		require.NotNil(t, tokenHistory)
		// spew.Dump(tokenHistory)
	}

	t.Log("Query")
	{
		// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
		// of DESC for block number.
		selector := NewSelectorRange(
			&TokenHistory{FromAddress: "0xabc1", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
			&TokenHistory{FromAddress: "0xabc1", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
		)

		query := tokenHistoryTable.Query().
			With(TokenHistoryByAccountIndex, selector)

		var tokenHistories []*TokenHistory
		err = query.Execute(context.Background(), &tokenHistories)
		require.NoError(t, err)
		require.Equal(t, 1, len(tokenHistories))
		// spew.Dump(tokenHistories)
	}
}

func TestBondTable_Case_TokenHistory_IndexMultiKeyFunc(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	const (
		TokenHistoryTableID TableID = 0xD0
	)

	tokenHistoryTable := NewTable(TableOptions[*TokenHistory]{
		DB:        db,
		TableID:   TokenHistoryTableID,
		TableName: "token_history",
		TablePrimaryKeyFunc: func(keyBuilder KeyBuilder, th *TokenHistory) []byte {
			return keyBuilder.
				AddUint64Field(th.BlockNumber).
				AddUint64Field(uint64(th.TxnIndex)).
				AddUint64Field(uint64(th.TxnLogIndex)).
				Bytes()
		},
	})

	require.NotNil(t, tokenHistoryTable)
	require.Equal(t, TokenHistoryTableID, tokenHistoryTable.ID())

	// add indexes
	TokenHistoryByAccountIndex := NewIndex(IndexOptions[*TokenHistory]{
		IndexID:   PrimaryIndexID + 1,
		IndexName: "token_history_by_account_idx",
		IndexKeyFunc: func(builder KeyBuilder, selector *TokenHistory) []byte {
			if selector.QueryFromOrToAddress != "" {
				return builder.AddStringField(selector.QueryFromOrToAddress).Bytes()
			}
			if selector.FromAddress != "" {
				return builder.AddStringField(selector.FromAddress).Bytes()
			}
			if selector.ToAddress != "" {
				return builder.AddStringField(selector.ToAddress).Bytes()
			}
			return builder.Bytes()
		},
		IndexMultiKeyFunc: func(builder KeyBuilder, record *TokenHistory) [][]byte {
			var indexKeys [][]byte

			// Generate key part for FromAddress
			if record.FromAddress != "" {
				// Use a *new* builder for each distinct key part
				fromKeyPart := NewKeyBuilder(nil).AddStringField(record.FromAddress).Bytes()
				indexKeys = append(indexKeys, fromKeyPart)
			}

			// Generate key part for ToAddress and avoid adding duplicate key parts if FromAddress == ToAddress
			if record.ToAddress != "" && record.ToAddress != record.FromAddress {
				// Use a *new* builder
				toKeyPart := NewKeyBuilder(nil).AddStringField(record.ToAddress).Bytes()
				indexKeys = append(indexKeys, toKeyPart)
			}
			return indexKeys
		},
		IndexOrderFunc: func(o IndexOrder, th *TokenHistory) IndexOrder {
			return o.
				OrderUint64(th.BlockNumber, IndexOrderTypeDESC).
				OrderUint64(uint64(th.TxnIndex), IndexOrderTypeASC).
				OrderUint64(uint64(th.TxnLogIndex), IndexOrderTypeASC)
		},
	})

	_ = tokenHistoryTable.AddIndex([]*Index[*TokenHistory]{
		TokenHistoryByAccountIndex,
	})

	secondaryIndexes := tokenHistoryTable.SecondaryIndexes()
	require.NotNil(t, secondaryIndexes)
	require.Equal(t, 1, len(secondaryIndexes))

	// add some records
	tokenHistory1 := &TokenHistory{
		BlockNumber:     1,
		TxnIndex:        1,
		TxnLogIndex:     1,
		FromAddress:     "0xabc1",
		ToAddress:       "0xdef1",
		ContractAddress: "0x1231",
		TokenIDs:        []uint64{1},
		Amounts:         []uint64{100},
		TS:              time.Now(),
	}

	tokenHistory2 := &TokenHistory{
		BlockNumber:     1,
		TxnIndex:        2,
		TxnLogIndex:     1,
		FromAddress:     "0xabc2",
		ToAddress:       "0xdef2",
		ContractAddress: "0x1232",
		TokenIDs:        []uint64{1},
		Amounts:         []uint64{200},
		TS:              time.Now(),
	}

	tokenHistory3 := &TokenHistory{
		BlockNumber:     1,
		TxnIndex:        3,
		TxnLogIndex:     1,
		FromAddress:     "0xfff3",
		ToAddress:       "0xabc1",
		ContractAddress: "0x1233",
		TokenIDs:        []uint64{1},
		Amounts:         []uint64{300},
		TS:              time.Now(),
	}

	err := tokenHistoryTable.Insert(context.Background(), []*TokenHistory{tokenHistory1, tokenHistory2, tokenHistory3})
	require.NoError(t, err)

	// test scan index
	t.Run("Scan all", func(t *testing.T) {
		var tokenHistories []*TokenHistory
		err = tokenHistoryTable.Scan(context.Background(), &tokenHistories, false)
		require.NoError(t, err)
		require.Equal(t, 3, len(tokenHistories))

		assert.Equal(t, tokenHistories[0].BlockNumber, tokenHistory1.BlockNumber)
		assert.Equal(t, tokenHistories[0].TxnIndex, tokenHistory1.TxnIndex)
		assert.Equal(t, tokenHistories[0].TxnLogIndex, tokenHistory1.TxnLogIndex)
		assert.Equal(t, tokenHistories[0].TxnHash, tokenHistory1.TxnHash)
		assert.Equal(t, tokenHistories[0].FromAddress, tokenHistory1.FromAddress)
		assert.Equal(t, tokenHistories[0].ToAddress, tokenHistory1.ToAddress)
		assert.Equal(t, tokenHistories[0].ContractAddress, tokenHistory1.ContractAddress)
		assert.Equal(t, tokenHistories[0].TokenIDs, tokenHistory1.TokenIDs)
		assert.Equal(t, tokenHistories[0].Amounts, tokenHistory1.Amounts)

		assert.Equal(t, tokenHistories[1].BlockNumber, tokenHistory2.BlockNumber)
		assert.Equal(t, tokenHistories[1].TxnIndex, tokenHistory2.TxnIndex)
		assert.Equal(t, tokenHistories[1].TxnLogIndex, tokenHistory2.TxnLogIndex)
		assert.Equal(t, tokenHistories[1].TxnHash, tokenHistory2.TxnHash)
		assert.Equal(t, tokenHistories[1].FromAddress, tokenHistory2.FromAddress)
		assert.Equal(t, tokenHistories[1].ToAddress, tokenHistory2.ToAddress)
		assert.Equal(t, tokenHistories[1].ContractAddress, tokenHistory2.ContractAddress)
		assert.Equal(t, tokenHistories[1].TokenIDs, tokenHistory2.TokenIDs)
		assert.Equal(t, tokenHistories[1].Amounts, tokenHistory2.Amounts)

		assert.Equal(t, tokenHistories[2].BlockNumber, tokenHistory3.BlockNumber)
		assert.Equal(t, tokenHistories[2].TxnIndex, tokenHistory3.TxnIndex)
		assert.Equal(t, tokenHistories[2].TxnLogIndex, tokenHistory3.TxnLogIndex)
		assert.Equal(t, tokenHistories[2].TxnHash, tokenHistory3.TxnHash)
		assert.Equal(t, tokenHistories[2].FromAddress, tokenHistory3.FromAddress)
		assert.Equal(t, tokenHistories[2].ToAddress, tokenHistory3.ToAddress)
		assert.Equal(t, tokenHistories[2].ContractAddress, tokenHistory3.ContractAddress)
		assert.Equal(t, tokenHistories[2].TokenIDs, tokenHistory3.TokenIDs)
		assert.Equal(t, tokenHistories[2].Amounts, tokenHistory3.Amounts)
	})

	t.Run("ScanIndex", func(t *testing.T) {
		var tokenHistories []*TokenHistory

		// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
		// of DESC for block number.
		selector := NewSelectorRange(
			&TokenHistory{FromAddress: "0xabc1", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
			&TokenHistory{FromAddress: "0xabc1", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
		)
		err = tokenHistoryTable.ScanIndex(context.Background(), TokenHistoryByAccountIndex, selector, &tokenHistories, false)
		require.NoError(t, err)
		require.Equal(t, 2, len(tokenHistories))

		assert.Equal(t, tokenHistories[0].BlockNumber, tokenHistory1.BlockNumber)
		assert.Equal(t, tokenHistories[0].TxnIndex, tokenHistory1.TxnIndex)
		assert.Equal(t, tokenHistories[0].TxnLogIndex, tokenHistory1.TxnLogIndex)
		assert.Equal(t, tokenHistories[0].TxnHash, tokenHistory1.TxnHash)
		assert.Equal(t, tokenHistories[0].FromAddress, tokenHistory1.FromAddress)
		assert.Equal(t, tokenHistories[0].ToAddress, tokenHistory1.ToAddress)
		assert.Equal(t, tokenHistories[0].ContractAddress, tokenHistory1.ContractAddress)
		assert.Equal(t, tokenHistories[0].TokenIDs, tokenHistory1.TokenIDs)
		assert.Equal(t, tokenHistories[0].Amounts, tokenHistory1.Amounts)

		assert.Equal(t, tokenHistories[1].BlockNumber, tokenHistory3.BlockNumber)
		assert.Equal(t, tokenHistories[1].TxnIndex, tokenHistory3.TxnIndex)
		assert.Equal(t, tokenHistories[1].TxnLogIndex, tokenHistory3.TxnLogIndex)
		assert.Equal(t, tokenHistories[1].TxnHash, tokenHistory3.TxnHash)
		assert.Equal(t, tokenHistories[1].FromAddress, tokenHistory3.FromAddress)
		assert.Equal(t, tokenHistories[1].ToAddress, tokenHistory3.ToAddress)
		assert.Equal(t, tokenHistories[1].ContractAddress, tokenHistory3.ContractAddress)
		assert.Equal(t, tokenHistories[1].TokenIDs, tokenHistory3.TokenIDs)
		assert.Equal(t, tokenHistories[1].Amounts, tokenHistory3.Amounts)
	})

	t.Run("GetPoint", func(t *testing.T) {
		{
			tokenHistory, err := tokenHistoryTable.GetPoint(context.Background(), &TokenHistory{BlockNumber: 1, TxnIndex: 1, TxnLogIndex: 1}, nil)
			require.NoError(t, err)
			require.Equal(t, tokenHistory.BlockNumber, tokenHistory1.BlockNumber)
			require.Equal(t, tokenHistory.TxnIndex, tokenHistory1.TxnIndex)
			require.Equal(t, tokenHistory.TxnLogIndex, tokenHistory1.TxnLogIndex)
			require.Equal(t, tokenHistory.TxnHash, tokenHistory1.TxnHash)
			require.Equal(t, tokenHistory.FromAddress, tokenHistory1.FromAddress)
			require.Equal(t, tokenHistory.ToAddress, tokenHistory1.ToAddress)
			require.Equal(t, tokenHistory.ContractAddress, tokenHistory1.ContractAddress)
			require.Equal(t, tokenHistory.TokenIDs, tokenHistory1.TokenIDs)
			require.Equal(t, tokenHistory.Amounts, tokenHistory1.Amounts)
		}
	})

	// test query
	t.Run("Query", func(t *testing.T) {
		{
			// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
			// of DESC for block number.
			selector := NewSelectorRange(
				&TokenHistory{QueryFromOrToAddress: "0xabc1", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
				&TokenHistory{QueryFromOrToAddress: "0xabc1", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
			)

			query := tokenHistoryTable.Query().
				With(TokenHistoryByAccountIndex, selector)

			var tokenHistories []*TokenHistory
			err = query.Execute(context.Background(), &tokenHistories)
			require.NoError(t, err)
			require.Equal(t, 2, len(tokenHistories))

			assert.Equal(t, tokenHistories[0].BlockNumber, tokenHistory1.BlockNumber)
			assert.Equal(t, tokenHistories[0].TxnIndex, tokenHistory1.TxnIndex)
			assert.Equal(t, tokenHistories[0].TxnLogIndex, tokenHistory1.TxnLogIndex)
			assert.Equal(t, tokenHistories[0].TxnHash, tokenHistory1.TxnHash)
			assert.Equal(t, tokenHistories[0].FromAddress, tokenHistory1.FromAddress)
			assert.Equal(t, tokenHistories[0].ToAddress, tokenHistory1.ToAddress)
			assert.Equal(t, tokenHistories[0].ContractAddress, tokenHistory1.ContractAddress)
			assert.Equal(t, tokenHistories[0].TokenIDs, tokenHistory1.TokenIDs)
			assert.Equal(t, tokenHistories[0].Amounts, tokenHistory1.Amounts)

			assert.Equal(t, tokenHistories[1].BlockNumber, tokenHistory3.BlockNumber)
			assert.Equal(t, tokenHistories[1].TxnIndex, tokenHistory3.TxnIndex)
			assert.Equal(t, tokenHistories[1].TxnLogIndex, tokenHistory3.TxnLogIndex)
			assert.Equal(t, tokenHistories[1].TxnHash, tokenHistory3.TxnHash)
			assert.Equal(t, tokenHistories[1].FromAddress, tokenHistory3.FromAddress)
			assert.Equal(t, tokenHistories[1].ToAddress, tokenHistory3.ToAddress)
			assert.Equal(t, tokenHistories[1].ContractAddress, tokenHistory3.ContractAddress)
			assert.Equal(t, tokenHistories[1].TokenIDs, tokenHistory3.TokenIDs)
			assert.Equal(t, tokenHistories[1].Amounts, tokenHistory3.Amounts)
		}
	})

	// test update and query
	t.Run("Update And Query", func(t *testing.T) {
		{
			tokenHistory1.Amounts = []uint64{1000}

			err = tokenHistoryTable.Update(context.Background(), []*TokenHistory{tokenHistory1})
			require.NoError(t, err)

			// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
			// of DESC for block number.
			selector := NewSelectorRange(
				&TokenHistory{QueryFromOrToAddress: "0xabc1", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
				&TokenHistory{QueryFromOrToAddress: "0xabc1", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
			)

			query := tokenHistoryTable.Query().
				With(TokenHistoryByAccountIndex, selector)

			var tokenHistories []*TokenHistory
			err = query.Execute(context.Background(), &tokenHistories)
			require.NoError(t, err)
			require.Equal(t, 2, len(tokenHistories))

			assert.Equal(t, tokenHistories[0].BlockNumber, tokenHistory1.BlockNumber)
			assert.Equal(t, tokenHistories[0].TxnIndex, tokenHistory1.TxnIndex)
			assert.Equal(t, tokenHistories[0].TxnLogIndex, tokenHistory1.TxnLogIndex)
			assert.Equal(t, tokenHistories[0].TxnHash, tokenHistory1.TxnHash)
			assert.Equal(t, tokenHistories[0].FromAddress, tokenHistory1.FromAddress)
			assert.Equal(t, tokenHistories[0].ToAddress, tokenHistory1.ToAddress)
			assert.Equal(t, tokenHistories[0].ContractAddress, tokenHistory1.ContractAddress)
			assert.Equal(t, tokenHistories[0].TokenIDs, tokenHistory1.TokenIDs)
			assert.Equal(t, tokenHistories[0].Amounts, tokenHistory1.Amounts)

			assert.Equal(t, tokenHistories[1].BlockNumber, tokenHistory3.BlockNumber)
			assert.Equal(t, tokenHistories[1].TxnIndex, tokenHistory3.TxnIndex)
			assert.Equal(t, tokenHistories[1].TxnLogIndex, tokenHistory3.TxnLogIndex)
			assert.Equal(t, tokenHistories[1].TxnHash, tokenHistory3.TxnHash)
			assert.Equal(t, tokenHistories[1].FromAddress, tokenHistory3.FromAddress)
			assert.Equal(t, tokenHistories[1].ToAddress, tokenHistory3.ToAddress)
			assert.Equal(t, tokenHistories[1].ContractAddress, tokenHistory3.ContractAddress)
			assert.Equal(t, tokenHistories[1].TokenIDs, tokenHistory3.TokenIDs)
			assert.Equal(t, tokenHistories[1].Amounts, tokenHistory3.Amounts)
		}

		t.Log("Update Change Address And Query")
		{
			tokenHistory1.FromAddress = "0xfff3"

			err = tokenHistoryTable.Update(context.Background(), []*TokenHistory{tokenHistory1})
			require.NoError(t, err)

			// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
			// of DESC for block number.
			selector := NewSelectorRange(
				&TokenHistory{QueryFromOrToAddress: "0xabc1", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
				&TokenHistory{QueryFromOrToAddress: "0xabc1", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
			)

			query := tokenHistoryTable.Query().
				With(TokenHistoryByAccountIndex, selector)

			var tokenHistories []*TokenHistory
			err = query.Execute(context.Background(), &tokenHistories)
			require.NoError(t, err)
			require.Equal(t, 1, len(tokenHistories))

			assert.Equal(t, tokenHistories[0].BlockNumber, tokenHistory3.BlockNumber)
			assert.Equal(t, tokenHistories[0].TxnIndex, tokenHistory3.TxnIndex)
			assert.Equal(t, tokenHistories[0].TxnLogIndex, tokenHistory3.TxnLogIndex)
			assert.Equal(t, tokenHistories[0].TxnHash, tokenHistory3.TxnHash)
			assert.Equal(t, tokenHistories[0].FromAddress, tokenHistory3.FromAddress)
			assert.Equal(t, tokenHistories[0].ToAddress, tokenHistory3.ToAddress)
			assert.Equal(t, tokenHistories[0].ContractAddress, tokenHistory3.ContractAddress)
			assert.Equal(t, tokenHistories[0].TokenIDs, tokenHistory3.TokenIDs)
			assert.Equal(t, tokenHistories[0].Amounts, tokenHistory3.Amounts)
		}
	})

	t.Run("Delete And Query", func(t *testing.T) {
		{
			err = tokenHistoryTable.Delete(context.Background(), []*TokenHistory{tokenHistory2})
			require.NoError(t, err)

			// we pass the selector range, to consider the IndexOrderFunc too, and notice the range
			// of DESC for block number.
			selector := NewSelectorRange(
				&TokenHistory{QueryFromOrToAddress: "0xabc2", BlockNumber: math.MaxUint64, TxnIndex: 0, TxnLogIndex: 0},
				&TokenHistory{QueryFromOrToAddress: "0xabc2", BlockNumber: 0, TxnIndex: math.MaxUint, TxnLogIndex: math.MaxUint},
			)

			query := tokenHistoryTable.Query().
				With(TokenHistoryByAccountIndex, selector)

			var tokenHistories []*TokenHistory
			err = query.Execute(context.Background(), &tokenHistories)
			require.NoError(t, err)
			require.Equal(t, 0, len(tokenHistories))
		}
	})
}

func BenchmarkBondTable_Upsert(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(nil, db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	var tokenBalances []*TokenBalance
	for i := 0; i < 10000; i++ {
		tokenBalances = append(tokenBalances, &TokenBalance{
			ID:              uint64(i),
			AccountAddress:  fmt.Sprintf("account_%d", i),
			ContractAddress: fmt.Sprintf("contract_%d", i),
			Balance:         uint64(i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := tokenBalanceTable.Upsert(context.Background(), tokenBalances, TableUpsertOnConflictReplace[*TokenBalance])
		require.NoError(b, err)
	}
}
