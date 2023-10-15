package bond

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/serializers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_NewTable(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
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

func TestBondTable_Serializer(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	tokenBalance, err := tokenBalanceTable.GetPoint(context.Background(), NewSelectorPoint(&TokenBalance{ID: tokenBalanceAccount1.ID}))
	require.NoError(t, err)
	assert.Equal(t, tokenBalanceAccount1, tokenBalance)

	// get token balance with non-existing id
	tokenBalance, err = tokenBalanceTable.GetPoint(context.Background(), NewSelectorPoint(&TokenBalance{ID: 2}))
	require.Error(t, err)
	assert.Nil(t, tokenBalance)
}

func TestBondTable_Get_Range(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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

	assert.Equal(t, insertTokenBalances, tokenBalances)

	// get token balance with non-existing points
	tokenBalances, err = tokenBalanceTable.Get(context.Background(), NewSelectorPoints(&TokenBalance{ID: 2}))
	require.NoError(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Nil(t, tokenBalances[0])
}

func TestBondTable_Get_Ranges(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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

	err = tokenBalanceTable.Upsert(
		context.Background(),
		[]*TokenBalance{tokenBalanceAccountUpdated, tokenBalanceAccount2},
		TableUpsertOnConflictReplace[*TokenBalance])
	require.NoError(t, err)

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
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}

	_ = it.Close()

	require.Equal(t, 2, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccountUpdated, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount2, tokenBalances[1])
}

func TestBondTable_Upsert_Context_Canceled(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	err = tokenBalanceTable.Upsert(
		ctx,
		[]*TokenBalance{tokenBalanceAccountUpdated, tokenBalanceAccount2},
		TableUpsertOnConflictReplace[*TokenBalance])
	require.Error(t, err)

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
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}

	_ = it.Close()

	require.Equal(t, 1, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccount, tokenBalances[0])
}

func TestBondTable_Upsert_OnConflict(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	err = tokenBalanceTable.Upsert(
		context.Background(),
		[]*TokenBalance{tokenBalanceAccountUpdate, tokenBalanceAccount2}, onConflictAddBalance)
	require.NoError(t, err)

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
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}

	_ = it.Close()

	require.Equal(t, 2, len(tokenBalances))
	assert.Equal(t, expectedTokenBalanceAccountUpdated, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount2, tokenBalances[1])
}

func TestBondTable_Upsert_OnConflict_Two_Updates_Same_Row(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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
		Balance:         19,
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

	err = tokenBalanceTable.Upsert(
		context.Background(),
		[]*TokenBalance{tokenBalanceAccountUpdate, tokenBalanceAccountUpdate, tokenBalanceAccount2}, onConflictAddBalance)
	require.NoError(t, err)

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
		tokenBalances = append(tokenBalances, &tokenBalanceAccountFromDB)
	}

	_ = it.Close()

	require.Equal(t, 2, len(tokenBalances))
	assert.Equal(t, expectedTokenBalanceAccountUpdated, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount2, tokenBalances[1])
}

func TestBondTable_Update_No_Such_Entry(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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

	assert.False(t, it.First())
}

func TestBondTable_Delete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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
	assert.False(t, it.First())
}

func TestBondTable_Exist(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	defer tearDownDatabase(db)

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
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
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
	defer tearDownDatabase(db)

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

	batch := db.Batch()

	exist := tokenBalanceTable.Exist(&TokenBalance{ID: 1}, batch)
	require.True(t, exist)

	tokenBalance, err := tokenBalanceTable.GetPoint(context.Background(), NewSelectorPoint(&TokenBalance{ID: 1}), batch)
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
}
