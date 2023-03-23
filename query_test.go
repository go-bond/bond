package bond

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDatabaseForQuery() (DB, Table[*TokenBalance], *Index[*TokenBalance], *Index[*TokenBalance], *Index[*TokenBalance]) {
	db := setupDatabase()

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
		TokenBalanceContractAddressIndexID
		TokenBalanceAccountAndContractAddressIndexID
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
		TokenBalanceContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceContractAddressIndexID,
			IndexName: "contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.ContractAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceContractAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	if err != nil {
		panic(err)
	}

	return db, tokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceContractAddressIndex, TokenBalanceAccountAndContractAddressIndex
}

func TestBond_Query_OnOrderedIndex(t *testing.T) {
	db, TokenBalanceTable, _, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceOrderedIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   lastIndex.IndexID + 1,
		IndexName: "account_address_ord_desc_bal_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})
	_ = TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceOrderedIndex})

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])
}

func TestBond_Query_Context_Canceled(t *testing.T) {
	db, TokenBalanceTable, _, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceOrderedIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   lastIndex.IndexID + 1,
		IndexName: "account_address_ord_desc_bal_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})
	_ = TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceOrderedIndex})

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})

	err = query.Execute(ctx, &tokenBalances)
	require.Error(t, err)
}

func TestBond_Query_Last_Row_As_Selector(t *testing.T) {
	db, TokenBalanceTable, _, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceOrderedIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   lastIndex.IndexID + 1,
		IndexName: "account_address_ord_desc_bal_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})

	err := TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceOrderedIndex})
	require.NoError(t, err)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err = TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, tokenBalances[1])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, tokenBalances[1])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))
}

func TestBond_Query_After(t *testing.T) {
	db, TokenBalanceTable, _, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceOrderedIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   lastIndex.IndexID + 1,
		IndexName: "account_address_ord_desc_bal_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})

	err := TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceOrderedIndex})
	require.NoError(t, err)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err = TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}).
		After(tokenBalances[1])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}).
		After(tokenBalances[0])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))

	err = TokenBalanceTable.Delete(context.Background(), []*TokenBalance{tokenBalance3Account1})
	require.NoError(t, err)

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}).
		After(tokenBalance3Account1)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}).
		Filter(func(r *TokenBalance) bool {
			return r.AccountAddress == "0xtestAccount"
		}).After(tokenBalance3Account1)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))
}

func TestBond_Query_After_With_Order_Error(t *testing.T) {
	db, TokenBalanceTable, _, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceOrderedIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   lastIndex.IndexID + 1,
		IndexName: "account_address_ord_desc_bal_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})

	err := TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceOrderedIndex})
	require.NoError(t, err)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err = TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}).
		After(tokenBalance2Account1).
		Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
			return tb.ID < tb2.ID
		})

	err = query.Execute(context.Background(), &tokenBalances)
	require.Error(t, err)
}

func TestBond_Query_Where(t *testing.T) {
	db, TokenBalanceTable, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance > 10
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[2])
}

func TestBond_Query_Where_Offset_Limit(t *testing.T) {
	db, TokenBalanceTable, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Offset(1).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Offset(3).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance1Account2, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Offset(4).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Where_Offset_Limit_With_Filter(t *testing.T) {
	db, TokenBalanceTable, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		}).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		}).
		Offset(1).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		}).
		Offset(3).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Where_Offset_Limit_With_Order(t *testing.T) {
	db, TokenBalanceTable, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Order(func(tr *TokenBalance, tr2 *TokenBalance) bool {
			return tr.Balance > tr2.Balance
		}).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Order(func(tr *TokenBalance, tr2 *TokenBalance) bool {
			return tr.Balance > tr2.Balance
		}).
		Offset(1).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance3Account1, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Order(func(tr *TokenBalance, tr2 *TokenBalance) bool {
			return tr.Balance > tr2.Balance
		}).
		Offset(3).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance1Account2, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Order(func(tr *TokenBalance, tr2 *TokenBalance) bool {
			return tr.Balance > tr2.Balance
		}).
		Offset(4).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Order(t *testing.T) {
	db, TokenBalanceTable, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])

	query = TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		}).
		Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
			return tb.Balance < tb2.Balance
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance1Account2, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[2])

	query = TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		}).
		Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
			return tb.Balance > tb2.Balance
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance3Account1, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])
}

func TestBond_Query_Indexes_Mix(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, _, TokenBalanceAccountAndContractAddressIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance > 10
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"}).
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"}).
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		}).
		Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
			return tb.Balance > tb2.Balance
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance3Account1, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		With(
			TokenBalanceAccountAndContractAddressIndex,
			&TokenBalance{AccountAddress: "0xtestAccount", ContractAddress: "0xtestContract"},
		).
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 15
		}).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)

	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
}

func TestBond_Query_Indexes_Intersect(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceContractAddressIndex, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	expectedTokenBalances := []*TokenBalance{
		{
			ID:              2,
			AccountID:       1,
			ContractAddress: "0xtestContract2",
			AccountAddress:  "0xtestAccount",
			Balance:         15,
		},
	}

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"})
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, &TokenBalance{ContractAddress: "0xtestContract2"})

	var tokenBalances []*TokenBalance

	err = q1.Intersects(q2).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)
}

func TestBond_Query_Indexes_Intersect_Offset_Limit(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceContractAddressIndex, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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
		TokenID:         1,
		Balance:         15,
	}

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance4Account1TokenID2 := &TokenBalance{
		ID:              5,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		TokenID:         3,
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance4Account1TokenID2,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	expectedTokenBalances := []*TokenBalance{
		tokenBalance2Account1,
		tokenBalance4Account1TokenID2,
	}

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"})
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, &TokenBalance{ContractAddress: "0xtestContract2"})

	var tokenBalances []*TokenBalance

	err = q1.Intersects(q2).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance2Account1,
	}

	err = q1.Intersects(q2).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance4Account1TokenID2,
	}

	err = q1.Intersects(q2).Offset(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)
}

func TestBond_Query_Indexes_Intersect_Filter_Offset_Limit(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceContractAddressIndex, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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
		TokenID:         1,
		Balance:         15,
	}

	tokenBalance4Account1TokenID2 := &TokenBalance{
		ID:              5,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		TokenID:         3,
		Balance:         7,
	}

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance4Account1TokenID2,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	expectedTokenBalances := []*TokenBalance{
		tokenBalance2Account1,
		tokenBalance4Account1TokenID2,
	}

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"})
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, &TokenBalance{ContractAddress: "0xtestContract2"})

	var tokenBalances []*TokenBalance

	err = q1.Intersects(q2).Filter(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount"
	}).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance2Account1,
	}

	err = q1.Intersects(q2).Filter(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount"
	}).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance4Account1TokenID2,
	}

	err = q1.Intersects(q2).Filter(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount"
	}).Offset(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{}

	err = q1.Intersects(q2).Filter(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount2"
	}).Offset(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)
}

func BenchmarkQuery_Intersects(b *testing.B) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceContractAddressIndex, TokenBalanceAccountAndContractAddress := setupDatabaseForQuery()
	defer tearDownDatabase(db)

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

	tokenBalance3Account1 := &TokenBalance{
		ID:              3,
		AccountID:       1,
		ContractAddress: "0xtestContract3",
		AccountAddress:  "0xtestAccount",
		Balance:         7,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              4,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         4,
	}

	err := TokenBalanceTable.Insert(
		context.Background(),
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance3Account1,
			tokenBalance1Account2,
		},
	)
	if err != nil {
		panic(err)
	}

	b.Run("NoIntersect", func(b *testing.B) {
		q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAndContractAddress, &TokenBalance{AccountAddress: "0xtestAccount", ContractAddress: "0xtestContract2"})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance
			err = q1.Execute(context.Background(), &tokenBalances)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("Intersect_2", func(b *testing.B) {
		q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"})
		q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, &TokenBalance{ContractAddress: "0xtestContract2"})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance
			err = q1.Intersects(q2).Execute(context.Background(), &tokenBalances)
			if err != nil {
				panic(err)
			}
		}
	})
}
