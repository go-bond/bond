package bond

import (
	"context"
	"math"
	"testing"

	"github.com/go-bond/bond/cond"
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
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}))

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])

	// after update
	tokenBalanceAccount1.Balance = 200

	err = TokenBalanceTable.Update(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}))

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[2])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		Reverse()

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance3Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])
}

func TestBond_Query_OnFilterIndex(t *testing.T) {
	db, TokenBalanceTable, _, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceFilterIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   lastIndex.IndexID + 1,
		IndexName: "account_address_filter_bal_idx",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		IndexFilterFunc: func(tb *TokenBalance) bool {
			return tb.Balance > 10
		},
	})

	err := TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceFilterIndex})
	require.NoError(t, err)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	err = TokenBalanceTable.Insert(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	var tokenBalances []*TokenBalance

	err = TokenBalanceTable.Query().
		With(TokenBalanceFilterIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})).
		Limit(50).
		Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)

	require.Equal(t, 1, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])

	tokenBalanceAccount1.Balance = 5

	err = TokenBalanceTable.Update(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	err = TokenBalanceTable.Query().
		With(TokenBalanceFilterIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})).
		Limit(50).
		Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)

	require.Equal(t, 0, len(tokenBalances))

	tokenBalanceAccount1.Balance = 20

	err = TokenBalanceTable.Update(context.Background(), []*TokenBalance{tokenBalanceAccount1})
	require.NoError(t, err)

	err = TokenBalanceTable.Query().
		With(TokenBalanceFilterIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})).
		Limit(50).
		Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)

	require.Equal(t, 1, len(tokenBalances))
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
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
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}))

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
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}))

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(tokenBalances[1]))

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(tokenBalances[1]))

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
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64}))

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		After(tokenBalances[1])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		After(tokenBalances[0])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))

	err = TokenBalanceTable.Delete(context.Background(), []*TokenBalance{tokenBalance3Account1})
	require.NoError(t, err)

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		After(tokenBalance3Account1)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		Filter(CondFunc(func(r *TokenBalance) bool {
			return r.AccountAddress == "0xtestAccount"
		})).After(tokenBalance3Account1)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		Order(func(tb, tb2 *TokenBalance) bool {
			return tb.Balance > tb2.Balance
		}).Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	query = TokenBalanceTable.Query().
		With(TokenBalanceOrderedIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", Balance: math.MaxUint64})).
		Order(func(tb, tb2 *TokenBalance) bool {
			return tb.Balance > tb2.Balance
		}).Limit(2).After(tokenBalances[1])

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
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
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance > 10
		})).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		})).
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
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		})).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		})).
		Offset(1).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		})).
		Offset(3).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Where_Offset_Limit_With_Filter_With_Cond(t *testing.T) {
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

	var AccountAddressGetter = func(r *TokenBalance) string {
		return r.AccountAddress
	}

	query := TokenBalanceTable.Query().
		Filter(cond.Eq(AccountAddressGetter, "0xtestAccount")).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Filter(cond.Eq(AccountAddressGetter, "0xtestAccount")).
		Offset(1).
		Limit(2)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Filter(cond.Func(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		})).
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
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		})).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])

	query = TokenBalanceTable.Query().
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		})).
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
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		})).
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
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance > 10
		})).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})).
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		})).
		Limit(50)

	err = query.Execute(context.Background(), &tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})).
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		})).
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
			NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", ContractAddress: "0xtestContract"}),
		).
		Filter(CondFunc(func(tb *TokenBalance) bool {
			return tb.Balance < 15
		})).
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

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}))
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract2"}))

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

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}))
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract2"}))

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

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}))
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract2"}))

	var tokenBalances []*TokenBalance

	err = q1.Intersects(q2).Filter(CondFunc(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount"
	})).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance2Account1,
	}

	err = q1.Intersects(q2).Filter(CondFunc(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount"
	})).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance4Account1TokenID2,
	}

	err = q1.Intersects(q2).Filter(CondFunc(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount"
	})).Offset(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{}

	err = q1.Intersects(q2).Filter(CondFunc(func(r *TokenBalance) bool {
		return r.AccountAddress == "0xtestAccount2"
	})).Offset(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)
}

func TestBond_Query_Indexes_Intersect_Sort(t *testing.T) {
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
	}

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}))
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract2"}))

	var tokenBalances []*TokenBalance

	err = q1.Intersects(q2).Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
		return tb.Balance > tb2.Balance
	}).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance4Account1TokenID2,
	}

	err = q1.Intersects(q2).Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
		return tb.Balance < tb2.Balance
	}).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance2Account1,
	}

	err = q1.Intersects(q2).Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
		return tb.Balance < tb2.Balance
	}).Offset(1).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)
}

func TestBond_Query_Indexes_Intersect_After(t *testing.T) {
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
	}

	q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}))
	q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract2"}))

	var tokenBalances []*TokenBalance

	err = q1.Intersects(q2).Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
		return tb.Balance > tb2.Balance
	}).Limit(1).Execute(context.Background(), &tokenBalances)
	require.NoError(t, err)

	assert.Equal(t, expectedTokenBalances, tokenBalances)

	expectedTokenBalances = []*TokenBalance{
		tokenBalance4Account1TokenID2,
	}

	err = q1.Intersects(q2).Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
		return tb.Balance > tb2.Balance
	}).After(tokenBalances[0]).Limit(1).Execute(context.Background(), &tokenBalances)
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
		q1 := TokenBalanceTable.Query().With(
			TokenBalanceAccountAndContractAddress,
			NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount", ContractAddress: "0xtestContract2"}),
		)

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
		q1 := TokenBalanceTable.Query().With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"}))
		q2 := TokenBalanceTable.Query().With(TokenBalanceContractAddressIndex, NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract2"}))

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

func TestQuery_Selectors(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, _ /*TokenBalanceContractAddressIndex*/, _ /*TokenBalanceAccountAndContractAddress*/ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceTablePrimaryIndex := TokenBalanceTable.PrimaryIndex()

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

	testCases := []struct {
		name  string
		query Query[*TokenBalance]
		want  []*TokenBalance
	}{
		{
			name:  "SelectorPoint",
			query: TokenBalanceTable.Query(),
			want: []*TokenBalance{
				tokenBalanceAccount1,
				tokenBalance2Account1,
				tokenBalance3Account1,
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorPoint_From_ID_4",
			query: TokenBalanceTable.Query().
				With(TokenBalanceTablePrimaryIndex, NewSelectorPoint(&TokenBalance{ID: 4})),
			want: []*TokenBalance{
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorPoint_AccountAddressIndex_0xtestAccount",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})),
			want: []*TokenBalance{
				tokenBalanceAccount1,
				tokenBalance2Account1,
				tokenBalance3Account1,
			},
		},
		{
			name: "SelectorPoint_AccountAddressIndex_0xtestAccount_After_tokenBalance2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount"})).
				After(tokenBalance2Account1),
			want: []*TokenBalance{
				tokenBalance3Account1,
			},
		},
		{
			name: "SelectorPoints_AccountAddressIndex",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorPoints(
					&TokenBalance{AccountAddress: "0xtestAccount"},
					&TokenBalance{AccountAddress: "0xtestAccount2"},
				)),
			want: []*TokenBalance{
				tokenBalanceAccount1,
				tokenBalance2Account1,
				tokenBalance3Account1,
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorPoints_AccountAddressIndex_After_tokenBalance2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorPoints(
					&TokenBalance{AccountAddress: "0xtestAccount"},
					&TokenBalance{AccountAddress: "0xtestAccount2"},
				)).
				After(tokenBalance2Account1),
			want: []*TokenBalance{
				tokenBalance3Account1,
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorPoints_AccountAddressIndex_After_tokenBalance3",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorPoints(
					&TokenBalance{AccountAddress: "0xtestAccount"},
					&TokenBalance{AccountAddress: "0xtestAccount2"},
				)).
				After(tokenBalance3Account1),
			want: []*TokenBalance{
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorPoints_AccountAddressIndex_After_tokenBalance1Account2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorPoints(
					&TokenBalance{AccountAddress: "0xtestAccount"},
					&TokenBalance{AccountAddress: "0xtestAccount2"},
				)).
				After(tokenBalance1Account2),
			want: []*TokenBalance{},
		},
		{
			name: "SelectorRange_AccountAddressIndex_0xtestAccount",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRange(
					&TokenBalance{AccountAddress: "0xtestAccount", ID: 0},
					&TokenBalance{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
				)),
			want: []*TokenBalance{
				tokenBalanceAccount1,
				tokenBalance2Account1,
				tokenBalance3Account1,
			},
		},
		{
			name: "SelectorRange_AccountAddressIndex_0xtestAccount_After_tokenBalance2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRange(
					&TokenBalance{AccountAddress: "0xtestAccount", ID: 0},
					&TokenBalance{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
				)).
				After(tokenBalance2Account1),
			want: []*TokenBalance{
				tokenBalance3Account1,
			},
		},
		{
			name: "SelectorRange_AccountAddressIndex_0xtestAccount_After_tokenBalance2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRange(
					&TokenBalance{AccountAddress: "0xtestAccount", ID: 0},
					&TokenBalance{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
				)).
				After(tokenBalance3Account1),
			want: []*TokenBalance{},
		},
		{
			name: "SelectorRanges_AccountAddressIndex",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRanges(
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount", ID: 0},
						{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
					},
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount2", ID: 0},
						{AccountAddress: "0xtestAccount2", ID: math.MaxUint64},
					},
				)),
			want: []*TokenBalance{
				tokenBalanceAccount1,
				tokenBalance2Account1,
				tokenBalance3Account1,
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorRanges_AccountAddressIndex_After_tokenBalance2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRanges(
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount", ID: 0},
						{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
					},
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount2", ID: 0},
						{AccountAddress: "0xtestAccount2", ID: math.MaxUint64},
					},
				)).
				After(tokenBalance2Account1),
			want: []*TokenBalance{
				tokenBalance3Account1,
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorRanges_AccountAddressIndex_After_tokenBalance3",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRanges(
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount", ID: 0},
						{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
					},
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount2", ID: 0},
						{AccountAddress: "0xtestAccount2", ID: math.MaxUint64},
					},
				)).
				After(tokenBalance3Account1),
			want: []*TokenBalance{
				tokenBalance1Account2,
			},
		},
		{
			name: "SelectorRanges_AccountAddressIndex_After_tokenBalance1Account2",
			query: TokenBalanceTable.Query().
				With(TokenBalanceAccountAddressIndex, NewSelectorRanges(
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount", ID: 0},
						{AccountAddress: "0xtestAccount", ID: math.MaxUint64},
					},
					[]*TokenBalance{
						{AccountAddress: "0xtestAccount2", ID: 0},
						{AccountAddress: "0xtestAccount2", ID: math.MaxUint64},
					},
				)).
				After(tokenBalance1Account2),
			want: []*TokenBalance{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var tokenBalances []*TokenBalance
			err = tc.query.Execute(context.Background(), &tokenBalances)
			require.NoError(t, err)
			assert.Equal(t, tc.want, tokenBalances)
		})
	}
}
