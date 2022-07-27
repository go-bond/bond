package bond

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func setupDatabaseForQuery() (*DB, *Table[*TokenBalance], *Index[*TokenBalance], *Index[*TokenBalance]) {
	db := setupDatabase()

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	if err != nil {
		panic(err)
	}

	return db, tokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceAccountAndContractAddressIndex
}

func TestBond_Query_OnOrderedIndex(t *testing.T) {
	db, TokenBalanceTable, _, lastIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	TokenBalanceOrderedIndex := NewIndex[*TokenBalance](
		lastIndex.IndexID+1,
		func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	)
	TokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceOrderedIndex})

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
		With(TokenBalanceOrderedIndex, &TokenBalance{AccountAddress: "0xtestAccount"})

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[2])
}

func TestBond_Query_Where(t *testing.T) {
	db, TokenBalanceTable, _, _ := setupDatabaseForQuery()
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Filter(func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount"
		}).
		Limit(50)

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[2])
}

func TestBond_Query_Where_Offset_Limit(t *testing.T) {
	db, TokenBalanceTable, _, _ := setupDatabaseForQuery()
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Offset(1).
		Limit(2)

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])
	assert.Equal(t, tokenBalance3Account1, tokenBalances[1])

	query = TokenBalanceTable.Query().
		Offset(3).
		Limit(2)

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance1Account2, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Offset(4).
		Limit(2)

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Where_Offset_Limit_With_Filter(t *testing.T) {
	db, TokenBalanceTable, _, _ := setupDatabaseForQuery()
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Where_Offset_Limit_With_Order(t *testing.T) {
	db, TokenBalanceTable, _, _ := setupDatabaseForQuery()
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance1Account2, tokenBalances[0])

	query = TokenBalanceTable.Query().
		Order(func(tr *TokenBalance, tr2 *TokenBalance) bool {
			return tr.Balance > tr2.Balance
		}).
		Offset(4).
		Limit(2)

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 0, len(tokenBalances))
}

func TestBond_Query_Order(t *testing.T) {
	db, TokenBalanceTable, _, _ := setupDatabaseForQuery()
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 3, len(tokenBalances))

	assert.Equal(t, tokenBalance3Account1, tokenBalances[0])
	assert.Equal(t, tokenBalanceAccount1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])
}

func TestBond_Query_Indexes_Mix(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceAccountAndContractAddressIndex := setupDatabaseForQuery()
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)
	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalance2Account1, tokenBalances[0])

	query = TokenBalanceTable.Query().
		With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0xtestAccount"}).
		Filter(func(tb *TokenBalance) bool {
			return tb.Balance < 10
		}).
		Limit(50)

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
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

	err = query.Execute(&tokenBalances)
	require.Nil(t, err)

	require.Equal(t, 1, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
}

func BenchmarkBondTableQuery_1000000_Account_200_Order_Balance(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_500_Order_Balance(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%2000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_1000_Order_Balance(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Limit(1000).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_200_Order_Balance(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_500_Order_Balance(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%2000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Order_Balance(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Limit(1000).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_1000_Limit_200(b *testing.B) {
	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_1000_Limit_500(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_1000_Offset_200_Limit_500(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(200).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_1000_Offset_500_Limit_500(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(500).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_1000000_Account_1000_Offset_500_Limit_200(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(500).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Limit_200(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Limit_500(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_200_Limit_500(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(200).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_500(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(500).
			Limit(500).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(500).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_OrderedIndex_Account_1000_Offset_500_Limit_200(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(500).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200_OrderByBalance(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderDefault[*TokenBalance],
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
				return tb.Balance > tb2.Balance
			}).
			Offset(500).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200_OrderedIndexByBalance(b *testing.B) {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	db := setupDatabase(&MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			func(o IndexOrder, tb *TokenBalance) IndexOrder {
				return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
			},
		)
	)

	err := tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})
	if err != nil {
		panic(err)
	}

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
			AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%1000),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance

		err := tokenBalanceTable.Query().
			With(
				TokenBalanceAccountAddressIndex,
				&TokenBalance{AccountAddress: "0xtestAccount0"},
			).
			Offset(500).
			Limit(200).
			Execute(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}
