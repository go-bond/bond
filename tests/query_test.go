package tests

import (
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func setupDatabaseForQuery() (*bond.DB, *bond.Table[*TokenBalance], *bond.Index[*TokenBalance], *bond.Index[*TokenBalance]) {
	db := setupDatabase()

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
		TokenBalanceAccountAndContractAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
		)
	)

	var TokenBalanceIndexes = []*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	tokenBalanceTable.AddIndexes(TokenBalanceIndexes, false)

	return db, tokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceAccountAndContractAddressIndex
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
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

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
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

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
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

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

	db := setupDatabase(&bond.MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

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

	db := setupDatabase(&bond.MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

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

	db := setupDatabase(&bond.MsgPackSerializer{})
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

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
