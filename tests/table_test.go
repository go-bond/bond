package tests

import (
	"encoding/binary"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_NewTable(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID bond.TableID = 0xC0
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](
		db,
		TokenBalanceTableID,
		func(tb *TokenBalance) []byte {
			keyBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(keyBytes, tb.ID)
			return keyBytes
		},
	)
	require.NotNil(t, tokenBalanceTable)
	assert.Equal(t, TokenBalanceTableID, tokenBalanceTable.TableID)
}

func TestBondTable_Scan(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, tb.ID)
		return keyBytes
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
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.Scan(&tokenBalances)
	require.NoError(t, err)
	require.Equal(t, len(tokenBalances), 3)

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
	assert.Equal(t, tokenBalance1Account2, tokenBalances[2])
}

func TestBondTable_ScanIndex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, tb.ID)
		return keyBytes
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(tb *TokenBalance) []byte {
				return append([]byte{}, tb.AccountAddress...)
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
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
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.ScanIndex(TokenBalanceAccountAddressIndex,
		&TokenBalance{AccountAddress: "0xtestAccount"}, &tokenBalances)
	require.NoError(t, err)
	require.Equal(t, 2, len(tokenBalances))

	assert.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	assert.Equal(t, tokenBalance2Account1, tokenBalances[1])
}

func BenchmarkBondTableScan_1000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, tb.ID)
		return keyBytes
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.Scan(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScan_1000000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, tb.ID)
		return keyBytes
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount" + string([]byte{byte(i % 10)}),
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.Scan(&tokenBalances)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_1000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, tb.ID)
		return keyBytes
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(tb *TokenBalance) []byte {
				return append([]byte{}, tb.AccountAddress...)
			},
		)
	)

	tokenBalanceTable.AddIndexes([]*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	var tokenBalancesForInsert []*TokenBalance
	for i := 0; i < 1000; i++ {
		tokenBalancesForInsert = append(tokenBalancesForInsert, &TokenBalance{
			ID:              uint64(i + 1),
			AccountID:       uint32(i % 10),
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.ScanIndex(
			TokenBalanceAccountAddressIndex,
			&TokenBalance{
				AccountAddress: "0xtestAccount",
			},
			&tokenBalances,
		)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

func BenchmarkBondTableScanIndex_1000000(b *testing.B) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(keyBytes, tb.ID)
		return keyBytes
	})

	const (
		TokenBalanceMainIndexID           = bond.MainIndexID
		TokenBalanceAccountAddressIndexID = iota
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(tb *TokenBalance) []byte {
				return append([]byte{}, tb.AccountAddress...)
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
			ContractAddress: "0xtestContract" + string([]byte{byte(i % 3)}),
			AccountAddress:  "0xtestAccount",
			Balance:         uint64((i % 100) * 10),
		})
	}
	_ = tokenBalanceTable.Insert(tokenBalancesForInsert)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var tokenBalances []*TokenBalance
		err := tokenBalanceTable.ScanIndex(
			TokenBalanceAccountAddressIndex,
			&TokenBalance{
				AccountAddress: "0xtestAccount",
			},
			&tokenBalances,
		)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}
