package tests

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_NewIndex(t *testing.T) {
	const (
		TokenBalanceAccountIndexID = bond.IndexID(1)
	)

	TokenBalanceAccountIndex := bond.NewIndex[*TokenBalance](
		TokenBalanceAccountIndexID,
		func(tb *TokenBalance) []byte {
			keyBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(keyBytes, tb.AccountID)
			return keyBytes
		},
	)

	expectedIndexKey := []uint8{0x1, 0x00, 0x00, 0x00, 0x01}

	assert.Equal(t, TokenBalanceAccountIndexID, TokenBalanceAccountIndex.IndexID)
	assert.Equal(t, expectedIndexKey, TokenBalanceAccountIndex.IndexKey(&TokenBalance{AccountID: 1}))
	assert.Equal(t, true, TokenBalanceAccountIndex.IndexFilterFunction(&TokenBalance{AccountID: 1}))

	TokenBalanceAccountIndexSelective := bond.NewIndex[*TokenBalance](
		TokenBalanceAccountIndexID,
		func(tb *TokenBalance) []byte {
			keyBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(keyBytes, tb.AccountID)
			return keyBytes
		},
		func(tb *TokenBalance) bool {
			return tb.AccountID == 1
		},
	)

	assert.Equal(t, TokenBalanceAccountIndexID, TokenBalanceAccountIndexSelective.IndexID)
	assert.Equal(t, expectedIndexKey, TokenBalanceAccountIndexSelective.IndexKey(&TokenBalance{AccountID: 1}))
	assert.Equal(t, true, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, false, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 2}))
}

func TestBond_Table_Index(t *testing.T) {
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
		TokenBalanceDefaultIndexID = bond.DefaultMainIndexID
		TokenBalanceAccountIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountIndexID,
			func(tb *TokenBalance) []byte {
				return append([]byte{}, tb.AccountAddress...)
			},
		)
		TokenBalanceAccountAndContractAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(tb *TokenBalance) []byte {
				return append(append([]byte{}, tb.AccountAddress...), tb.ContractAddress...)
			},
			func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		)
	)

	var TokenBalanceIndexes = []*bond.Index[*TokenBalance]{
		TokenBalanceAccountIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	tokenBalanceTable.AddIndexes(TokenBalanceIndexes, false)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(tokenBalanceAccount1)
	require.Nil(t, err)

	err = tokenBalanceTable.Insert(tokenBalance2Account1)
	require.Nil(t, err)

	err = tokenBalanceTable.Insert(tokenBalance1Account2)
	require.Nil(t, err)

	it := db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x: %s\n", it.Key(), it.Value())
	}
}
