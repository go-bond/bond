package tests

import (
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_NewIndex(t *testing.T) {
	const (
		TokenBalanceAccountIDIndexID = bond.IndexID(1)
	)

	TokenBalanceAccountIDIndex := bond.NewIndex[*TokenBalance](
		TokenBalanceAccountIDIndexID,
		func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
	)

	assert.Equal(t, TokenBalanceAccountIDIndexID, TokenBalanceAccountIDIndex.IndexID)
	assert.Equal(t, true, TokenBalanceAccountIDIndex.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, true, TokenBalanceAccountIDIndex.IndexFilterFunction(&TokenBalance{AccountID: 2}))

	TokenBalanceAccountIndexSelective := bond.NewIndex[*TokenBalance](
		TokenBalanceAccountIDIndexID,
		func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
		func(tb *TokenBalance) bool {
			return tb.AccountID == 1
		},
	)

	assert.Equal(t, TokenBalanceAccountIDIndexID, TokenBalanceAccountIndexSelective.IndexID)
	assert.Equal(t, true, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, false, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 2}))
}

func TestBond_Table_Index(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = bond.PrimaryIndexID
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
			func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		)
	)

	var TokenBalanceIndexes = []*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
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

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it := db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}
