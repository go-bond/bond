package tests

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

func TestBond_Table_Index(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		buffer := bytes.NewBuffer([]byte{})
		_, _ = fmt.Fprintf(buffer, "%d", tb.ID)
		return buffer.Bytes()
	})

	const (
		TokenBalanceDefaultIndexID                   = bond.DefaultMainIndexID
		TokenBalanceAccountIndexID                   = iota
		TokenBalanceAccountAndContractAddressIndexID = iota
	)

	var (
		TokenBalanceAccountIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountIndexID,
			func(tb *TokenBalance) []byte {
				buffer := bytes.NewBuffer([]byte{})
				_, _ = fmt.Fprintf(buffer, "%d", tb.AccountID)
				return buffer.Bytes()
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

	err := tokenBalanceTable.Insert(tokenBalanceAccount1)
	require.Nil(t, err)

	err = tokenBalanceTable.Insert(tokenBalance2Account1)
	require.Nil(t, err)

	err = tokenBalanceTable.Insert(tokenBalance1Account2)
	require.Nil(t, err)

	it := db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("%s: %s\n", it.Key(), it.Value())
	}

	fmt.Printf("----------------- GetAll Contents ----------------- \n")

	tkns, err := tokenBalanceTable.GetAll()
	require.Nil(t, err)

	spew.Dump(tkns)

	fmt.Printf("----------------- GetAllForIndex Account=1 Contents ----------------- \n")
	tkns, err = tokenBalanceTable.GetAllForIndex(bond.DefaultMainIndexID+1, &TokenBalance{AccountID: 1})
	require.Nil(t, err)

	spew.Dump(tkns)

	fmt.Printf("----------------- GetAllForIndex Account=2 Contents ----------------- \n")
	tkns, err = tokenBalanceTable.GetAllForIndex(bond.DefaultMainIndexID+1, &TokenBalance{AccountID: 2})
	require.Nil(t, err)

	spew.Dump(tkns)
}
