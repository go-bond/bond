package playground

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

type TokenBalance struct {
	ID              uint64 `json:"uint64"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

func Test_Table(t *testing.T) {
	db, _ := pebble.Open("demo2", &pebble.Options{})
	defer func() { _ = os.RemoveAll("demo2") }()

	tokenBalanceTableID := TableID(1)

	tokenBalanceTable := NewTable[TokenBalance](db, tokenBalanceTableID, func(tb TokenBalance) []byte {
		buffer := bytes.NewBuffer([]byte{})
		_, _ = fmt.Fprintf(buffer, "%d", tb.ID)
		return buffer.Bytes()
	})

	tokenBalanceAccountIndexID := DefaultMainIndexID + 1
	tokenBalanceAccountIdx := NewIndex[TokenBalance](
		tokenBalanceAccountIndexID,
		func(tb TokenBalance) []byte {
			buffer := bytes.NewBuffer([]byte{})
			_, _ = fmt.Fprintf(buffer, "%d", tb.AccountID)
			return buffer.Bytes()
		},
	)

	tokenBalanceAccountAndContractAddressIndexID := DefaultMainIndexID + 2
	tokenBalanceAccountAndContractAddressIdx := NewIndex[TokenBalance](
		tokenBalanceAccountAndContractAddressIndexID,
		func(tb TokenBalance) []byte {
			return append(append([]byte{}, tb.AccountAddress...), tb.ContractAddress...)
		},
		func(tb TokenBalance) bool {
			return tb.ContractAddress == "0xtestContract"
		},
	)

	tokenBalanceTable.AddIndex(tokenBalanceAccountIdx)
	tokenBalanceTable.AddIndex(tokenBalanceAccountAndContractAddressIdx)

	tokenBalanceAccount1 := TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount",
		Balance:         5,
	}

	tokenBalance2Account1 := TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount",
		Balance:         15,
	}

	tokenBalance1Account2 := TokenBalance{
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

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("%s: %s\n", it.Key(), it.Value())
	}
}
