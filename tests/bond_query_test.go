package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

func setupDatabaseForQuery() (*bond.DB, *bond.Table[*TokenBalance], *bond.Index[*TokenBalance], *bond.Index[*TokenBalance]) {
	db := setupDatabase()

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(tb *TokenBalance) []byte {
		keyBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(keyBytes, tb.AccountID)
		return keyBytes
	})

	const (
		TokenBalanceDefaultIndexID = bond.DefaultMainIndexID
		TokenBalanceAccountIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
			TokenBalanceAccountIndexID,
			func(tb *TokenBalance) []byte {
				buffer := bytes.NewBuffer([]byte{})
				_, _ = fmt.Fprintf(buffer, "%s", tb.AccountAddress)
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
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	tokenBalanceTable.AddIndexes(TokenBalanceIndexes, false)

	return db, tokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceAccountAndContractAddressIndex
}

func TestBond_Query(t *testing.T) {
	db, TokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceAccountAndContractAddressIndex := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	var tokenBalances []*TokenBalance

	query := TokenBalanceTable.Query().
		Where(&bond.Gt[*TokenBalance, uint64]{
			Record: func(c *TokenBalance) uint64 {
				return c.Balance
			},
			Greater: 10,
		}).
		Limit(50)

	err := query.Execute(tokenBalances)
	require.Nil(t, err)

	query = TokenBalanceTable.Query().
		With(TokenBalanceAccountAddressIndex, &TokenBalance{AccountAddress: "0x0c"}).
		Where(&bond.Gt[*TokenBalance, uint64]{
			Record: func(c *TokenBalance) uint64 {
				return c.Balance
			},
			Greater: 10,
		}).
		Limit(50)

	err = query.Execute(tokenBalances)
	require.Nil(t, err)

	query = TokenBalanceTable.Query().
		With(
			TokenBalanceAccountAndContractAddressIndex,
			&TokenBalance{AccountAddress: "0x0c", ContractAddress: "0xtestContract"},
		).
		Where(&bond.Gt[*TokenBalance, uint64]{
			Record: func(c *TokenBalance) uint64 {
				return c.Balance
			},
			Greater: 10,
		}).
		Order(func(tb *TokenBalance, tb2 *TokenBalance) bool {
			return tb.Balance > tb.Balance
		}).
		Limit(50)

	err = query.Execute(tokenBalances)
	require.Nil(t, err)
}
