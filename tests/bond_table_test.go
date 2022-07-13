package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, tb.ID)
			return key
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
		buffer := bytes.NewBuffer([]byte{})
		_, _ = fmt.Fprintf(buffer, "%d", tb.ID)
		return buffer.Bytes()
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

	err := tokenBalanceTable.Insert(tokenBalanceAccount1)
	require.NoError(t, err)

	err = tokenBalanceTable.Insert(tokenBalance2Account1)
	require.NoError(t, err)

	err = tokenBalanceTable.Insert(tokenBalance1Account2)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = tokenBalanceTable.Scan(&tokenBalances)
	require.NoError(t, err)
	require.Equal(t, len(tokenBalances), 3)

	require.Equal(t, tokenBalanceAccount1, tokenBalances[0])
	require.Equal(t, tokenBalance2Account1, tokenBalances[1])
	require.Equal(t, tokenBalance1Account2, tokenBalances[2])
}
