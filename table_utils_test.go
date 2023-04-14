package bond

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableAnyScanner(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID TableID = 0xC0
	)

	// create a table for TokenBalance
	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	// expected TokenBalances
	tokenBalances := []*TokenBalance{
		{
			ID:              1,
			AccountID:       1,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount",
			Balance:         5,
		},
		{
			ID:              2,
			AccountID:       2,
			ContractAddress: "0xtestContract",
			AccountAddress:  "0xtestAccount2",
			Balance:         10,
		},
	}

	// insert expected TokenBalances
	err := tokenBalanceTable.Insert(context.Background(), tokenBalances)
	require.NoError(t, err)

	// create a any scanner for the table
	anyScanner := TableAnyScanner[*TokenBalance](tokenBalanceTable)

	// scan all TokenBalances
	var scannedAnyTokenBalances []any
	err = anyScanner.Scan(context.Background(), &scannedAnyTokenBalances, false)
	require.NoError(t, err)
	assert.Equal(t, len(tokenBalances), len(scannedAnyTokenBalances))

	// assert that the scanned TokenBalances are the same as the expected TokenBalances
	for i, scannedTokenBalance := range scannedAnyTokenBalances {
		assert.Equal(t, tokenBalances[i], scannedTokenBalance.(*TokenBalance))
	}
}
