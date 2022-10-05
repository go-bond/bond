package bond

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspect_Tables(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]TableInfo{table})
	require.NoError(t, err)

	expectedTables := []string{
		"token_balance",
	}

	assert.Equal(t, expectedTables, insp.Tables())
}

func TestInspect_Indexes(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]TableInfo{table})
	require.NoError(t, err)

	expectedIndexes := []string{
		"primary",
		"account_address_idx",
		"account_and_contract_address_idx",
	}

	require.Equal(t, expectedIndexes, insp.Indexes("token_balance"))
}

func TestInspect_Query(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insertTokenBalance := []*TokenBalance{
		{
			ID:              1,
			AccountID:       1,
			ContractAddress: "0xc",
			AccountAddress:  "0xa",
			TokenID:         10,
			Balance:         501,
		},
	}

	expectedTokenBalance := []map[string]interface{}{
		{
			"ID":              uint64(1),
			"AccountID":       uint32(1),
			"ContractAddress": "0xc",
			"AccountAddress":  "0xa",
			"TokenID":         uint32(10),
			"Balance":         uint64(501),
		},
	}

	err := table.Insert(context.Background(), insertTokenBalance)
	require.NoError(t, err)

	insp, err := NewInspect([]TableInfo{table})
	require.NoError(t, err)

	tables := insp.Tables()
	require.Equal(t, 1, len(tables))

	resp, err := insp.Query(tables[0], PrimaryIndexName, nil, nil, 10, nil)
	require.NoError(t, err)
	assert.Equal(t, resp, expectedTokenBalance)
}
