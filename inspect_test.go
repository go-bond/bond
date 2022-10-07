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

func TestInspect_EntryFields(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]TableInfo{table})
	require.NoError(t, err)

	expectedIndexes := map[string]string{
		"ID":              "uint64",
		"AccountID":       "uint32",
		"AccountAddress":  "string",
		"ContractAddress": "string",
		"TokenID":         "uint32",
		"Balance":         "uint64",
	}

	require.Equal(t, expectedIndexes, insp.EntryFields("token_balance"))
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
		{
			ID:              2,
			AccountID:       1,
			ContractAddress: "0xc",
			AccountAddress:  "0xa",
			TokenID:         5,
			Balance:         1,
		},
	}

	err := table.Insert(context.Background(), insertTokenBalance)
	require.NoError(t, err)

	t.Run("Simple", func(t *testing.T) {
		expectedTokenBalance := []map[string]interface{}{
			{
				"ID":              uint64(1),
				"AccountID":       uint32(1),
				"ContractAddress": "0xc",
				"AccountAddress":  "0xa",
				"TokenID":         uint32(10),
				"Balance":         uint64(501),
			},
			{
				"ID":              uint64(2),
				"AccountID":       uint32(1),
				"ContractAddress": "0xc",
				"AccountAddress":  "0xa",
				"TokenID":         uint32(5),
				"Balance":         uint64(1),
			},
		}

		insp, err := NewInspect([]TableInfo{table})
		require.NoError(t, err)

		tables := insp.Tables()
		require.Equal(t, 1, len(tables))

		resp, err := insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, nil, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)
	})

	t.Run("SimpleWithLimit", func(t *testing.T) {
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

		insp, err := NewInspect([]TableInfo{table})
		require.NoError(t, err)

		tables := insp.Tables()
		require.Equal(t, 1, len(tables))

		resp, err := insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, nil, 1, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)
	})

	t.Run("SimpleWithFilter", func(t *testing.T) {
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

		insp, err := NewInspect([]TableInfo{table})
		require.NoError(t, err)

		tables := insp.Tables()
		require.Equal(t, 1, len(tables))

		filter := map[string]interface{}{
			"ID": uint64(1),
		}

		resp, err := insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": uint32(1),
		}

		resp, err = insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": uint16(1),
		}

		resp, err = insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": 1,
		}

		resp, err = insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": 1.0,
		}

		resp, err = insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)
	})

	t.Run("SimpleWithSecondaryIndex", func(t *testing.T) {
		expectedTokenBalance := []map[string]interface{}{
			{
				"ID":              uint64(1),
				"AccountID":       uint32(1),
				"ContractAddress": "0xc",
				"AccountAddress":  "0xa",
				"TokenID":         uint32(10),
				"Balance":         uint64(501),
			},
			{
				"ID":              uint64(2),
				"AccountID":       uint32(1),
				"ContractAddress": "0xc",
				"AccountAddress":  "0xa",
				"TokenID":         uint32(5),
				"Balance":         uint64(1),
			},
		}

		insp, err := NewInspect([]TableInfo{table})
		require.NoError(t, err)

		tables := insp.Tables()
		require.Equal(t, 1, len(tables))

		selector := map[string]interface{}{
			"AccountAddress": "0xa",
		}

		resp, err := insp.Query(context.Background(), tables[0], "account_address_idx", selector, nil, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		selector = map[string]interface{}{
			"AccountAddress": "0xb",
		}

		resp, err = insp.Query(context.Background(), tables[0], "account_address_idx", selector, nil, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, []map[string]interface{}{})

		selector = map[string]interface{}{
			"ID":             5,
			"AccountAddress": "0xb",
		}

		resp, err = insp.Query(context.Background(), tables[0], "account_address_idx", selector, nil, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, []map[string]interface{}{})

		selector = map[string]interface{}{
			"ID":             "5",
			"AccountAddress": "0xb",
		}

		resp, err = insp.Query(context.Background(), tables[0], "account_address_idx", selector, nil, 0, nil)
		require.Error(t, err)

		selector = map[string]interface{}{
			"IDd":            5,
			"AccountAddress": "0xb",
		}

		resp, err = insp.Query(context.Background(), tables[0], "account_address_idx", selector, nil, 0, nil)
		require.Error(t, err)
	})

	t.Run("SimpleWithAfter", func(t *testing.T) {
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

		expectedTokenBalance2 := []map[string]interface{}{
			{
				"ID":              uint64(2),
				"AccountID":       uint32(1),
				"ContractAddress": "0xc",
				"AccountAddress":  "0xa",
				"TokenID":         uint32(5),
				"Balance":         uint64(1),
			},
		}

		insp, err := NewInspect([]TableInfo{table})
		require.NoError(t, err)

		tables := insp.Tables()
		require.Equal(t, 1, len(tables))

		resp, err := insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, nil, 1, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		resp, err = insp.Query(context.Background(), tables[0], PrimaryIndexName, nil, nil, 1, resp[0])
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance2)
	})
}
