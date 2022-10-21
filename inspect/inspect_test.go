package inspect

import (
	"context"
	"os"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TokenBalance struct {
	ID              uint64 `json:"id"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

const dbName = "test_db"

func setupDatabase(serializer ...bond.Serializer[any]) *bond.DB {
	options := &bond.Options{}
	if len(serializer) > 0 && serializer[0] != nil {
		options.Serializer = serializer[0]
	}

	db, _ := bond.Open(dbName, options)
	return db
}

func tearDownDatabase(db *bond.DB) {
	_ = db.Close()
	_ = os.RemoveAll(dbName)
}

func setupDatabaseForQuery() (*bond.DB, *bond.Table[*TokenBalance], *bond.Index[*TokenBalance], *bond.Index[*TokenBalance]) {
	db := setupDatabase()

	const (
		TokenBalanceTableID = bond.TableID(1)
	)

	tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	const (
		_                                 = bond.PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
		})
		TokenBalanceAccountAndContractAddressIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAndContractAddressIndexID,
			IndexName: "account_and_contract_address_idx",
			IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
		})
	)

	var TokenBalanceIndexes = []*bond.Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	err := tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)
	if err != nil {
		panic(err)
	}

	return db, tokenBalanceTable, TokenBalanceAccountAddressIndex, TokenBalanceAccountAndContractAddressIndex
}

func TestBond_Open(t *testing.T) {
	db, err := bond.Open(dbName, &bond.Options{})
	defer func() { _ = os.RemoveAll(dbName) }()

	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}

func TestInspect_Tables(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]bond.TableInfo{table})
	require.NoError(t, err)

	expectedTables := []string{
		"token_balance",
	}

	tables, err := insp.Tables()
	require.NoError(t, err)
	assert.Equal(t, expectedTables, tables)
}

func TestInspect_Indexes(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]bond.TableInfo{table})
	require.NoError(t, err)

	expectedIndexes := []string{
		"primary",
		"account_address_idx",
		"account_and_contract_address_idx",
	}

	indexes, err := insp.Indexes("token_balance")
	require.NoError(t, err)
	require.Equal(t, expectedIndexes, indexes)
}

func TestInspect_EntryFields(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]bond.TableInfo{table})
	require.NoError(t, err)

	expectedFields := map[string]string{
		"ID":              "uint64",
		"AccountID":       "uint32",
		"AccountAddress":  "string",
		"ContractAddress": "string",
		"TokenID":         "uint32",
		"Balance":         "uint64",
	}

	fields, err := insp.EntryFields("token_balance")
	require.NoError(t, err)
	require.Equal(t, expectedFields, fields)
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

		insp, err := NewInspect([]bond.TableInfo{table})
		require.NoError(t, err)

		tables, err := insp.Tables()
		require.NoError(t, err)
		require.Equal(t, 1, len(tables))

		resp, err := insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, nil, 0, nil)
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

		insp, err := NewInspect([]bond.TableInfo{table})
		require.NoError(t, err)

		tables, err := insp.Tables()
		require.NoError(t, err)
		require.Equal(t, 1, len(tables))

		resp, err := insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, nil, 1, nil)
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

		insp, err := NewInspect([]bond.TableInfo{table})
		require.NoError(t, err)

		tables, err := insp.Tables()
		require.NoError(t, err)
		require.Equal(t, 1, len(tables))

		filter := map[string]interface{}{
			"ID": uint64(1),
		}

		resp, err := insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": uint32(1),
		}

		resp, err = insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": uint16(1),
		}

		resp, err = insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": 1,
		}

		resp, err = insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, filter, 0, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		filter = map[string]interface{}{
			"ID": 1.0,
		}

		resp, err = insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, filter, 0, nil)
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

		insp, err := NewInspect([]bond.TableInfo{table})
		require.NoError(t, err)

		tables, err := insp.Tables()
		require.NoError(t, err)
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

		insp, err := NewInspect([]bond.TableInfo{table})
		require.NoError(t, err)

		tables, err := insp.Tables()
		require.NoError(t, err)
		require.Equal(t, 1, len(tables))

		resp, err := insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, nil, 1, nil)
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance)

		resp, err = insp.Query(context.Background(), tables[0], bond.PrimaryIndexName, nil, nil, 1, resp[0])
		require.NoError(t, err)
		assert.Equal(t, resp, expectedTokenBalance2)
	})
}
