package tests

import (
	"encoding/binary"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_CreateTable(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	tokenBalanceTable := bond.CreateTable[*TokenBalance](db, func(tb *TokenBalance) []byte {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, tb.ID)

		return key
	})
	require.NotNil(t, tokenBalanceTable)
	assert.Equal(t, bond.TableID(1), tokenBalanceTable.TableID)
}

func TestBond_CreateTableWithTableID(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	tokenBalanceTableID := bond.TableID(0xC0)
	tokenBalanceTable := bond.CreateTableWithTableID[*TokenBalance](
		db,
		tokenBalanceTableID,
		func(tb *TokenBalance) []byte {
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, tb.ID)

			return key
		},
	)
	require.NotNil(t, tokenBalanceTable)
	assert.Equal(t, tokenBalanceTableID, tokenBalanceTable.TableID)
}
