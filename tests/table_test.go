package tests

import (
	"encoding/binary"
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
