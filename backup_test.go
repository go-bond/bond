package bond

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBond_BackupRestore(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)
	defer os.RemoveAll("export")

	const (
		TokenBalanceTableID TableID = 0xC0
		TokenTableID        TableID = 0xC1
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	tokenTable := NewTable[*Token](TableOptions[*Token]{
		DB:        db,
		TableID:   TokenTableID,
		TableName: "token",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *Token) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = IndexID(iota)
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	_ = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	for i := 0; i < 10; i++ {
		err := tokenBalanceTable.Insert(context.TODO(), []*TokenBalance{{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: fmt.Sprintf("contractaddr_%d", i),
			AccountAddress:  fmt.Sprintf("accountaddr_%d", i),
			Balance:         uint64(i),
		}})
		require.NoError(t, err)
		err = tokenTable.Insert(context.TODO(), []*Token{
			{
				ID:   uint64(i),
				Name: fmt.Sprintf("%d", i),
			},
		})
		require.NoError(t, err)
	}
	err := db.Dump(context.TODO(), "./export", []TableID{TokenBalanceTableID, TokenTableID}, true)
	require.NoError(t, err)

	// create a tmp db.
	db2 := setupDB("tmp_db")
	defer tearDownDB("tmp_db", db2)
	table := tokenBalanceTable.(*_table[*TokenBalance])
	table.db = db2
	table2 := tokenTable.(*_table[*Token])
	table2.db = db2
	err = db2.Restore(context.TODO(), "export", []TableID{TokenBalanceTableID, TokenTableID}, true)
	require.NoError(t, err)

	err = db2.Restore(context.TODO(), "export", []TableID{TokenTableID + 1}, true)
	require.Error(t, err)

	// make sure both db has same keys and values.
	itr := db.Iter(&IterOptions{})
	itr2 := db2.Iter(&IterOptions{})
	defer itr.Close()
	defer itr2.Close()

	for _, _ = itr.First(), itr2.First(); itr.Valid(); _, _ = itr.Next(), itr2.Next() {
		require.Equal(t, itr.Key(), itr2.Key())
		require.Equal(t, itr.Value(), itr2.Value())
	}
	require.False(t, itr2.Valid())
}

func TestBond_RestoreDifferentVersion(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)
	defer os.RemoveAll("export")

	const (
		TokenBalanceTableID TableID = 0xC0
		TokenTableID        TableID = 0xC1
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	tokenTable := NewTable[*Token](TableOptions[*Token]{
		DB:        db,
		TableID:   TokenTableID,
		TableName: "token",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *Token) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = IndexID(iota)
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	_ = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	for i := 0; i < 10; i++ {
		err := tokenBalanceTable.Insert(context.TODO(), []*TokenBalance{{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: fmt.Sprintf("contractaddr_%d", i),
			AccountAddress:  fmt.Sprintf("accountaddr_%d", i),
			Balance:         uint64(i),
		}})
		require.NoError(t, err)
		err = tokenTable.Insert(context.TODO(), []*Token{
			{
				ID:   uint64(i),
				Name: fmt.Sprintf("%d", i),
			},
		})
		require.NoError(t, err)
	}
	err := db.Dump(context.TODO(), "./export", []TableID{TokenBalanceTableID, TokenTableID}, true)
	require.NoError(t, err)

	// rewrite the version for bond to use batchedinsert strategy.
	err = os.Remove("./export/VERSION")
	require.NoError(t, err)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], BOND_DB_DATA_VERSION-1)
	err = os.WriteFile("./export/VERSION", buf[:], 0755)
	require.NoError(t, err)

	// create a tmp db.
	db2 := setupDB("tmp_db")
	defer tearDownDB("tmp_db", db2)
	err = db2.Restore(context.TODO(), "export", []TableID{}, true)
	require.Error(t, err)
}

func Test_BondIDRetrival(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)
	defer os.RemoveAll("export")

	const (
		TokenBalanceTableID TableID = 0xC0
		TokenTableID        TableID = 0xC1
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalanceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	tokenTable := NewTable[*Token](TableOptions[*Token]{
		DB:        db,
		TableID:   TokenTableID,
		TableName: "token",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *Token) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})
	require.NotNil(t, tokenBalanceTable)

	const (
		_                                 = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = IndexID(iota)
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   TokenBalanceAccountAddressIndexID,
			IndexName: "account_address_idx",
			IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
			IndexOrderFunc: IndexOrderDefault[*TokenBalance],
		})
	)

	_ = tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
	})

	for i := 0; i < 10; i++ {
		err := tokenBalanceTable.Insert(context.TODO(), []*TokenBalance{{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: fmt.Sprintf("contractaddr_%d", i),
			AccountAddress:  fmt.Sprintf("accountaddr_%d", i),
			Balance:         uint64(i),
		}})
		require.NoError(t, err)
		err = tokenTable.Insert(context.TODO(), []*Token{
			{
				ID:   uint64(i),
				Name: fmt.Sprintf("%d", i),
			},
		})
		require.NoError(t, err)
	}
	db_ := db.(*_db)
	indexIDs := db_.getIndexIDS(TokenBalanceTableID)
	assert.ElementsMatch(t, indexIDs, []IndexID{TokenBalanceAccountAddressIndexID})
}
