package bond

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestStructure struct {
	ID          int
	Name        string
	Description string
}

func TestBond_NewIndex(t *testing.T) {
	const (
		TokenBalanceAccountIDIndexID = IndexID(1)
	)

	TokenBalanceAccountIDIndex := NewIndex[*TokenBalance](
		TokenBalanceAccountIDIndexID,
		func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
	)

	assert.Equal(t, TokenBalanceAccountIDIndexID, TokenBalanceAccountIDIndex.IndexID)
	assert.Equal(t, true, TokenBalanceAccountIDIndex.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, true, TokenBalanceAccountIDIndex.IndexFilterFunction(&TokenBalance{AccountID: 2}))

	TokenBalanceAccountIndexSelective := NewIndex[*TokenBalance](
		TokenBalanceAccountIDIndexID,
		func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
		func(tb *TokenBalance) bool {
			return tb.AccountID == 1
		},
	)

	assert.Equal(t, TokenBalanceAccountIDIndexID, TokenBalanceAccountIndexSelective.IndexID)
	assert.Equal(t, true, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 1}))
	assert.Equal(t, false, TokenBalanceAccountIndexSelective.IndexFilterFunction(&TokenBalance{AccountID: 2}))
}

func TestBond_Table_Index_Insert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		)
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it := db.NewIter(nil)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount2") && strings.Contains(string(keys[6]), "0xtestContract"))

	it = db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Index_Update(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		)
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	tokenBalanceAccount1.AccountAddress = "0xtestAccount3"
	tokenBalance2Account1.AccountAddress = "0xtestAccount3"
	tokenBalance1Account2.AccountAddress = "0xtestAccount3"

	err = tokenBalanceTable.Update(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	it := db.NewIter(nil)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount3"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount3") && strings.Contains(string(keys[6]), "0xtestContract"))

	it = db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Index_Delete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		)
	)

	var TokenBalanceIndexes = []*Index[*TokenBalance]{
		TokenBalanceAccountAddressIndex,
		TokenBalanceAccountAndContractAddressIndex,
	}

	tokenBalanceTable.AddIndex(TokenBalanceIndexes, false)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	err = tokenBalanceTable.Delete(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)
	assert.False(t, db.NewIter(nil).First())

	it := db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}
}

func TestBond_Table_Reindex(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalanceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder KeyBuilder, tb *TokenBalance) []byte {
		return builder.AddUint64Field(tb.ID).Bytes()
	})

	const (
		TokenBalanceDefaultIndexID        = PrimaryIndexID
		TokenBalanceAccountAddressIndexID = iota
		TokenBalanceAccountAndContractAddressIndexID
	)

	var (
		TokenBalanceAccountAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddStringField(tb.AccountAddress).Bytes()
			},
		)
		TokenBalanceAccountAndContractAddressIndex = NewIndex[*TokenBalance](
			TokenBalanceAccountAndContractAddressIndexID,
			func(builder KeyBuilder, tb *TokenBalance) []byte {
				return builder.
					AddStringField(tb.AccountAddress).
					AddStringField(tb.ContractAddress).
					Bytes()
			},
			func(tb *TokenBalance) bool {
				return tb.ContractAddress == "0xtestContract"
			},
		)
	)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountAddressIndex}, false)

	tokenBalanceAccount1 := &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount1",
		Balance:         5,
	}

	tokenBalance2Account1 := &TokenBalance{
		ID:              2,
		AccountID:       1,
		ContractAddress: "0xtestContract2",
		AccountAddress:  "0xtestAccount1",
		Balance:         15,
	}

	tokenBalance1Account2 := &TokenBalance{
		ID:              3,
		AccountID:       2,
		ContractAddress: "0xtestContract",
		AccountAddress:  "0xtestAccount2",
		Balance:         7,
	}

	err := tokenBalanceTable.Insert(
		[]*TokenBalance{
			tokenBalanceAccount1,
			tokenBalance2Account1,
			tokenBalance1Account2,
		},
	)
	require.NoError(t, err)

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountAndContractAddressIndex}, true)

	it := db.NewIter(nil)

	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 8, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))
	assert.True(t, strings.Contains(string(keys[7]), "0xtestAccount2") && strings.Contains(string(keys[7]), "0xtestContract"))

	_ = it.Close()

	TokenBalanceAccountAndContractAddressIndex.IndexFilterFunction = func(tr *TokenBalance) bool { return tr.ContractAddress == "0xtestContract2" }

	tokenBalanceTable.AddIndex([]*Index[*TokenBalance]{TokenBalanceAccountAndContractAddressIndex}, true)

	it = db.NewIter(nil)

	keys = [][]byte{}
	for it.First(); it.Valid(); it.Next() {
		buff := make([]byte, len(it.Key()))
		copy(buff, it.Key())
		keys = append(keys, buff)
	}

	require.Equal(t, 7, len(keys))
	assert.True(t, strings.Contains(string(keys[3]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[4]), "0xtestAccount1"))
	assert.True(t, strings.Contains(string(keys[5]), "0xtestAccount2"))
	assert.True(t, strings.Contains(string(keys[6]), "0xtestAccount1") && strings.Contains(string(keys[6]), "0xtestContract"))

	_ = it.Close()

	it = db.NewIter(nil)

	fmt.Printf("----------------- Database Contents ----------------- \n")

	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("0x%x(%s): %s\n", it.Key(), it.Key(), it.Value())
	}

	_ = it.Close()
}

func TestIndex_indexKey(t *testing.T) {
	NameIndexID := IndexID(1)
	NameIndex := NewIndex[*TestStructure](
		NameIndexID,
		func(builder KeyBuilder, ts *TestStructure) []byte {
			return builder.AddStringField(ts.Name).Bytes()
		},
	)

	testStructure := &TestStructure{1, "test", "test desc"}

	assert.Equal(t, NameIndexID, NameIndex.IndexID)
	assert.Equal(t, []byte{0x01, 't', 'e', 's', 't'}, NameIndex.indexKey(NewKeyBuilder(nil), testStructure))
}
