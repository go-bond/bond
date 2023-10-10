package bond

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBtreeIndexOnInsert(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalaceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalaceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
			return builder.AddInt64Field(int64(t.ID)).Bytes()
		},
	})

	tokenBalanceContractIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   PrimaryIndexID + 1,
		IndexName: "token_balance_contract_index",
		IndexKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
			return builder.AddStringField(t.ContractAddress).Bytes()
		},
		IndexFilterFunc: func(t *TokenBalance) bool {
			return true
		},
	})

	entries := []*TokenBalance{{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              3,
		AccountID:       3,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}}

	btreeIndex := &IndexTypeBtree[*TokenBalance]{}
	batch := db.Batch()
	for _, entry := range entries {
		err := btreeIndex.OnInsert(tokenBalanceTable, tokenBalanceContractIndex, entry, batch)
		require.NoError(t, err)
	}

	err := batch.Commit(Sync)
	require.NoError(t, err)

	itr := btreeIndex.Iter(tokenBalanceTable, tokenBalanceContractIndex, NewSelectorPoint(&TokenBalance{
		ContractAddress: "0xcontract1",
	}))

	require.True(t, itr.Valid())
	key := itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[0], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Last())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[2], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Prev())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[1], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Next())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[2], tokenBalanceContractIndex, []byte{}), key)
	require.False(t, itr.Next())
}

func TestBtreeDelete(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalaceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalaceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
			return builder.AddInt64Field(int64(t.ID)).Bytes()
		},
	})

	tokenBalanceContractIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   PrimaryIndexID + 1,
		IndexName: "token_balance_contract_index",
		IndexKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
			return builder.AddStringField(t.ContractAddress).Bytes()
		},
		IndexFilterFunc: func(t *TokenBalance) bool {
			return true
		},
	})

	entries := []*TokenBalance{{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              3,
		AccountID:       3,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}}

	btreeIndex := &IndexTypeBtree[*TokenBalance]{}
	batch := db.Batch()
	for _, entry := range entries {
		err := btreeIndex.OnInsert(tokenBalanceTable, tokenBalanceContractIndex, entry, batch)
		require.NoError(t, err)
	}

	err := batch.Commit(Sync)
	require.NoError(t, err)

	// delete the second entry.
	batch = db.Batch()
	btreeIndex.OnDelete(tokenBalanceTable, tokenBalanceContractIndex, entries[1], batch)
	err = batch.Commit(Sync)
	require.NoError(t, err)

	itr := btreeIndex.Iter(tokenBalanceTable, tokenBalanceContractIndex, NewSelectorPoint(&TokenBalance{
		ContractAddress: "0xcontract1",
	}))

	require.True(t, itr.Valid())
	key := itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[0], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Last())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[2], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Prev())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[0], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Next())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[2], tokenBalanceContractIndex, []byte{}), key)
	require.False(t, itr.Next())

}

func TestBtreeUpdate(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(db)

	const (
		TokenBalaceTableID = TableID(1)
	)

	tokenBalanceTable := NewTable[*TokenBalance](TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TokenBalaceTableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
			return builder.AddInt64Field(int64(t.ID)).Bytes()
		},
	})

	tokenBalanceContractIndex := NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
		IndexID:   PrimaryIndexID + 1,
		IndexName: "token_balance_contract_index",
		IndexKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
			return builder.AddStringField(t.ContractAddress).Bytes()
		},
		IndexFilterFunc: func(t *TokenBalance) bool {
			return true
		},
	})

	entries := []*TokenBalance{{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              2,
		AccountID:       2,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              3,
		AccountID:       3,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}}

	btreeIndex := &IndexTypeBtree[*TokenBalance]{}
	batch := db.Batch()
	for _, entry := range entries {
		err := btreeIndex.OnInsert(tokenBalanceTable, tokenBalanceContractIndex, entry, batch)
		require.NoError(t, err)
	}

	err := batch.Commit(Sync)
	require.NoError(t, err)
	// update the last entry.
	updatedEntry := &TokenBalance{
		ID:              3,
		AccountID:       3,
		ContractAddress: "0xcontract2",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}
	batch = db.Batch()
	err = btreeIndex.OnUpdate(tokenBalanceTable, tokenBalanceContractIndex, entries[2], updatedEntry, batch)
	require.NoError(t, err)
	err = batch.Commit(Sync)
	require.NoError(t, err)

	itr := btreeIndex.Iter(tokenBalanceTable, tokenBalanceContractIndex, NewSelectorPoint(&TokenBalance{
		ContractAddress: "0xcontract1",
	}))

	require.True(t, itr.Valid())
	key := itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[0], tokenBalanceContractIndex, []byte{}), key)
	require.True(t, itr.Last())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, entries[1], tokenBalanceContractIndex, []byte{}), key)
	require.False(t, itr.Next())
	require.NoError(t, itr.Close())

	itr = btreeIndex.Iter(tokenBalanceTable, tokenBalanceContractIndex, NewSelectorPoint(&TokenBalance{
		ContractAddress: "0xcontract2",
	}))
	require.True(t, itr.Valid())
	key = itr.Key()
	require.Equal(t, encodeIndexKey(tokenBalanceTable, updatedEntry, tokenBalanceContractIndex, []byte{}), key)
	require.False(t, itr.Next())
}
