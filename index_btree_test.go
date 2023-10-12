package bond

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
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

	locker := NewLocker()

	btreeIndex := &IndexTypeBtree[*TokenBalance]{
		locker: locker,
	}
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

func TestBtreeSelectorPoints(t *testing.T) {
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
	}, {
		ID:              4,
		AccountID:       4,
		ContractAddress: "0xcontract2",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              5,
		AccountID:       5,
		ContractAddress: "0xcontract2",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, {
		ID:              6,
		AccountID:       6,
		ContractAddress: "0xcontract3",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}}

	locker := NewLocker()
	btreeIndex := &IndexTypeBtree[*TokenBalance]{locker: locker}
	batch := db.Batch()
	for _, entry := range entries {
		err := btreeIndex.OnInsert(tokenBalanceTable, tokenBalanceContractIndex, entry, batch)
		require.NoError(t, err)
	}

	err := batch.Commit(Sync)
	require.NoError(t, err)

	itr := btreeIndex.Iter(tokenBalanceTable, tokenBalanceContractIndex, NewSelectorPoints(&TokenBalance{
		ContractAddress: "0xcontract1",
	}, &TokenBalance{
		ContractAddress: "0xcontract2",
	}, &TokenBalance{
		ContractAddress: "0xcontract3",
	}))
	defer itr.Close()

	for idx, entry := range entries {
		key := itr.Key()
		require.Equal(t, encodeIndexKey(tokenBalanceTable, entry, tokenBalanceContractIndex, []byte{}), key, "failed at idx", idx)
		if idx < len(entries)-1 {
			require.True(t, itr.Next(), "failed at index", idx)
		}
	}
}

func TestBondIndexChunker(t *testing.T) {
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

	locker := NewLocker()
	btreeIndex := &IndexTypeBtree[*TokenBalance]{locker: locker}
	for i := 0; i < 2000; i++ {
		batch := db.Batch()
		err := btreeIndex.OnInsert(tokenBalanceTable, tokenBalanceContractIndex, &TokenBalance{
			ID:              uint64(i),
			AccountID:       uint32(i),
			ContractAddress: "0xcontract1",
			AccountAddress:  "0xaccount1",
			Balance:         100,
		}, batch)
		require.NoError(t, err)
		require.NoError(t, batch.Commit(Sync))
	}
	// now default idx should have 2000 index.
	indexkKey, _ := encodeBtreeIndex(tokenBalanceTable, &TokenBalance{
		ID:              1,
		AccountID:       1,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, tokenBalanceContractIndex, DEFAULT_CHUNK_ID, []byte{})
	data, closer, err := db.Get(indexkKey)
	require.NoError(t, err)
	chunkItr := NewChunkIterator(data, map[string]struct{}{})
	require.Equal(t, 2000, len(chunkItr.lens))
	closer.Close()

	chunker := &BtreeIndexChunker{
		_db:    db,
		locker: locker,
	}
	// this will create two index with 1000 entries each.
	chunker.Chunk(indexkKey)
	batch := db.Batch()
	err = btreeIndex.OnInsert(tokenBalanceTable, tokenBalanceContractIndex, &TokenBalance{
		ID:              2000,
		AccountID:       2000,
		ContractAddress: "0xcontract1",
		AccountAddress:  "0xaccount1",
		Balance:         100,
	}, batch)
	require.NoError(t, err)
	require.NoError(t, batch.Commit(Sync))

	itr := db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: indexkKey,
			UpperBound: btreeKeySuccessor(indexkKey, []byte{}),
		},
	})
	i := 0
	for itr.First(); itr.Valid(); itr.Next() {
		if i == 0 {
			itr := NewChunkIterator(itr.Value(), map[string]struct{}{})
			require.Equal(t, len(itr.lens), 1)
			i++
			continue
		}
		itr := NewChunkIterator(itr.Value(), map[string]struct{}{})
		require.Equal(t, len(itr.lens), 1000)
		i++
	}
	require.Equal(t, 3, i)
	require.NoError(t, itr.Close())
}

func TestIndexBenchSize(t *testing.T) {

	ingestIndex := func(n int, index *Index[*TokenBalance], table Table[*TokenBalance], chunker *BtreeIndexChunker) {
		for i := 0; i < n; i++ {
			batch := table.DB().Batch()
			index.OnInsert(table, &TokenBalance{
				ID:              uint64(i),
				AccountID:       uint32(i),
				ContractAddress: "0xcontract1",
				AccountAddress:  "0xaccount1",
				Balance:         100,
			}, batch)
			err := batch.Commit(Sync)
			require.NoError(t, err)
			if i%2000 == 0 && chunker != nil {
				indexkKey, _ := encodeBtreeIndex(table, &TokenBalance{
					ID:              1,
					AccountID:       1,
					ContractAddress: "0xcontract1",
					AccountAddress:  "0xaccount1",
					Balance:         100,
				}, index, DEFAULT_CHUNK_ID, []byte{})
				chunker._db = table.DB()
				chunker.Chunk(indexkKey)
			}
		}
		maxKey := KeyEncode(Key{
			TableID: 0xff,
			IndexID: 0xff,
		})
		err := table.DB().Backend().Compact(nil, maxKey, true)
		require.NoError(t, err)
		time.Sleep(time.Second * 10)
		// wait for sometime for comeplete compaction
	}

	locker := NewLocker()
	type TestCase[T any] struct {
		index   *Index[T]
		chunker *BtreeIndexChunker
	}
	testCases := []TestCase[*TokenBalance]{{
		index: NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   PrimaryIndexID + 1,
			IndexName: "token_balance_contract_index",
			IndexKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
				return builder.AddStringField(t.ContractAddress).Bytes()
			},
			IndexFilterFunc: func(t *TokenBalance) bool {
				return true
			},
		}),
	}, {
		index: NewIndex[*TokenBalance](IndexOptions[*TokenBalance]{
			IndexID:   PrimaryIndexID + 2,
			IndexName: "token_balance_contract_index_btree",
			IndexKeyFunc: func(builder KeyBuilder, t *TokenBalance) []byte {
				return builder.AddStringField(t.ContractAddress).Bytes()
			},
			IndexFilterFunc: func(t *TokenBalance) bool {
				return true
			},
			IndexType: &IndexTypeBtree[*TokenBalance]{locker: locker},
		}),
		chunker: &BtreeIndexChunker{
			locker: locker,
		},
	}}

	DirSize := func(path string) (int64, error) {
		var size int64
		err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.Contains(path, "sst") {
				fmt.Println(path)
				size += info.Size()
			}
			return err
		})
		return size, err
	}

	for idx, testCase := range testCases {
		dbName = fmt.Sprintf("%s_%d", dbName, idx)
		db := setupDatabase()

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
		ingestIndex(10^10000, testCase.index, tokenBalanceTable, testCase.chunker)
		db.Backend().Flush()
		size, err := DirSize(dbName)
		require.NoError(t, err)
		fmt.Println("case ", idx, "size", size)
		db.Close()
	}

}
