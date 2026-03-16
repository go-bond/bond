package bond

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsafeUpdate_OrphanedIndexEntry_POC(t *testing.T) {
	db := setupDatabase()
	defer tearDownDatabase(t, db)

	var (
		contractAddress = "0xbc1d81dbe4fdad3fc9e958b9eccdb287eddfaa47"
		accountAddress  = "0x45cc2ae9a3fbe747aab491a3dfb747877facfbd4"
		accountID       = uint32(100)
	)

	var (
		staleBalance   uint64 = 500
		currentBalance uint64 = 1000
		newBalance     uint64 = 3000
	)

	const (
		TableID = TableID(1)
		IndexID = IndexID(1)
	)

	balanceIndex := NewIndex(IndexOptions[*TokenBalance]{
		IndexID:   IndexID,
		IndexName: "balance_ordered_index",
		IndexKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint32Field(tb.AccountID).Bytes()
		},
		IndexOrderFunc: func(o IndexOrder, tb *TokenBalance) IndexOrder {
			return o.OrderUint64(tb.Balance, IndexOrderTypeDESC)
		},
	})

	table := NewTable(TableOptions[*TokenBalance]{
		DB:        db,
		TableID:   TableID,
		TableName: "token_balance",
		TablePrimaryKeyFunc: func(builder KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	err := table.AddIndex([]*Index[*TokenBalance]{
		balanceIndex,
	}, true)
	require.NoError(t, err)

	ctx := context.Background()

	// Insert the original record
	originalRecord := &TokenBalance{
		ID:              1,
		AccountID:       accountID,
		ContractAddress: contractAddress,
		AccountAddress:  accountAddress,
		Balance:         currentBalance,
	}

	err = table.Insert(ctx, []*TokenBalance{
		originalRecord,
	})
	require.NoError(t, err)

	count := countIndexEntries(t, table, balanceIndex, accountID)
	require.Equal(t, 1, count, "Should have 1 index entry after insert")

	staleRecord := &TokenBalance{
		ID:              1,
		AccountID:       accountID,
		ContractAddress: contractAddress,
		AccountAddress:  accountAddress,
		Balance:         staleBalance,
	}

	newRecord := &TokenBalance{
		ID:              1,
		AccountID:       accountID,
		ContractAddress: contractAddress,
		AccountAddress:  accountAddress,
		Balance:         newBalance,
	}

	// PROBLEM: we only inserted originalRecord, staleRecord is not in DB, why
	// does UnsafeUpdate work?

	unsafeUpdater := table.(TableUnsafeUpdater[*TokenBalance])
	err = unsafeUpdater.UnsafeUpdate(
		ctx,
		[]*TokenBalance{newRecord},
		[]*TokenBalance{staleRecord},
	)
	require.NoError(t, err)

	count = countIndexEntries(t, table, balanceIndex, accountID)
	assert.Equal(t, 2, count, "This is the orphaned index entry scenario")

	t.Logf("newRecord.id=%0x", balanceIndex.IndexKeyFunction(NewKeyBuilder([]byte{}), newRecord))
	t.Logf("staleRecord.id=%0x", balanceIndex.IndexKeyFunction(NewKeyBuilder([]byte{}), staleRecord))
}

func countIndexEntries(
	t *testing.T,
	table Table[*TokenBalance],
	index *Index[*TokenBalance],
	accountID uint32,
) int {
	count := 0
	err := table.ScanIndexForEach(
		context.Background(),
		index,
		NewSelectorRange(
			&TokenBalance{AccountID: accountID, Balance: math.MaxUint64},
			&TokenBalance{AccountID: accountID, Balance: 0},
		),
		func(keyBytes KeyBytes, lazy Lazy[*TokenBalance]) (bool, error) {
			record, err := lazy.Get()
			if err != nil {
				return false, err
			}
			t.Logf("  Found index entry: key=%x -> Record ID=%d, Balance=%d",
				keyBytes,
				record.ID,
				record.Balance,
			)
			count++
			return true, nil
		},
		false,
	)
	require.NoError(t, err)
	return count
}
