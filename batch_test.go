package bond

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Batch_Callbacks(t *testing.T) {
	db, t1, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	counter := 0

	batch := db.Batch()
	batch.OnCommit(func(b Batch) error {
		counter++
		return nil
	})
	batch.OnCommitted(func(b Batch) {
		counter++
	})

	err := batch.Commit(Sync)
	require.NoError(t, err)
	assert.Equal(t, 0, counter)

	err = t1.Insert(context.Background(), []*TokenBalance{
		{
			ID:              0,
			AccountID:       0,
			ContractAddress: "",
			AccountAddress:  "",
			TokenID:         0,
			Balance:         0,
		},
	}, batch)
	require.NoError(t, err)

	err = batch.Commit(Sync)
	require.NoError(t, err)
	assert.Equal(t, 2, counter)
}

func Test_Batch_ResetRetained(t *testing.T) {
	db, t1, _, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	batch := db.Batch()
	defer func() { _ = batch.Close() }()

	var count int
	var inserts []*TokenBalance
	for i := 0; i < 10000; i++ {
		inserts = append(inserts, &TokenBalance{
			ID:              uint64(count),
			AccountID:       4,
			ContractAddress: "0x122442131373712864321876413274623814238164214631",
			AccountAddress:  "0x122442131373712864321876413274623814238164214631",
			TokenID:         5,
			Balance:         1,
		})
		count++
	}

	err := t1.Insert(context.Background(), inserts, batch)
	require.NoError(t, err)

	err = batch.Commit(Sync)
	require.NoError(t, err)

	batch.ResetRetained()

	for _, tb := range inserts {
		tb.Balance = 5
	}

	err = t1.Update(context.Background(), inserts, batch)
	require.NoError(t, err)

	err = batch.Commit(Sync)
	require.NoError(t, err)

	var tokenBalances []*TokenBalance
	err = t1.Scan(context.Background(), &tokenBalances, false)
	require.NoError(t, err)
	assert.Equal(t, inserts, tokenBalances)
}
