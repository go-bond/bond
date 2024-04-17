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

	batch := db.Batch(BatchTypeWriteOnly)
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
