package bond

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func TestContextWithBatch(t *testing.T) {
	batch := &pebble.Batch{}

	bCtx := ContextWithBatch(context.Background(), batch)
	batchFromCtx := ContextRetrieveBatch(bCtx)

	assert.Equal(t, batch, batchFromCtx)
}

func TestContextWithSyncBatch(t *testing.T) {
	batch := NewSyncBatch(&pebble.Batch{})

	bCtx := ContextWithSyncBatch(context.Background(), batch)
	batchFromCtx := ContextRetrieveSyncBatch(bCtx)

	assert.Equal(t, batch, batchFromCtx)
}
