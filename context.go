package bond

import (
	"context"

	"github.com/cockroachdb/pebble"
)

const contextKeyName = "go-bond-batch"
const contextSyncKeyName = "go-bond-sync-batch"

func ContextWithBatch(ctx context.Context, batch *pebble.Batch) context.Context {
	return context.WithValue(ctx, contextKeyName, batch)
}

func ContextRetrieveBatch(ctx context.Context) *pebble.Batch {
	return ctx.Value(contextKeyName).(*pebble.Batch)
}

func ContextWithSyncBatch(ctx context.Context, batch *SyncBatch) context.Context {
	return context.WithValue(ctx, contextSyncKeyName, batch)
}

func ContextRetrieveSyncBatch(ctx context.Context) *SyncBatch {
	return ctx.Value(contextSyncKeyName).(*SyncBatch)
}
