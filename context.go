package bond

import (
	"context"
)

const contextKeyName = "go-bond-batch"
const contextSyncKeyName = "go-bond-sync-batch"

func ContextWithBatch(ctx context.Context, batch Batch) context.Context {
	return context.WithValue(ctx, contextKeyName, batch)
}

func ContextRetrieveBatch(ctx context.Context) Batch {
	if batchI := ctx.Value(contextKeyName); batchI != nil {
		return batchI.(Batch)
	}
	return nil
}

func ContextWithSyncBatch(ctx context.Context, batch *SyncBatch) context.Context {
	return context.WithValue(ctx, contextSyncKeyName, batch)
}

func ContextRetrieveSyncBatch(ctx context.Context) *SyncBatch {
	if batchI := ctx.Value(contextSyncKeyName); batchI != nil {
		return batchI.(*SyncBatch)
	}
	return nil
}
