package bond_tests

import (
	"context"
	"testing"

	"github.com/go-bond/bond"
	bondmock "github.com/go-bond/bond/internal/testing/mocks/bond"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestContextWithBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mBatch := bondmock.NewMockBatch(ctrl)

	bCtx := bond.ContextWithBatch(context.Background(), mBatch)
	batchFromCtx := bond.ContextRetrieveBatch(bCtx)

	assert.Equal(t, mBatch, batchFromCtx)
}

func TestContextWithSyncBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBatch := bondmock.NewMockBatch(ctrl)
	batch := bond.NewSyncBatch(mockBatch)

	bCtx := bond.ContextWithSyncBatch(context.Background(), batch)
	batchFromCtx := bond.ContextRetrieveSyncBatch(bCtx)

	assert.Equal(t, batch, batchFromCtx)
}
