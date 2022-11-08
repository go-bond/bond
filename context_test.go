package bond

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockBatch struct {
	mock.Mock
}

func (m *MockBatch) ID() uint64 {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Len() int {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Empty() bool {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Reset() {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Get(key []byte, batch ...Batch) (data []byte, closer io.Closer, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Set(key []byte, value []byte, opt WriteOptions, batch ...Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Delete(key []byte, opt WriteOptions, batch ...Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) DeleteRange(start []byte, end []byte, opt WriteOptions, batch ...Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Iter(opt *IterOptions, batch ...Batch) Iterator {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Apply(b Batch, opt WriteOptions) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Commit(opt WriteOptions) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) Close() error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) OnCommit(f func(b Batch) error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) OnCommitted(f func(b Batch)) {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) OnError(f func(b Batch, err error)) {
	//TODO implement me
	panic("implement me")
}

func (m *MockBatch) OnClose(f func(b Batch)) {
	//TODO implement me
	panic("implement me")
}

func TestContextWithBatch(t *testing.T) {
	mBatch := &MockBatch{}

	bCtx := ContextWithBatch(context.Background(), mBatch)
	batchFromCtx := ContextRetrieveBatch(bCtx)

	assert.Equal(t, mBatch, batchFromCtx)
}

func TestContextWithSyncBatch(t *testing.T) {
	batch := NewSyncBatch(&MockBatch{})

	bCtx := ContextWithSyncBatch(context.Background(), batch)
	batchFromCtx := ContextRetrieveSyncBatch(bCtx)

	assert.Equal(t, batch, batchFromCtx)
}
