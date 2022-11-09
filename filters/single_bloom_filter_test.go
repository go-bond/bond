package filters

import (
	"context"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockScanner[T any] struct {
	mock.Mock
}

func (m *MockScanner[T]) Scan(ctx context.Context, tr *[]T, optBatch ...bond.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockScanner[T]) ScanIndex(ctx context.Context, i *bond.Index[T], s T, tr *[]T, optBatch ...bond.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockScanner[T]) ScanForEach(ctx context.Context, f func(keyBytes bond.KeyBytes, l bond.Lazy[T]) (bool, error), optBatch ...bond.Batch) error {
	args := m.Called(ctx, f)
	if err := args.Get(0); err != nil {
		return err.(error)
	}
	return nil
}

func (m *MockScanner[T]) ScanIndexForEach(ctx context.Context, idx *bond.Index[T], s T, f func(keyBytes bond.KeyBytes, t bond.Lazy[T]) (bool, error), optBatch ...bond.Batch) error {
	//TODO implement me
	panic("implement me")
}

func TestNewSingleBloomFilter(t *testing.T) {
	mScanner := &MockScanner[int]{}

	mTableId := bond.TableID(1)
	mIndexId := bond.IndexID(0)
	mKeyPrefix := []byte{byte(mTableId), byte(mIndexId)}
	mKeyPrefix2 := []byte{byte(mTableId), byte(mIndexId + 1)}

	mKey := append(mKeyPrefix, []byte("key1")...)
	mKey2 := append(mKeyPrefix2, []byte("key2")...)

	mScanner.On("ScanForEach", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		f := args.Get(1).(func(key bond.KeyBytes, lazy bond.Lazy[int]) (bool, error))
		_, _ = f(mKey, bond.Lazy[int]{GetFunc: func() (int, error) { return 5, nil }})
	}).Return(nil).Once()

	filter := NewSingleBloomFilter(1000, 0.1)

	err := InitializeFilter(context.Background(), filter, []bond.TableScanner[any]{AnyTableScanner[int](mScanner)})
	require.NoError(t, err)

	assert.Equal(t, true, filter.MayContain(context.TODO(), mKey))
	assert.Equal(t, false, filter.MayContain(context.TODO(), mKey2))

	mScanner.AssertExpectations(t)
}

func TestSingleBloomFilter_Add(t *testing.T) {
	mScanner := &MockScanner[int]{}

	mTableId := bond.TableID(1)
	mIndexId := bond.IndexID(0)
	mKeyPrefix := []byte{byte(mTableId), byte(mIndexId)}

	mKey := append(mKeyPrefix, []byte("key1")...)

	filter := NewSingleBloomFilter(1000, 0.1)

	mScanner.On("ScanForEach", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	err := InitializeFilter(context.Background(), filter, []bond.TableScanner[any]{AnyTableScanner[int](mScanner)})
	require.NoError(t, err)

	assert.Equal(t, false, filter.MayContain(context.TODO(), mKey))

	filter.Add(context.TODO(), mKey)

	assert.Equal(t, true, filter.MayContain(context.TODO(), mKey))

	mScanner.AssertExpectations(t)
}
