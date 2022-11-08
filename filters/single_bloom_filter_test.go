package filters

import (
	"context"
	"io"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockBondDB struct {
	mock.Mock
}

func (m *MockBondDB) Serializer() bond.Serializer[any] {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) Delete(key []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) Iter(opt *bond.IterOptions, batch ...bond.Batch) bond.Iterator {
	args := m.Called(opt, batch)
	return args.Get(0).(bond.Iterator)
}

func (m *MockBondDB) Batch() bond.Batch {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) Apply(b bond.Batch, opt bond.WriteOptions) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBondDB) Close() error {
	//TODO implement me
	panic("implement me")
}

type MockIterator struct {
	mock.Mock
}

func (m *MockIterator) First() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *MockIterator) Last() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *MockIterator) Prev() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *MockIterator) Next() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *MockIterator) Valid() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *MockIterator) Error() error {
	//TODO implement me
	panic("implement me")
}

func (m *MockIterator) SeekGE(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (m *MockIterator) SeekPrefixGE(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (m *MockIterator) SeekLT(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (m *MockIterator) Key() []byte {
	args := m.Called()
	return args.Get(0).([]byte)
}

func (m *MockIterator) Value() []byte {
	//TODO implement me
	panic("implement me")
}

func (m *MockIterator) Close() error {
	args := m.Called()
	if err := args.Get(0); err != nil {
		return err.(error)
	}
	return nil
}

func TestNewSingleBloomFilter(t *testing.T) {
	mDB := &MockBondDB{}
	mIterator := &MockIterator{}

	mTableId := bond.TableID(1)
	mIndexId := bond.IndexID(0)
	mKeyPrefix := []byte{byte(mTableId), byte(mIndexId)}
	mKeyPrefix2 := []byte{byte(mTableId), byte(mIndexId + 1)}

	mKey := append(mKeyPrefix, []byte("key1")...)
	mKey2 := append(mKeyPrefix2, []byte("key2")...)

	mDB.On("Iter", mock.Anything, mock.Anything).Return(mIterator)

	mIterator.On("First").Return(true).Once()
	mIterator.On("Valid").Return(true).Once()
	mIterator.On("Valid").Return(true).Once()
	mIterator.On("Next").Return(true).Once()
	mIterator.On("Valid").Return(false).Once()
	mIterator.On("Next").Return(false).Once()

	mIterator.On("Key").Return(mKey).Once()
	mIterator.On("Key").Return(mKey2).Once()

	mIterator.On("Close").Return(nil).Once()

	filter := NewSingleBloomFilter(mDB, 0, 1000, 0.1)

	assert.Equal(t, true, filter.MayContain(context.TODO(), mKey))
	assert.Equal(t, false, filter.MayContain(context.TODO(), mKey2))

	mDB.AssertExpectations(t)
	mIterator.AssertExpectations(t)
}

func TestNewSingleBloomFilter_SingleTable(t *testing.T) {
	mDB := &MockBondDB{}
	mIterator := &MockIterator{}

	mTableId := bond.TableID(1)
	mIndexId := bond.IndexID(0)
	mKeyPrefix := []byte{byte(mTableId), byte(mIndexId)}
	mKeyPrefix2 := []byte{byte(mTableId + 1), byte(mIndexId)}
	mKey := append(mKeyPrefix, []byte("key1")...)
	mKey2 := append(mKeyPrefix2, []byte("key1")...)

	mDB.On("Iter", mock.Anything, mock.Anything).Return(mIterator)

	mIterator.On("First").Return(true).Once()
	mIterator.On("Valid").Return(true).Once()
	mIterator.On("Valid").Return(true).Once()
	mIterator.On("Next").Return(true).Once()

	mIterator.On("Key").Return(mKey).Once()
	mIterator.On("Key").Return(mKey2).Once()

	mIterator.On("Close").Return(nil).Once()

	filter := NewSingleBloomFilter(mDB, 1, 1000, 0.1)

	assert.Equal(t, true, filter.MayContain(context.TODO(), mKey))
	assert.Equal(t, false, filter.MayContain(context.TODO(), mKey2))

	mDB.AssertExpectations(t)
	mIterator.AssertExpectations(t)
}

func TestSingleBloomFilter_Add(t *testing.T) {
	mDB := &MockBondDB{}
	mIterator := &MockIterator{}

	mTableId := bond.TableID(1)
	mIndexId := bond.IndexID(0)
	mKeyPrefix := []byte{byte(mTableId), byte(mIndexId)}

	mKey := append(mKeyPrefix, []byte("key1")...)

	mDB.On("Iter", mock.Anything, mock.Anything).Return(mIterator)

	mIterator.On("First").Return(false).Once()
	mIterator.On("Valid").Return(false).Once()

	mIterator.On("Close").Return(nil).Once()

	filter := NewSingleBloomFilter(mDB, 0, 1000, 0.1)

	assert.Equal(t, false, filter.MayContain(context.TODO(), mKey))

	filter.Add(context.TODO(), mKey)

	assert.Equal(t, true, filter.MayContain(context.TODO(), mKey))

	mDB.AssertExpectations(t)
	mIterator.AssertExpectations(t)
}
