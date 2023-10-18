package bond

import (
	"context"
	"math/big"
)

type IndexID uint8
type IndexKeyFunction[T any] func(builder KeyBuilder, t T) []byte
type IndexFilterFunction[T any] func(t T) bool
type IndexOrderFunction[T any] func(o IndexOrder, t T) IndexOrder
type IndexOrderType bool

const (
	IndexOrderTypeASC  IndexOrderType = false
	IndexOrderTypeDESC                = true
)

type IndexOrder struct {
	keyBuilder KeyBuilder
}

func (o IndexOrder) OrderInt64(i int64, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = -i
	}

	o.keyBuilder = o.keyBuilder.AddInt64Field(i)
	return o
}

func (o IndexOrder) OrderInt32(i int32, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = -i
	}

	o.keyBuilder = o.keyBuilder.AddInt32Field(i)
	return o
}

func (o IndexOrder) OrderInt16(i int16, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = -i
	}

	o.keyBuilder = o.keyBuilder.AddInt16Field(i)
	return o
}

func (o IndexOrder) OrderUint64(i uint64, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = ^i
	}

	o.keyBuilder = o.keyBuilder.AddUint64Field(i)
	return o
}

func (o IndexOrder) OrderUint32(i uint32, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = ^i
	}

	o.keyBuilder = o.keyBuilder.AddUint32Field(i)
	return o
}

func (o IndexOrder) OrderUint16(i uint16, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = ^i
	}

	o.keyBuilder = o.keyBuilder.AddUint16Field(i)
	return o
}

func (o IndexOrder) OrderByte(b byte, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		b = ^b
	}

	o.keyBuilder = o.keyBuilder.AddByteField(b)
	return o
}

func (o IndexOrder) OrderBytes(b []byte, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		for i := 0; i < len(b); i++ {
			b[i] = ^b[i]
		}
	}

	o.keyBuilder = o.keyBuilder.AddBytesField(b)
	return o
}

func (o IndexOrder) OrderBigInt(b *big.Int, bits int, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		b = big.NewInt(0).Neg(b)
	}

	o.keyBuilder = o.keyBuilder.AddBigIntField(b, bits)
	return o
}

func (o IndexOrder) Bytes() []byte {
	return o.keyBuilder.Bytes()
}

func IndexOrderDefault[T any](o IndexOrder, t T) IndexOrder {
	return o
}

type IndexType[T any] interface {
	OnInsert(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error
	OnUpdate(table Table[T], idx *Index[T], oldTr T, tr T, batch Batch, buffs ...[]byte) error
	OnDelete(table Table[T], idx *Index[T], t T, batch Batch, buffs ...[]byte) error

	Iter(table Table[T], idx *Index[T], selector Selector[T], optBatch ...Batch) Iterator
	Intersect(ctx context.Context, table Table[T], idx *Index[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error)
}

const PrimaryIndexID = IndexID(0)
const PrimaryIndexName = "primary"

type IndexInfo interface {
	ID() IndexID
	Name() string
}

type IndexOptions[T any] struct {
	IndexID   IndexID
	IndexName string
	IndexType IndexType[T]

	IndexKeyFunc    IndexKeyFunction[T]
	IndexOrderFunc  IndexOrderFunction[T]
	IndexFilterFunc IndexFilterFunction[T]
}

type Index[T any] struct {
	IndexID   IndexID
	IndexName string
	IndexType IndexType[T]

	IndexKeyFunction    IndexKeyFunction[T]
	IndexFilterFunction IndexFilterFunction[T]
	IndexOrderFunction  IndexOrderFunction[T]
}

func NewIndex[T any](opt IndexOptions[T]) *Index[T] {
	idx := &Index[T]{
		IndexID:             opt.IndexID,
		IndexName:           opt.IndexName,
		IndexType:           opt.IndexType,
		IndexKeyFunction:    opt.IndexKeyFunc,
		IndexOrderFunction:  opt.IndexOrderFunc,
		IndexFilterFunction: opt.IndexFilterFunc,
	}

	if idx.IndexType == nil {
		//idx.IndexType = //&IndexTypeBond[T]{}
		idx.IndexType = &IndexTypeBtree[T]{
			count:          1,
			currentChunkID: 1,
		}
	}

	if idx.IndexOrderFunction == nil {
		idx.IndexOrderFunction = IndexOrderDefault[T]
	}

	if idx.IndexFilterFunction == nil {
		idx.IndexFilterFunction = func(t T) bool {
			return true
		}
	}

	return idx
}

func (idx *Index[T]) ID() IndexID {
	return idx.IndexID
}

func (idx *Index[T]) Name() string {
	return idx.IndexName
}

func (idx *Index[T]) OnInsert(table Table[T], tr T, batch Batch, buffs ...[]byte) error {
	return idx.IndexType.OnInsert(table, idx, tr, batch, buffs...)
}

func (idx *Index[T]) OnUpdate(table Table[T], oldTr T, tr T, batch Batch, buffs ...[]byte) error {
	return idx.IndexType.OnUpdate(table, idx, oldTr, tr, batch, buffs...)
}

func (idx *Index[T]) OnDelete(table Table[T], tr T, batch Batch, buffs ...[]byte) error {
	return idx.IndexType.OnDelete(table, idx, tr, batch, buffs...)
}

// Iter returns an iterator for the index.
func (idx *Index[T]) Iter(table Table[T], selector Selector[T], optBatch ...Batch) Iterator {
	return idx.IndexType.Iter(table, idx, selector, optBatch...)
}

func (idx *Index[T]) Intersect(ctx context.Context, table Table[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error) {
	return idx.IndexType.Intersect(ctx, table, idx, sel, indexes, sels, optBatch...)
}

func encodeIndexKey[T any](table Table[T], tr T, idx *Index[T], buff []byte) []byte {
	return KeyEncodeRaw(
		table.ID(),
		idx.IndexID,
		func(b []byte) []byte {
			return idx.IndexKeyFunction(NewKeyBuilder(b), tr)
		},
		func(b []byte) []byte {
			return idx.IndexOrderFunction(
				IndexOrder{keyBuilder: NewKeyBuilder(b)}, tr,
			).Bytes()
		},
		func(b []byte) []byte {
			return table.PrimaryKey(NewKeyBuilder(b), tr)
		},
		buff,
	)
}
