package bond

import "math"

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
		if i > 0 {
			i = math.MinInt64 + i
		} else if i < 0 {
			i = i - math.MaxInt64
		}
	}

	o.keyBuilder = o.keyBuilder.AddInt64Field(i)
	return o
}

func (o IndexOrder) OrderInt32(i int32, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		if i > 0 {
			i = math.MinInt32 + i
		} else if i < 0 {
			i = i - math.MaxInt32
		}
	}

	o.keyBuilder = o.keyBuilder.AddInt32Field(i)
	return o
}

func (o IndexOrder) OrderInt16(i int16, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		if i > 0 {
			i = math.MinInt16 + i
		} else if i < 0 {
			i = i - math.MaxInt16
		}
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

func (o IndexOrder) Bytes() []byte {
	return o.keyBuilder.Bytes()
}

func IndexOrderDefault[T any](o IndexOrder, t T) IndexOrder {
	return o
}

const PrimaryIndexID = IndexID(0)

type Index[T any] struct {
	IndexID             IndexID
	IndexKeyFunction    IndexKeyFunction[T]
	IndexFilterFunction IndexFilterFunction[T]
	IndexOrderFunction  IndexOrderFunction[T]
}

func NewIndex[T any](idxID IndexID, idxFn IndexKeyFunction[T], idxOrderFn IndexOrderFunction[T], idxFFn ...IndexFilterFunction[T]) *Index[T] {
	idx := &Index[T]{
		IndexID:            idxID,
		IndexKeyFunction:   idxFn,
		IndexOrderFunction: idxOrderFn,
		IndexFilterFunction: func(t T) bool {
			return true
		},
	}

	if len(idxFFn) > 0 {
		idx.IndexFilterFunction = idxFFn[0]
	}

	return idx
}
