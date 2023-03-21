package bond

import (
	"bytes"
	"math/big"

	"github.com/cockroachdb/pebble"
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

const PrimaryIndexID = IndexID(0)
const PrimaryIndexName = "primary"

type IndexInfo interface {
	ID() IndexID
	Name() string
}

type IndexOptions[T any] struct {
	IndexID         IndexID
	IndexName       string
	IndexKeyFunc    IndexKeyFunction[T]
	IndexOrderFunc  IndexOrderFunction[T]
	IndexFilterFunc IndexFilterFunction[T]
}

type Index[T any] struct {
	IndexID   IndexID
	IndexName string

	IndexKeyFunction    IndexKeyFunction[T]
	IndexFilterFunction IndexFilterFunction[T]
	IndexOrderFunction  IndexOrderFunction[T]
}

func NewIndex[T any](opt IndexOptions[T]) *Index[T] {
	idx := &Index[T]{
		IndexID:             opt.IndexID,
		IndexName:           opt.IndexName,
		IndexKeyFunction:    opt.IndexKeyFunc,
		IndexOrderFunction:  opt.IndexOrderFunc,
		IndexFilterFunction: opt.IndexFilterFunc,
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

func (idx *Index[T]) Iter(table Table[T], sel T, optBatch ...Batch) Iterator {
	lowerBound := encodeIndexKey(table, sel, idx, make([]byte, 0, 1024))
	upperBound := keySuccessor(lowerBound[0:_KeyPrefixSplitIndex(lowerBound)], make([]byte, 0, 1024))

	if len(optBatch) > 0 {
		return optBatch[0].Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
		})
	} else {
		return table.DB().Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
		})
	}
}

func (idx *Index[T]) OnInsert(table Table[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction == nil || (idx.IndexFilterFunction != nil && idx.IndexFilterFunction(tr)) {
		return batch.Set(encodeIndexKey[T](table, tr, idx, buff), _indexKeyValue, Sync)
	}
	return nil
}

func (idx *Index[T]) OnUpdate(table Table[T], oldTr T, tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction == nil || (idx.IndexFilterFunction != nil && idx.IndexFilterFunction(tr)) {
		err := batch.Delete(encodeIndexKey[T](table, oldTr, idx, buff), Sync)
		if err != nil {
			return err
		}

		err = batch.Set(encodeIndexKey[T](table, tr, idx, buff), _indexKeyValue, Sync)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index[T]) OnDelete(table Table[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction == nil || (idx.IndexFilterFunction != nil && idx.IndexFilterFunction(tr)) {
		err := batch.Delete(encodeIndexKey[T](table, tr, idx, buff), Sync)
		if err != nil {
			return err
		}
	}
	return nil
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
		buff[:0],
	)
}

func encodeIndexKeys[T any](table Table[T], tr T, idxs map[IndexID]*Index[T], buff []byte, indexKeysBuff [][]byte) [][]byte {
	indexKeys := indexKeysBuff[:0]

	for _, idx := range idxs {
		if idx.IndexFilterFunction(tr) {
			indexKey := encodeIndexKey(table, tr, idx, buff)
			indexKeys = append(indexKeys, indexKey)
			buff = indexKey[len(indexKey):]
		}
	}
	return indexKeys
}

func encodeIndexKeysDiff[T any](table Table[T], newTr T, oldTr T, idxs map[IndexID]*Index[T], buff []byte) (toAdd [][]byte, toRemove [][]byte) {
	newTrKeys := encodeIndexKeys(table, newTr, idxs, buff[:0], [][]byte{})
	if len(newTrKeys) != 0 {
		buff = newTrKeys[len(newTrKeys)-1]
		buff = buff[len(buff):]
	}

	oldTrKeys := encodeIndexKeys(table, oldTr, idxs, buff[:0], [][]byte{})

	for _, newKey := range newTrKeys {
		found := false
		for _, oldKey := range oldTrKeys {
			if bytes.Equal(newKey, oldKey) {
				found = true
			}
		}

		if !found {
			toAdd = append(toAdd, newKey)
		}
	}

	for _, oldKey := range oldTrKeys {
		found := false
		for _, newKey := range newTrKeys {
			if bytes.Equal(oldKey, newKey) {
				found = true
			}
		}

		if !found {
			toRemove = append(toRemove, oldKey)
		}
	}

	return
}
