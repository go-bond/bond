package bond

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
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
	lowerBound := encodeIndexKey(table, sel, idx, table.DB().getKeyBufferPool().Get()[:0])
	upperBound := keySuccessor(lowerBound[0:_KeyPrefixSplitIndex(lowerBound)], table.DB().getKeyBufferPool().Get()[:0])

	releaseBuffers := func() {
		table.DB().getKeyBufferPool().Put(lowerBound[:0])
		table.DB().getKeyBufferPool().Put(upperBound[:0])
	}

	if len(optBatch) > 0 {
		return optBatch[0].Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
			releaseBufferOnClose: releaseBuffers,
		})
	} else {
		return table.DB().Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
			releaseBufferOnClose: releaseBuffers,
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
	var (
		buff  []byte
		buff2 []byte
	)

	if len(buffs) > 1 {
		buff = buffs[0]
		buff2 = buffs[1]
	} else if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction == nil || (idx.IndexFilterFunction != nil && idx.IndexFilterFunction(tr)) {
		deleteKey := encodeIndexKey[T](table, oldTr, idx, buff)
		setKey := encodeIndexKey[T](table, tr, idx, buff2)

		if bytes.Compare(deleteKey, setKey) != 0 {
			err := batch.Delete(deleteKey, Sync)
			if err != nil {
				return err
			}

			err = batch.Set(setKey, _indexKeyValue, Sync)
			if err != nil {
				return err
			}
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

func (idx *Index[T]) Intersect(ctx context.Context, table Table[T], sel T, indexes []*Index[T], sels []T, optBatch ...Batch) ([][]byte, error) {
	tempKeysMap := map[string]struct{}{}
	intersectKeysMap := map[string]struct{}{}

	it := idx.Iter(table, sel, optBatch...)
	for it.First(); it.Valid(); it.Next() {
		select {
		case <-ctx.Done():
			_ = it.Close()
			return nil, fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		intersectKeysMap[utils.BytesToString(KeyBytes(it.Key()).ToDataKeyBytes([]byte{}))] = struct{}{}
	}
	_ = it.Close()

	for i, idx2 := range indexes {
		it = idx2.Iter(table, sels[i], optBatch...)
		for it.First(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				_ = it.Close()
				return nil, fmt.Errorf("context done: %w", ctx.Err())
			default:
			}

			key := utils.BytesToString(KeyBytes(it.Key()).ToDataKeyBytes([]byte{}))
			if _, ok := intersectKeysMap[key]; ok {
				tempKeysMap[key] = struct{}{}
			}
		}
		_ = it.Close()

		intersectKeysMap = tempKeysMap
		tempKeysMap = map[string]struct{}{}
	}

	intersectKeys := make([][]byte, 0, len(intersectKeysMap))
	for key, _ := range intersectKeysMap {
		intersectKeys = append(intersectKeys, utils.StringToBytes(key))
	}

	sort.Slice(intersectKeys, func(i, j int) bool {
		return bytes.Compare(intersectKeys[i], intersectKeys[j]) == -1
	})

	return intersectKeys, nil
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
