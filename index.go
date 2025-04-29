package bond

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/cockroachdb/pebble"
)

type IndexID uint8
type IndexKeyFunction[T any] func(builder KeyBuilder, t T) []byte
type IndexMultiKeyFunction[T any] func(builder KeyBuilder, t T) [][]byte
type IndexFilterFunction[T any] func(t T) bool
type IndexOrderFunction[T any] func(o IndexOrder, t T) IndexOrder
type IndexOrderType bool

const (
	IndexOrderTypeASC  IndexOrderType = false
	IndexOrderTypeDESC                = true
)

type IndexOrder struct {
	KeyBuilder KeyBuilder
}

func (o IndexOrder) OrderInt64(i int64, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = -i
	}
	o.KeyBuilder = o.KeyBuilder.AddInt64Field(i)
	return o
}

func (o IndexOrder) OrderInt32(i int32, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = -i
	}
	o.KeyBuilder = o.KeyBuilder.AddInt32Field(i)
	return o
}

func (o IndexOrder) OrderInt16(i int16, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = -i
	}
	o.KeyBuilder = o.KeyBuilder.AddInt16Field(i)
	return o
}

func (o IndexOrder) OrderUint64(i uint64, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = ^i
	}
	o.KeyBuilder = o.KeyBuilder.AddUint64Field(i)
	return o
}

func (o IndexOrder) OrderUint32(i uint32, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = ^i
	}
	o.KeyBuilder = o.KeyBuilder.AddUint32Field(i)
	return o
}

func (o IndexOrder) OrderUint16(i uint16, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		i = ^i
	}
	o.KeyBuilder = o.KeyBuilder.AddUint16Field(i)
	return o
}

func (o IndexOrder) OrderByte(b byte, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		b = ^b
	}
	o.KeyBuilder = o.KeyBuilder.AddByteField(b)
	return o
}

func (o IndexOrder) OrderBytes(b []byte, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		for i := 0; i < len(b); i++ {
			b[i] = ^b[i]
		}
	}
	o.KeyBuilder = o.KeyBuilder.AddBytesField(b)
	return o
}

func (o IndexOrder) OrderBigInt(b *big.Int, bits int, orderType IndexOrderType) IndexOrder {
	if orderType == IndexOrderTypeDESC {
		b = big.NewInt(0).Neg(b)
	}
	o.KeyBuilder = o.KeyBuilder.AddBigIntField(b, bits)
	return o
}

func (o IndexOrder) Bytes() []byte {
	return o.KeyBuilder.Bytes()
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
	// IndexID is the ID of the index used when generating the index prefix
	// of an index entry.
	IndexID IndexID

	// IndexName is the name of the index useful for a human readable name.
	IndexName string

	// IndexKeyFunc is the function that generates the index key for a given
	// record. It is also used querying data via an index where you pass the
	// partial record and set the field values that you want to query on,
	// which then generates the corresponding index key entry.
	IndexKeyFunc IndexKeyFunction[T]

	// IndexMultiKeyFunc is the function that generates multiple index keys
	// for a given record. This is helpful for a some what "OR" query operation
	// on multiple fields that will always point to the same record. Note, that
	// during query time, the IndexKeyFunc is still used during query time.
	// See table_test.go for an example of how to use this.
	IndexMultiKeyFunc IndexMultiKeyFunction[T]

	// IndexOrderFunc is the function that generates the index order for a given
	// record. This is useful for a query operation that you want to sort the
	// results by a given field.
	IndexOrderFunc IndexOrderFunction[T]

	// IndexFilterFunc is the function that filters the records that will be
	// indexed. This is useful for a query operation that you want to filter
	// the records that will be indexed.
	IndexFilterFunc IndexFilterFunction[T]
}

type Index[T any] struct {
	IndexID               IndexID
	IndexName             string
	IndexKeyFunction      IndexKeyFunction[T]
	IndexMultiKeyFunction IndexMultiKeyFunction[T]
	IndexFilterFunction   IndexFilterFunction[T]
	IndexOrderFunction    IndexOrderFunction[T]
}

func NewIndex[T any](opt IndexOptions[T]) *Index[T] {
	isSecondary := opt.IndexID != PrimaryIndexID
	hasKeyFunc := opt.IndexKeyFunc != nil
	hasMultiKeyFunc := opt.IndexMultiKeyFunc != nil

	if isSecondary && !hasKeyFunc {
		panic(fmt.Errorf("bond: IndexKeyFunc is required for secondary index %s (ID: %d) to enable iteration/scanning", opt.IndexName, opt.IndexID))
	}
	if !isSecondary && hasMultiKeyFunc {
		panic(fmt.Errorf("bond: IndexMultiKeyFunc cannot be used with the primary index (ID: %d)", opt.IndexID))
	}

	idx := &Index[T]{
		IndexID:               opt.IndexID,
		IndexName:             opt.IndexName,
		IndexKeyFunction:      opt.IndexKeyFunc,
		IndexMultiKeyFunction: opt.IndexMultiKeyFunc,
		IndexOrderFunction:    opt.IndexOrderFunc,
		IndexFilterFunction:   opt.IndexFilterFunc,
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

// Iter returns an iterator for the index.
func (idx *Index[T]) Iter(table Table[T], selector Selector[T], optBatch ...Batch) Iterator {
	var iterConstructor Iterable = table.DB()
	if len(optBatch) > 0 && optBatch[0] != nil {
		iterConstructor = optBatch[0]
	}

	keyBufferPool := table.DB().getKeyBufferPool()

	switch selector.Type() {
	case SelectorTypePoint:
		sel := selector.(SelectorPoint[T])
		lowerBound := encodeIndexKey(table, sel.Point(), idx, keyBufferPool.Get())
		upperBound := keySuccessor(lowerBound[0:_KeyPrefixSplitIndex(lowerBound)], keyBufferPool.Get())

		releaseBuffers := func() {
			keyBufferPool.Put(lowerBound[:0])
			keyBufferPool.Put(upperBound[:0])
		}
		return iterConstructor.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
			releaseBufferOnClose: releaseBuffers,
		})
	case SelectorTypePoints:
		sel := selector.(SelectorPoints[T])
		var pebbleOpts []*IterOptions
		for _, point := range sel.Points() {
			lowerBound := encodeIndexKey(table, point, idx, keyBufferPool.Get())
			upperBound := keySuccessor(lowerBound[0:_KeyPrefixSplitIndex(lowerBound)], keyBufferPool.Get())
			if idx.IndexID == PrimaryIndexID {
				upperBound = keySuccessor(lowerBound, upperBound[:0])
			}

			releaseBuffers := func() {
				keyBufferPool.Put(lowerBound[:0])
				keyBufferPool.Put(upperBound[:0])
			}

			pebbleOpts = append(pebbleOpts, &IterOptions{
				IterOptions: pebble.IterOptions{
					LowerBound: lowerBound,
					UpperBound: upperBound,
				},
				releaseBufferOnClose: releaseBuffers,
			})
		}

		return newIteratorMulti(iterConstructor, pebbleOpts)
	case SelectorTypeRange:
		sel := selector.(SelectorRange[T])
		low, up := sel.Range()

		lowerBound := encodeIndexKey(table, low, idx, keyBufferPool.Get())
		upperBound := encodeIndexKey(table, up, idx, keyBufferPool.Get())
		upperBound = keySuccessor(upperBound, nil)

		releaseBuffers := func() {
			keyBufferPool.Put(lowerBound[:0])
			keyBufferPool.Put(upperBound[:0])
		}
		return iterConstructor.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
			releaseBufferOnClose: releaseBuffers,
		})
	case SelectorTypeRanges:
		sel := selector.(SelectorRanges[T])
		var pebbleOpts []*IterOptions
		for _, r := range sel.Ranges() {
			low, up := r[0], r[1]

			lowerBound := encodeIndexKey(table, low, idx, keyBufferPool.Get())
			upperBound := encodeIndexKey(table, up, idx, keyBufferPool.Get())
			upperBound = keySuccessor(upperBound, nil)

			releaseBuffers := func() {
				keyBufferPool.Put(lowerBound[:0])
				keyBufferPool.Put(upperBound[:0])
			}
			pebbleOpts = append(pebbleOpts, &IterOptions{
				IterOptions: pebble.IterOptions{
					LowerBound: lowerBound,
					UpperBound: upperBound,
				},
				releaseBufferOnClose: releaseBuffers,
			})
		}
		return newIteratorMulti(iterConstructor, pebbleOpts)
	default:
		return errIterator{err: fmt.Errorf("unknown selector type: %v", selector.Type())}
	}
}

func (idx *Index[T]) OnInsert(table Table[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexMultiKeyFunction != nil {
		multiKey := idx.IndexMultiKeyFunction(NewKeyBuilder(nil), tr)
		for _, part := range multiKey {
			err := batch.Set(encodeMultiIndexKeyPart(table, tr, idx, part, nil), _indexKeyValue, Sync)
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		if idx.IndexFilterFunction(tr) {
			return batch.Set(encodeIndexKey(table, tr, idx, buff), _indexKeyValue, Sync)
		}
	}
	return nil
}

// TODOXXX: add support for multi-index-key..
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

	var deleteKeys, setKeys [][]byte
	if idx.IndexMultiKeyFunction != nil {
		if idx.IndexFilterFunction(oldTr) {
			multiKey := idx.IndexMultiKeyFunction(NewKeyBuilder(nil), oldTr)
			for _, part := range multiKey {
				deleteKeys = append(deleteKeys, encodeMultiIndexKeyPart(table, oldTr, idx, part, nil))
			}
		}
		if idx.IndexFilterFunction(tr) {
			multiKey := idx.IndexMultiKeyFunction(NewKeyBuilder(nil), tr)
			for _, part := range multiKey {
				setKeys = append(setKeys, encodeMultiIndexKeyPart(table, tr, idx, part, nil))
			}
		}

		for i := len(setKeys); i < len(deleteKeys); i++ {
			deleteKeys = append(deleteKeys, nil)
		}

		for i := len(deleteKeys); i < len(setKeys); i++ {
			setKeys = append(setKeys, nil)
		}

	} else {
		if idx.IndexFilterFunction(oldTr) {
			deleteKeys = append(deleteKeys, encodeIndexKey(table, oldTr, idx, buff))
		} else {
			deleteKeys = append(deleteKeys, nil)
		}
		if idx.IndexFilterFunction(tr) {
			setKeys = append(setKeys, encodeIndexKey(table, tr, idx, buff2))
		} else {
			setKeys = append(setKeys, nil)
		}
	}

	for i := 0; i < len(deleteKeys); i++ {
		deleteKey := deleteKeys[i]
		setKey := setKeys[i]

		if deleteKey != nil && setKey != nil {
			if !bytes.Equal(deleteKey, setKey) {
				err := batch.Delete(deleteKey, Sync)
				if err != nil {
					return err
				}

				err = batch.Set(setKey, _indexKeyValue, Sync)
				if err != nil {
					return err
				}
			}
		} else if deleteKey != nil {
			err := batch.Delete(deleteKey, Sync)
			if err != nil {
				return err
			}
		} else if setKey != nil {
			err := batch.Set(setKey, _indexKeyValue, Sync)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// TODOXXX: add support for multi-index-key..
func (idx *Index[T]) OnDelete(table Table[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction(tr) {
		if idx.IndexMultiKeyFunction != nil {
			deleteKey := idx.IndexMultiKeyFunction(NewKeyBuilder(nil), tr)
			for _, part := range deleteKey {
				err := batch.Delete(encodeMultiIndexKeyPart(table, tr, idx, part, nil), Sync)
				if err != nil {
					return err
				}
			}
		} else {
			err := batch.Delete(encodeIndexKey(table, tr, idx, buff), Sync)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// TODOXXX: add support for multi-index-key...
func (idx *Index[T]) Intersect(ctx context.Context, table Table[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error) {
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

		intersectKeysMap[BytesToString(KeyBytes(it.Key()).ToDataKeyBytes([]byte{}))] = struct{}{}
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

			key := BytesToString(KeyBytes(it.Key()).ToDataKeyBytes([]byte{}))
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
		intersectKeys = append(intersectKeys, StringToBytes(key))
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
				IndexOrder{KeyBuilder: NewKeyBuilder(b)}, tr,
			).Bytes()
		},
		func(b []byte) []byte {
			return table.PrimaryKey(NewKeyBuilder(b), tr)
		},
		buff,
	)
}

func encodeMultiIndexKeyPart[T any](table Table[T], tr T, idx *Index[T], indexKeyPart []byte, buff []byte) []byte {
	return KeyEncodeRaw(
		table.ID(),
		idx.IndexID,
		func(b []byte) []byte {
			return append(b, indexKeyPart...)
		},
		func(b []byte) []byte {
			return idx.IndexOrderFunction(
				IndexOrder{KeyBuilder: NewKeyBuilder(b)}, tr,
			).Bytes()
		},
		func(b []byte) []byte {
			return table.PrimaryKey(NewKeyBuilder(b), tr)
		},
		buff,
	)
}
