package bond

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
)

type IndexTypeBond[T any] struct {
}

func (ie *IndexTypeBond[T]) OnInsert(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction(tr) {
		return batch.Set(encodeIndexKey(table, tr, idx, buff), _indexKeyValue, Sync)
	}
	return nil
}

func (ie *IndexTypeBond[T]) OnUpdate(table Table[T], idx *Index[T], oldTr T, tr T, batch Batch, buffs ...[]byte) error {
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

	var deleteKey, setKey []byte
	if idx.IndexFilterFunction(oldTr) {
		deleteKey = encodeIndexKey(table, oldTr, idx, buff)
	}
	if idx.IndexFilterFunction(tr) {
		setKey = encodeIndexKey(table, tr, idx, buff2)
	}

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

	return nil
}

func (ie *IndexTypeBond[T]) OnDelete(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction(tr) {
		err := batch.Delete(encodeIndexKey(table, tr, idx, buff), Sync)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ie *IndexTypeBond[T]) Iter(table Table[T], idx *Index[T], selector Selector[T], optBatch ...Batch) Iterator {
	var iterConstructor Iterationer = table.DB()
	if len(optBatch) > 0 {
		iterConstructor = optBatch[0]
	}

	keyBufferPool := table.DB().getKeyBufferPool()

	switch selector.Type() {
	case SelectorTypePoint:
		sel := selector.(SelectorPoint[T])

		lowerBound := encodeIndexKey(table, sel.Point(), idx, keyBufferPool.Get()[:0])
		upperBound := keySuccessor(lowerBound[0:_KeyPrefixSplitIndex(lowerBound)], keyBufferPool.Get()[:0])

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
			lowerBound := encodeIndexKey(table, point, idx, keyBufferPool.Get()[:0])
			upperBound := keySuccessor(lowerBound[0:_KeyPrefixSplitIndex(lowerBound)], keyBufferPool.Get()[:0])
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

		lowerBound := encodeIndexKey(table, low, idx, keyBufferPool.Get()[:0])
		upperBound := encodeIndexKey(table, up, idx, keyBufferPool.Get()[:0])
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

			lowerBound := encodeIndexKey(table, low, idx, keyBufferPool.Get()[:0])
			upperBound := encodeIndexKey(table, up, idx, keyBufferPool.Get()[:0])
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

func (ie *IndexTypeBond[T]) Intersect(ctx context.Context, table Table[T], idx *Index[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error) {
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

var _ IndexType[any] = (*IndexTypeBond[any])(nil)
