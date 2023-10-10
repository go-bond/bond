package bond

import (
	"context"
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
)

const DELETE_CHUNK_ID = 0

const DEFAULT_CHUNK_ID = 1

type IndexTypeBtree[T any] struct {
}

// draft Idea:
// Primary key's are part of index key in index_bond.go. So, each record produced `n`` index key for `n` index.
// This increased the size of the db significantly.
// To tackle the problem, btree index are used when it's possible.
// Here, primary key will be part of value instead of index key.
// eg:
// index_key -> [primary1, primary2]
// we'll be using merge operator to append primary key to index key. so that we can
// avoid `Get` operation during inserting or updating. This leads to following problem.
// 1) primary key per index will grow as the number of record being inserted.
// 2) how do we delete primary key from the list.
// solutions:
// 1) we'll have a background rotuine which actively split the list across multiple index
//   key so all the primary key don't get accumlated in the same index key.
// 2) we'll have a reserved key for each index key called delete index key, which track all the
//   deleted primary key. That will be used to skip the primary key while retriving the
//  records. later background routine will remove the deleted primary key
// from the original list while spliting the list.

func (ie *IndexTypeBtree[T]) OnInsert(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 && buffs[0] != nil {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction(tr) {
		indexKey, primaryKey := encodeBtreeIndex(table, tr, idx, DEFAULT_CHUNK_ID, buff)
		return batch.Merge(indexKey, primaryKey, Sync)
	}
	return nil
}

func (ie *IndexTypeBtree[T]) OnUpdate(table Table[T], idx *Index[T], oldTr T, tr T, batch Batch, buffs ...[]byte) error {
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

	if idx.IndexFilterFunction(tr) {
		indexKey, primaryKey := encodeBtreeIndex(table, tr, idx, DEFAULT_CHUNK_ID, buff)
		if err := batch.Merge(indexKey, primaryKey, Sync); err != nil {
			return err
		}
	}

	if idx.IndexFilterFunction(oldTr) {
		indexKey, primaryKey := encodeBtreeIndex(table, oldTr, idx, DELETE_CHUNK_ID, buff2)
		if err := batch.Merge(indexKey, primaryKey, Sync); err != nil {
			return err
		}
	}
	return nil
}

func (ie *IndexTypeBtree[T]) OnDelete(table Table[T], idx *Index[T], tr T, batch Batch, buffs ...[]byte) error {
	var buff []byte
	if len(buffs) > 0 {
		buff = buffs[0]
	}

	if idx.IndexFilterFunction(tr) {
		indexKey, primaryKey := encodeBtreeIndex(table, tr, idx, DELETE_CHUNK_ID, buff)
		if err := batch.Merge(indexKey, primaryKey, Sync); err != nil {
			return err
		}
	}
	return nil
}

func (ie *IndexTypeBtree[T]) Iter(table Table[T], idx *Index[T], selector Selector[T], optBatch ...Batch) Iterator {
	var iterConstructor Iterationer = table.DB()
	if len(optBatch) > 0 {
		iterConstructor = optBatch[0]
	}

	keyBufferPool := table.DB().getKeyBufferPool()
	switch selector.Type() {
	case SelectorTypePoint:
		sel := selector.(*selectorPoint[T])
		lowerBound := encodeBtreeKey(table, sel.Point(), idx, keyBufferPool.Get()[:0])
		upperBound := btreeKeySuccessor(lowerBound, keyBufferPool.Get()[:0])
		releaseBuffer := func() {
			keyBufferPool.Put(lowerBound)
			keyBufferPool.Put(upperBound)
		}
		itr := iterConstructor.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowerBound,
				UpperBound: upperBound,
			},
			releaseBufferOnClose: releaseBuffer,
		})
		return NewBtreeIter(itr)
	default:
		panic("not implemented")
	}
	panic("not implemented!")
}

func (ie *IndexTypeBtree[T]) Intersect(ctx context.Context, table Table[T], idx *Index[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error) {
	panic("not implemented!")
}

var _ IndexType[any] = (*IndexTypeBtree[any])(nil)

func encodeBtreeIndex[T any](table Table[T], tr T, idx *Index[T], chunkID uint8, buff []byte) ([]byte, []byte) {
	buff = KeyEncodeRaw(table.ID(),
		idx.IndexID,
		func(b []byte) []byte {
			return idx.IndexKeyFunction(NewKeyBuilder(b), tr)
		},
		nil,
		nil,
		buff)
	buff = append(buff, chunkID)
	indexLen := len(buff)

	// placeholder for primary key len
	buff = append(buff, []byte{0x00, 0x00, 0x00, 0x00}...)
	buff = table.PrimaryKey(NewKeyBuilder(buff), tr)
	binary.BigEndian.PutUint32(buff[indexLen:indexLen+4], uint32(len(buff)-(indexLen+4)))
	return buff[:indexLen], buff[indexLen:]
}

func encodeBtreeKey[T any](table Table[T], tr T, idx *Index[T], buff []byte) []byte {
	buff = KeyEncodeRaw(table.ID(),
		idx.IndexID,
		func(b []byte) []byte {
			return idx.IndexKeyFunction(NewKeyBuilder(b), tr)
		},
		nil,
		nil,
		buff)
	buff = append(buff, DELETE_CHUNK_ID)
	return buff
}

func btreeKeySuccessor(src []byte, dst []byte) []byte {
	if dst != nil {
		dst = append(dst, src...)
	} else {
		dst = src
	}
	dst[len(src)-1] = 0xff
	return dst
}

type BtreeIter struct {
	source    Iterator
	prunedIDS map[string]struct{}
	chunkItr  *ChunkIterator
}

var _ Iterator = &BtreeIter{}

func NewBtreeIter(source Iterator) Iterator {
	itr := &BtreeIter{
		source:    source,
		prunedIDS: make(map[string]struct{}),
		chunkItr:  NewChunkIterator([]byte{}, map[string]struct{}{}),
	}
	itr.First()
	return itr
}

func (b *BtreeIter) First() bool {
	if !b.source.First() {
		return false
	}

	for chunkIdx := b.source.Key(); b.source.Valid(); b.source.Next() {
		if chunkIdx[len(chunkIdx)-1] == DELETE_CHUNK_ID {
			if len(b.prunedIDS) != 0 {
				continue
			}
			// initialize the pruned ids if it's not intialized.
			chunkItr := NewChunkIterator(b.source.Value(), map[string]struct{}{})
			for ; chunkItr.Valid(); chunkItr.Next() {
				b.prunedIDS[utils.BytesToString(chunkItr.Value())] = struct{}{}
			}
			continue
		}
		b.chunkItr = NewChunkIterator(b.source.Value(), b.prunedIDS)
		if !b.chunkItr.First() {
			return b.Next()
		}
		return true
	}
	return false
}

func (b *BtreeIter) Key() []byte {
	if !b.chunkItr.Valid() {
		return []byte{}
	}
	// construct indexKey as per index_bond.go
	indexKey := make([]byte, 0)
	indexKey = append(indexKey, b.source.Key()[:len(b.source.Key())-1]...)
	indexKey = append(indexKey, []byte{0x00, 0x00, 0x00, 0x00}...)
	indexKey = append(indexKey, b.chunkItr.Value()...)
	return indexKey
}

func (b *BtreeIter) Value() []byte {
	return []byte{}
}

func (b *BtreeIter) Next() bool {
	if b.chunkItr.Next() {
		return true
	}
	if !b.source.Next() {
		return false
	}
	b.chunkItr = NewChunkIterator(b.source.Value(), b.prunedIDS)
	return true
}

func (b *BtreeIter) Last() bool {
	if !b.source.Last() {
		return false
	}
	b.chunkItr = NewChunkIterator(b.source.Value(), b.prunedIDS)
	if !b.chunkItr.Last() {
		return b.Prev()
	}
	return true
}

func (b *BtreeIter) Error() error {
	return b.source.Error()
}

func (b *BtreeIter) Prev() bool {
	if b.chunkItr.Prev() {
		return true
	}
	if b.source.Prev() {
		b.chunkItr = NewChunkIterator(b.source.Value(), b.prunedIDS)
		if !b.chunkItr.Last() {
			return b.Prev()
		}
		return true
	}
	return false
}

func (b *BtreeIter) Valid() bool {
	if b.chunkItr == nil {
		return b.source.Valid()
	}
	return b.chunkItr.Valid()
}

func (b *BtreeIter) SeekGE(key []byte) bool {
	panic("implement me")
}

func (b *BtreeIter) SeekPrefixGE(key []byte) bool {
	panic("implement me")
}

func (b *BtreeIter) SeekLT(key []byte) bool {
	panic("implement me")
}

func (b *BtreeIter) Close() error {
	return b.source.Close()
}

type ChunkIterator struct {
	chunk     []byte
	lens      []int
	pos       int
	prunedIDS map[string]struct{}
}

func NewChunkIterator(buf []byte, prunedIDS map[string]struct{}) *ChunkIterator {
	lens := []int{}
	current := 0
	for current+4 < len(buf) {
		idLen := binary.BigEndian.Uint32(buf[current : current+4])
		lens = append(lens, current)
		current = current + 4 + int(idLen)
	}
	return &ChunkIterator{
		chunk:     buf,
		lens:      lens,
		pos:       0,
		prunedIDS: prunedIDS,
	}
}

func (c *ChunkIterator) Valid() bool {
	return c.pos < len(c.lens) && c.pos > -1
}

func (c *ChunkIterator) First() bool {
	if !c.Valid() {
		return false
	}
	if !c.isPruned() {
		return true
	}
	return c.Next()
}

func (c *ChunkIterator) Next() bool {
	if !c.Valid() {
		return false
	}
	c.pos++
	if c.Valid() && !c.isPruned() {
		return true
	}
	return c.Next()
}

func (c *ChunkIterator) isPruned() bool {
	_, ok := c.prunedIDS[utils.BytesToString(c.Value())]
	return ok
}

func (c *ChunkIterator) Last() bool {
	c.pos = len(c.lens) - 1
	if c.Valid() && !c.isPruned() {
		return true
	}
	return c.Prev()
}

func (c *ChunkIterator) Prev() bool {
	if !c.Valid() {
		return false
	}
	c.pos--
	if c.Valid() && !c.isPruned() {
		return true
	}
	return c.Prev()
}

func (c *ChunkIterator) Value() []byte {
	idx := c.lens[c.pos]
	idxLen := binary.BigEndian.Uint32(c.chunk[idx : idx+4])
	return c.chunk[idx+4 : idx+4+int(idxLen)]
}
