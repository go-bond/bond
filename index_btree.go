package bond

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
)

const DELETE_CHUNK_ID = uint32(0)

var DEFAULT_CHUNK_ID = uint32(1)

var CURRENT_CHUNK_ID = uint32(1)

var DEFAULT_CHUNK_ID_BUF = [4]byte{}

var count = uint64(1)

var DELETE_CHUNK_ID_BUF = [4]byte{}

func init() {
	binary.BigEndian.PutUint32(DEFAULT_CHUNK_ID_BUF[:], DEFAULT_CHUNK_ID)
	binary.BigEndian.PutUint32(DELETE_CHUNK_ID_BUF[:], DELETE_CHUNK_ID)
}

type IndexTypeBtree[T any] struct {
	locker *KeyLocker
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
		indexKey, primaryKey := encodeBtreeIndex(table, tr, idx, CURRENT_CHUNK_ID, buff)
		data, closer, err := batch.Get(indexKey)
		if err != nil {
			return batch.Set(indexKey, primaryKey, Sync)
		}
		defer closer.Close()
		// animating merge method.
		dst := append([]byte{}, data...)
		dst = append(dst, primaryKey...)
		count++
		if count > 1000 {
			count = 0
			CURRENT_CHUNK_ID++
		}
		return batch.Set(indexKey, dst, Sync)
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
		ie.locker.RLockKey(indexKey)
		if err := batch.Merge(indexKey, primaryKey, Sync); err != nil {
			ie.locker.RUnlockKey(indexKey)
			return err
		}
		ie.locker.RUnlockKey(indexKey)
	}

	if idx.IndexFilterFunction(oldTr) {
		indexKey, primaryKey := encodeBtreeIndex(table, oldTr, idx, DELETE_CHUNK_ID, buff2)
		ie.locker.RLockKey(indexKey)
		if err := batch.Merge(indexKey, primaryKey, Sync); err != nil {
			ie.locker.RUnlockKey(indexKey)
			return err
		}
		ie.locker.RUnlockKey(indexKey)
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
		ie.locker.RLockKey(indexKey)
		defer ie.locker.RUnlockKey(indexKey)
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
	case SelectorTypePoints:
		sel := selector.(SelectorPoints[T])

		var pebbleOpts []*IterOptions
		for _, point := range sel.Points() {
			lowerBound := encodeBtreeKey(table, point, idx, keyBufferPool.Get()[:0])
			upperBound := btreeKeySuccessor(lowerBound, keyBufferPool.Get()[:0])
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
		return newIteratorMulti(&BtreeIterConstructor{iterConstructor: iterConstructor}, pebbleOpts)
	default:
		panic("not implemented")
	}
	panic("not implemented!")
}

func (ie *IndexTypeBtree[T]) Intersect(ctx context.Context, table Table[T], idx *Index[T], sel Selector[T], indexes []*Index[T], sels []Selector[T], optBatch ...Batch) ([][]byte, error) {
	panic("not implemented!")
}

var _ IndexType[any] = (*IndexTypeBtree[any])(nil)

func encodeBtreeIndex[T any](table Table[T], tr T, idx *Index[T], chunkID uint32, buff []byte) ([]byte, []byte) {
	buff = KeyEncodeRaw(table.ID(),
		idx.IndexID,
		func(b []byte) []byte {
			return idx.IndexKeyFunction(NewKeyBuilder(b), tr)
		},
		nil,
		nil,
		buff)
	buf := [4]byte{}
	binary.BigEndian.PutUint32(buf[:], chunkID)
	buff = append(buff, buf[:]...)
	indexLen := len(buff)

	// placeholder for primary key len
	primaryKey := table.PrimaryKey(NewKeyBuilder([]byte{}), tr)
	buff = binary.AppendUvarint(buff, uint64(len(primaryKey)))
	buff = append(buff, primaryKey...)
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
	buf := [4]byte{}
	binary.BigEndian.PutUint32(buf[:], DEFAULT_CHUNK_ID)
	buff = append(buff, buf[:]...)
	return buff
}

func btreeKeySuccessor(src []byte, dst []byte) []byte {
	if dst != nil {
		dst = append(dst, src...)
	} else {
		dst = src
	}
	for i := len(src) - 1; i >= len(src)-4; i-- {
		dst[i] = 0xff
	}

	return dst
}

type BtreeIter struct {
	source    Iterator
	prunedIDS map[string]struct{}
	chunkItr  *ChunkIterator
}

type BtreeIterConstructor struct {
	iterConstructor Iterationer
}

func (b *BtreeIterConstructor) Iter(opt *IterOptions, batch ...Batch) Iterator {
	return NewBtreeIter(b.iterConstructor.Iter(opt, batch...))
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
		if bytes.Equal(chunkIdx[len(chunkIdx)-4:len(chunkIdx)], DELETE_CHUNK_ID_BUF[:]) {
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
	indexKey = append(indexKey, b.source.Key()[:len(b.source.Key())-4]...)
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
	for current < len(buf) {
		size, n := binary.Uvarint(buf[current:])
		lens = append(lens, current)
		current = current + n + int(size)
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
	size, n := binary.Uvarint(c.chunk[idx:])
	return c.chunk[idx+n : idx+n+int(size)]
}

func (c *ChunkIterator) Chunk() []byte {
	idx := c.lens[c.pos]
	size, n := binary.Uvarint(c.chunk[idx:])
	return c.chunk[idx : idx+n+int(size)]
}

type BtreeIndexChunker struct {
	locker *KeyLocker
	_db    DB
}

func (c *BtreeIndexChunker) Chunk(key []byte) {
	c.locker.LockKey(key)
	defer c.locker.UnlockKey(key)
	lowerBound := key
	upperBound := btreeKeySuccessor(lowerBound, []byte{})
	itr := c._db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		},
	})
	defer itr.Close()
	LAST_CHUNK_ID := uint32(0)
	for itr.First(); itr.Valid(); itr.Next() {
		key := itr.Key()
		LAST_CHUNK_ID = binary.BigEndian.Uint32(key[len(key)-4 : len(key)])
	}
	LAST_CHUNK_ID++
	// move all the chunks from DEFAULT CHUNKS TO SUBSEQUENET
	// CHUNKS
	batch := c._db.Batch()
	data, closer, err := c._db.Get(key)
	if err != nil {
		panic("error while retriving default chunk")
	}
	defer closer.Close()
	chunkIter := NewChunkIterator(data, map[string]struct{}{})
	chunk := make([]byte, 0)
	count := 0
	for chunkIter.First(); chunkIter.Valid(); chunkIter.Next() {
		chunk = append(chunk, chunkIter.Chunk()...)
		count++
		if count >= 1000 {
			binary.BigEndian.PutUint32(key[len(key)-4:len(key)], LAST_CHUNK_ID)
			if err := batch.Set(key, chunk, Sync); err != nil {
				panic(err)
			}
			LAST_CHUNK_ID++
			chunk = chunk[:0]
			count = 0
		}
	}
	if count > 0 {
		binary.BigEndian.PutUint32(key[len(key)-4:len(key)], LAST_CHUNK_ID)
		if err := batch.Set(key, chunk, Sync); err != nil {
			panic(err)
		}
	}
	binary.BigEndian.PutUint32(key[len(key)-4:len(key)], DEFAULT_CHUNK_ID)
	if err := batch.Delete(key, Sync); err != nil {
		panic(err)
	}
	err = batch.Commit(Sync)
	if err != nil {
		panic(err)
	}
}
