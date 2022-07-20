package bond

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"golang.org/x/exp/maps"
)

const PrimaryKeyBufferSize = 10240
const IndexKeyBufferSize = 10240
const DataKeyBufferSize = PrimaryKeyBufferSize + IndexKeyBufferSize + 6

type TableID uint8
type TablePrimaryKeyFunc[T any] func(builder KeyBuilder, t T) []byte

type Table[T any] struct {
	TableID TableID

	db *DB

	primaryKeyFunc TablePrimaryKeyFunc[T]

	primaryIndex     *Index[T]
	secondaryIndexes map[IndexID]*Index[T]

	mutex sync.RWMutex
}

func NewTable[T any](db *DB, id TableID, trkFn TablePrimaryKeyFunc[T]) *Table[T] {
	return &Table[T]{
		TableID:          id,
		db:               db,
		primaryKeyFunc:   trkFn,
		primaryIndex:     NewIndex[T](PrimaryIndexID, func(builder KeyBuilder, t T) []byte { return []byte{} }),
		secondaryIndexes: make(map[IndexID]*Index[T]),
		mutex:            sync.RWMutex{},
	}
}

func (t *Table[T]) AddIndexes(idxs []*Index[T], reIndex ...bool) {
	t.mutex.Lock()
	for _, idx := range idxs {
		t.secondaryIndexes[idx.IndexID] = idx
	}
	t.mutex.Unlock()

	if len(reIndex) > 0 && reIndex[0] {
		// todo: build index
	}
}

func (t *Table[T]) PrimaryIndex() *Index[T] {
	return t.primaryIndex
}

func (t *Table[T]) Insert(trs []T) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	batch := t.db.NewIndexedBatch()
	indexKeys := make([][]byte, 0, len(t.secondaryIndexes))

	var (
		keyBuffer       [DataKeyBufferSize]byte
		indexKeysBuffer = make([]byte, 0, (PrimaryKeyBufferSize+IndexKeyBufferSize)*len(indexes))
	)

	for _, tr := range trs {
		// insert key
		key := t.key(tr, keyBuffer[:0])

		// check if exist
		if ok, _ := t.exist(key, batch); ok {
			return fmt.Errorf("record: %x already exist", key[_KeyPrefixSplitIndex(key):])
		}

		// serialize
		data, err := t.db.Serializer().Serialize(tr)
		if err != nil {
			return err
		}

		err = batch.Set(key, data, pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}

		// index keys
		indexKeys = t.indexKeys(tr, indexes, indexKeysBuffer[:0], indexKeys[:0])

		// update indexes
		for _, indexKey := range indexKeys {
			err = batch.Set(indexKey, []byte{}, pebble.Sync)
			if err != nil {
				_ = batch.Close()
				return err
			}
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		_ = batch.Close()
		return err
	}

	return nil
}

func (t *Table[T]) Update(trs []T) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	batch := t.db.NewIndexedBatch()

	var (
		keyBuffer [DataKeyBufferSize]byte
		//indexKeyBuffer    = make([]byte, DataKeyBufferSize*len(indexes))
		//oldIndexKeyBuffer = make([]byte, DataKeyBufferSize*len(indexes))
	)

	for _, tr := range trs {
		// update key
		key := t.key(tr, keyBuffer[:0])

		// old record
		oldTrData, closer, err := batch.Get(key)
		if err != nil {
			_ = batch.Close()
			return err
		}

		var oldTr T
		err = t.db.Serializer().Deserialize(oldTrData, &oldTr)
		if err != nil {
			_ = batch.Close()
			return err
		}

		_ = closer.Close()

		// serialize
		data, err := t.db.Serializer().Serialize(tr)
		if err != nil {
			return err
		}

		// update entry
		err = batch.Set(key, data, pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}

		// indexKeys to add and remove
		toAddIndexKeys, toRemoveIndexKeys := t.indexKeysDiff(tr, oldTr, indexes)

		// update indexes
		for _, indexKey := range toAddIndexKeys {
			err = batch.Set(indexKey, []byte{}, pebble.Sync)
			if err != nil {
				_ = batch.Close()
				return err
			}
		}

		for _, indexKey := range toRemoveIndexKeys {
			err = batch.Delete(indexKey, pebble.Sync)
			if err != nil {
				_ = batch.Close()
				return err
			}
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		_ = batch.Close()
		return err
	}

	return nil
}

func (t *Table[T]) Delete(trs []T) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	batch := t.db.NewBatch()

	var (
		keyBuffer      [DataKeyBufferSize]byte
		indexKeyBuffer = make([]byte, DataKeyBufferSize*len(indexes))
		indexKeys      = make([][]byte, len(indexes))
	)

	for _, tr := range trs {
		var key = t.key(tr, keyBuffer[:0])
		indexKeys = t.indexKeys(tr, indexes, indexKeyBuffer[:0], indexKeys[:0])

		err := batch.Delete(key, pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}

		for _, indexKey := range indexKeys {
			err = batch.Delete(indexKey, pebble.Sync)
			if err != nil {
				_ = batch.Close()
				return err
			}
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		_ = batch.Close()
		return err
	}

	return nil
}

func (t *Table[T]) Exist(tr T) (bool, T) {
	var keyBuffer [DataKeyBufferSize]byte

	key := t.key(tr, keyBuffer[:0])
	return t.exist(key, nil)
}

func (t *Table[T]) exist(key []byte, batch *pebble.Batch) (bool, T) {
	data, closer, err := t.db.getBatchOrDB(key, batch)
	if err != nil {
		return false, make([]T, 1)[0]
	}

	defer func() { _ = closer.Close() }()

	var tr T
	err = t.db.Serializer().Deserialize(data, &tr)
	if err != nil {
		return false, make([]T, 1)[0]
	}

	return true, tr
}

func (t *Table[T]) Query() Query[T] {
	return newQuery[T](t, t.primaryIndex)
}

func (t *Table[T]) Scan(tr *[]T) error {
	return t.ScanIndex(t.primaryIndex, make([]T, 1)[0], tr)
}

func (t *Table[T]) ScanIndex(i *Index[T], s T, tr *[]T) error {
	return t.ScanIndexForEach(i, s, func(lazy Lazy[T]) (bool, error) {
		if record, err := lazy.Get(); err == nil {
			*tr = append(*tr, record)
			return true, nil
		} else {
			return false, err
		}
	})
}

func (t *Table[T]) ScanForEach(f func(l Lazy[T]) (bool, error)) error {
	return t.ScanIndexForEach(t.primaryIndex, make([]T, 1)[0], f)
}

func (t *Table[T]) ScanIndexForEach(idx *Index[T], s T, f func(t Lazy[T]) (bool, error)) error {
	var prefixBuffer [DataKeyBufferSize]byte

	prefix := t.keyPrefix(idx, s, prefixBuffer[:0])
	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})

	var getValue func() (T, error)
	var keyBuffer [DataKeyBufferSize]byte
	if idx.IndexID == PrimaryIndexID {
		getValue = func() (T, error) {
			var record T
			if err := t.db.Serializer().Deserialize(iter.Value(), &record); err == nil {
				return record, nil
			} else {
				return make([]T, 1)[0], err
			}
		}
	} else {
		getValue = func() (T, error) {
			tableKey := _KeyBytesToDataKeyBytes(
				iter.Key(),
				keyBuffer[:0],
			)

			valueData, closer, err := t.db.Get(tableKey)
			if err != nil {
				return make([]T, 1)[0], err
			}

			defer func() { _ = closer.Close() }()

			var record T
			if err = t.db.Serializer().Deserialize(valueData, &record); err == nil {
				return record, nil
			} else {
				return make([]T, 1)[0], err
			}
		}
	}

	for iter.SeekPrefixGE(prefix); iter.Valid(); iter.Next() {
		if cont, err := f(Lazy[T]{getValue}); !cont || err != nil {
			break
		} else {
			if err != nil {
				return err
			}

			if !cont {
				break
			}
		}
	}

	err := iter.Close()
	if err != nil {
		return err
	}

	return nil
}

func (t *Table[T]) key(tr T, buff []byte) []byte {
	var primaryKey = t.primaryKeyFunc(NewKeyBuilder(buff[:0]), tr)

	return _KeyEncode(_Key{
		TableID:    t.TableID,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		PrimaryKey: primaryKey,
	}, buff[len(primaryKey):len(primaryKey)])
}

func (t *Table[T]) keyPrefix(idx *Index[T], s T, buff []byte) []byte {
	indexKey := idx.indexKey(NewKeyBuilder(buff[:0]), s)

	return _KeyEncode(_Key{
		TableID:    t.TableID,
		IndexID:    idx.IndexID,
		IndexKey:   indexKey,
		PrimaryKey: []byte{},
	}, indexKey[len(indexKey):])
}

func (t *Table[T]) indexKey(tr T, idx *Index[T], buff []byte) []byte {
	primaryKey := t.primaryKeyFunc(NewKeyBuilder(buff[:0]), tr)
	indexKeyPart := idx.indexKey(NewKeyBuilder(primaryKey[len(primaryKey):]), tr)

	return _KeyEncode(_Key{
		TableID:    t.TableID,
		IndexID:    idx.IndexID,
		IndexKey:   indexKeyPart,
		PrimaryKey: primaryKey,
	}, indexKeyPart[len(indexKeyPart):])
}

func (t *Table[T]) indexKeys(tr T, idxs map[IndexID]*Index[T], buff []byte, indexKeysBuff [][]byte) [][]byte {
	indexKeys := indexKeysBuff[:0]

	for _, idx := range idxs {
		if idx.IndexFilterFunction(tr) {
			indexKey := t.indexKey(tr, idx, buff)
			indexKeys = append(indexKeys, indexKey)
			buff = indexKey[len(indexKey):]
		}
	}
	return indexKeys
}

func (t *Table[T]) indexKeysDiff(newTr T, oldTr T, idxs map[IndexID]*Index[T]) (toAdd [][]byte, toRemove [][]byte) {
	newTrKeys := t.indexKeys(newTr, idxs, []byte{}, [][]byte{})
	oldTrKeys := t.indexKeys(oldTr, idxs, []byte{}, [][]byte{})

	for _, newKey := range newTrKeys {
		found := false
		for _, oldKey := range oldTrKeys {
			if bytes.Compare(newKey, oldKey) == 0 {
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
			if bytes.Compare(oldKey, newKey) == 0 {
				found = true
			}
		}

		if !found {
			toAdd = append(toRemove, oldKey)
		}
	}

	return
}
