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

const ReindexBatchSize = 10000

type TableID uint8
type TablePrimaryKeyFunc[T any] func(builder KeyBuilder, t T) []byte

func primaryIndexKey[T any](builder KeyBuilder, t T) []byte { return []byte{} }

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
		primaryIndex:     NewIndex[T](PrimaryIndexID, primaryIndexKey[T], IndexOrderDefault[T]),
		secondaryIndexes: make(map[IndexID]*Index[T]),
		mutex:            sync.RWMutex{},
	}
}

func (t *Table[T]) PrimaryIndex() *Index[T] {
	return t.primaryIndex
}

func (t *Table[T]) AddIndex(idxs []*Index[T], reIndex ...bool) error {
	t.mutex.Lock()
	for _, idx := range idxs {
		t.secondaryIndexes[idx.IndexID] = idx
	}
	t.mutex.Unlock()

	if len(reIndex) > 0 && reIndex[0] {
		return t.reindex(idxs)
	}

	return nil
}

func (t *Table[T]) reindex(idxs []*Index[T]) error {
	idxsMap := make(map[IndexID]*Index[T])
	for _, idx := range idxs {
		idxsMap[idx.IndexID] = idx
		err := t.db.DeleteRange(
			[]byte{byte(t.TableID), byte(idx.IndexID)},
			[]byte{byte(t.TableID), byte(idx.IndexID + 1)}, pebble.Sync)
		if err != nil {
			return fmt.Errorf("failed to delete index: %w", err)
		}
	}

	snap := t.db.NewSnapshot()

	var prefixBuffer [DataKeyBufferSize]byte
	prefix := t.keyPrefix(t.primaryIndex, make([]T, 1)[0], prefixBuffer[:0])

	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})

	batch := t.db.NewBatch()

	counter := 0
	indexKeysBuffer := make([]byte, 0, (PrimaryKeyBufferSize+IndexKeyBufferSize)*len(idxs))
	indexKeys := make([][]byte, 0, len(t.secondaryIndexes))
	for iter.SeekPrefixGE(prefix); iter.Valid(); iter.Next() {
		var tr T

		err := t.db.Serializer().Deserialize(iter.Value(), &tr)
		if err != nil {
			return fmt.Errorf("failed to deserialize during reindexing: %w", err)
		}

		indexKeys = t.indexKeys(tr, idxsMap, indexKeysBuffer[:0], indexKeys[:0])

		for _, indexKey := range indexKeys {
			err = batch.Set(indexKey, []byte{}, pebble.Sync)
			if err != nil {
				return fmt.Errorf("failed to set index key during reindexing: %w", err)
			}
		}

		counter++
		if counter >= ReindexBatchSize {
			counter = 0

			err = batch.Commit(pebble.Sync)
			if err != nil {
				return fmt.Errorf("failed to commit reindex batch: %w", err)
			}

			batch = t.db.NewBatch()
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to commit reindex batch: %w", err)
	}

	_ = iter.Close()

	return nil
}

func (t *Table[T]) Insert(trs []T, batches ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(batches) > 0 && batches[0] != nil
	if externalBatch {
		batch = batches[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var (
		keyBuffer       [DataKeyBufferSize]byte
		indexKeysBuffer = make([]byte, 0, (PrimaryKeyBufferSize+IndexKeyBufferSize)*len(indexes))

		indexKeys = make([][]byte, 0, len(t.secondaryIndexes))
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

	if !externalBatch {
		err := batch.Commit(pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}
	}

	return nil
}

func (t *Table[T]) Update(trs []T, batches ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(batches) > 0 && batches[0] != nil
	if externalBatch {
		batch = batches[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var (
		keyBuffer      [DataKeyBufferSize]byte
		indexKeyBuffer = make([]byte, DataKeyBufferSize*len(indexes)*2)
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
		toAddIndexKeys, toRemoveIndexKeys := t.indexKeysDiff(tr, oldTr, indexes, indexKeyBuffer[:0])

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

	if !externalBatch {
		err := batch.Commit(pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}
	}

	return nil
}

func (t *Table[T]) Delete(trs []T, batches ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(batches) > 0 && batches[0] != nil
	if externalBatch {
		batch = batches[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

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

	if !externalBatch {
		err := batch.Commit(pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}
	}

	return nil
}

func (t *Table[T]) Upsert(trs []T, batches ...*pebble.Batch) error {
	var batch *pebble.Batch
	var externalBatch = len(batches) > 0 && batches[0] != nil
	if externalBatch {
		batch = batches[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var toInsert []T
	var toUpdate []T

	for _, tr := range trs {
		if exist, _ := t.Exist(tr, batch); exist {
			toUpdate = append(toUpdate, tr)
		} else {
			toInsert = append(toInsert, tr)
		}
	}

	if len(toInsert) > 0 {
		err := t.Insert(toInsert, batch)
		if err != nil {
			return err
		}
	}

	if len(toUpdate) > 0 {
		err := t.Update(toUpdate, batch)
		if err != nil {
			return err
		}
	}

	if !externalBatch {
		err := batch.Commit(pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}
	}

	return nil
}

func (t *Table[T]) Exist(tr T, batches ...*pebble.Batch) (bool, T) {
	var batch *pebble.Batch
	if len(batches) > 0 && batches[0] != nil {
		batch = batches[0]
	} else {
		batch = nil
	}

	var keyBuffer [DataKeyBufferSize]byte

	key := t.key(tr, keyBuffer[:0])
	exist, retTr := t.exist(key, batch)
	return exist, retTr
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

func (t *Table[T]) Scan(tr *[]T, batches ...*pebble.Batch) error {
	return t.ScanIndex(t.primaryIndex, make([]T, 1)[0], tr, batches...)
}

func (t *Table[T]) ScanIndex(i *Index[T], s T, tr *[]T, batches ...*pebble.Batch) error {
	return t.ScanIndexForEach(i, s, func(lazy Lazy[T]) (bool, error) {
		if record, err := lazy.Get(); err == nil {
			*tr = append(*tr, record)
			return true, nil
		} else {
			return false, err
		}
	}, batches...)
}

func (t *Table[T]) ScanForEach(f func(l Lazy[T]) (bool, error), batches ...*pebble.Batch) error {
	return t.ScanIndexForEach(t.primaryIndex, make([]T, 1)[0], f, batches...)
}

func (t *Table[T]) ScanIndexForEach(idx *Index[T], s T, f func(t Lazy[T]) (bool, error), batches ...*pebble.Batch) error {
	var prefixBuffer [DataKeyBufferSize]byte

	prefix := t.keyPrefix(idx, s, prefixBuffer[:0])

	var iter *pebble.Iterator
	if len(batches) > 0 && batches[0] != nil {
		batches[0].NewIter(&pebble.IterOptions{
			LowerBound: prefix,
		})
	} else {
		iter = t.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
		})
	}

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
		IndexOrder: []byte{},
		PrimaryKey: primaryKey,
	}, buff[len(primaryKey):len(primaryKey)])
}

func (t *Table[T]) keyPrefix(idx *Index[T], s T, buff []byte) []byte {
	indexKey := idx.IndexKeyFunction(NewKeyBuilder(buff[:0]), s)

	return _KeyEncode(_Key{
		TableID:    t.TableID,
		IndexID:    idx.IndexID,
		IndexKey:   indexKey,
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}, indexKey[len(indexKey):])
}

func (t *Table[T]) indexKey(tr T, idx *Index[T], buff []byte) []byte {
	primaryKey := t.primaryKeyFunc(NewKeyBuilder(buff[:0]), tr)
	indexKeyPart := idx.IndexKeyFunction(NewKeyBuilder(primaryKey[len(primaryKey):]), tr)
	orderKeyPart := idx.IndexOrderFunction(
		IndexOrder{keyBuilder: NewKeyBuilder(indexKeyPart[len(indexKeyPart):])}, tr).Bytes()

	return _KeyEncode(_Key{
		TableID:    t.TableID,
		IndexID:    idx.IndexID,
		IndexKey:   indexKeyPart,
		IndexOrder: orderKeyPart,
		PrimaryKey: primaryKey,
	}, orderKeyPart[len(orderKeyPart):])
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

func (t *Table[T]) indexKeysDiff(newTr T, oldTr T, idxs map[IndexID]*Index[T], buff []byte) (toAdd [][]byte, toRemove [][]byte) {
	newTrKeys := t.indexKeys(newTr, idxs, buff[:0], [][]byte{})
	if len(newTrKeys) != 0 {
		buff = newTrKeys[len(newTrKeys)-1]
		buff = buff[len(buff):]
	}

	oldTrKeys := t.indexKeys(oldTr, idxs, buff[:0], [][]byte{})

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
			toRemove = append(toRemove, oldKey)
		}
	}

	return
}
