package bond

import (
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

func (t *Table[T]) Insert(tr []T) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	batch := t.db.NewIndexedBatch()
	keysForIndexInsert := make([][]byte, 0, len(t.secondaryIndexes))

	var (
		keyBuffer       [DataKeyBufferSize]byte
		indexKeyBuffer  [IndexKeyBufferSize]byte
		indexKeysBuffer = make([]byte, 0, (PrimaryKeyBufferSize+IndexKeyBufferSize)*len(indexes))
	)

	for _, r := range tr {
		recordKey := t.primaryKeyFunc(
			NewKeyBuilder(keyBuffer[:0]),
			r,
		)

		// index keys
		keysForIndexInsert = keysForIndexInsert[:0]
		for _, idx := range t.secondaryIndexes {
			if idx.IndexFilterFunction(r) {
				rawIndexKey := _KeyEncode(_Key{
					TableID:   t.TableID,
					IndexID:   idx.IndexID,
					IndexKey:  idx.indexKey(NewKeyBuilder(indexKeyBuffer[:0]), r),
					RecordKey: recordKey,
				}, indexKeysBuffer)

				keysForIndexInsert = append(keysForIndexInsert, rawIndexKey)
				indexKeysBuffer = rawIndexKey[len(rawIndexKey):]
			}
		}

		// serialize
		data, err := t.db.Serializer().Serialize(r)
		if err != nil {
			return err
		}

		// insert data
		keyRaw := _KeyEncode(_Key{
			TableID:   t.TableID,
			IndexID:   PrimaryIndexID,
			IndexKey:  []byte{},
			RecordKey: recordKey,
		}, keyBuffer[len(recordKey):len(recordKey)])

		// check if exist
		if ok, _ := t.exist(keyRaw, batch); ok {
			return fmt.Errorf("record: 0x%x(%s) already exist", recordKey, recordKey)
		}

		err = batch.Set(keyRaw, data, pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}

		// update indexes
		for _, key := range keysForIndexInsert {
			err = batch.Set(key, []byte{}, pebble.Sync)
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
	batch := t.db.NewBatch()

	var keyBuffer [KeyBufferSize]byte
	for _, tr := range trs {
		var recordKey = t.recordKeyFunc(NewKeyBuilder(keyBuffer[:0]), tr)
		var key = KeyEncode(Key{
			TableID:   t.TableID,
			IndexID:   MainIndexID,
			IndexKey:  []byte{},
			RecordKey: recordKey,
		}, keyBuffer[len(recordKey):len(recordKey)])

		err := batch.Delete(key, pebble.Sync)
		if err != nil {
			_ = batch.Close()
			return err
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
	var recordKey = t.primaryKeyFunc(NewKeyBuilder(keyBuffer[:0]), tr)
	var key = _KeyEncode(_Key{
		TableID:   t.TableID,
		IndexID:   PrimaryIndexID,
		IndexKey:  []byte{},
		RecordKey: recordKey,
	}, keyBuffer[len(recordKey):len(recordKey)])

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
	return t.ScanIndexForEach(i, s, func(record T) {
		*tr = append(*tr, record)
	})
}

func (t *Table[T]) ScanForEach(f func(t T)) error {
	return t.ScanIndexForEach(t.primaryIndex, make([]T, 1)[0], f)
}

func (t *Table[T]) ScanIndexForEach(i *Index[T], s T, f func(t T)) error {
	var prefixBuffer [DataKeyBufferSize]byte
	var indexKeyBuffer [IndexKeyBufferSize]byte

	prefix := _KeyEncode(_Key{
		TableID:   t.TableID,
		IndexID:   i.IndexID,
		IndexKey:  i.indexKey(NewKeyBuilder(indexKeyBuffer[:0]), s),
		RecordKey: []byte{},
	}, prefixBuffer[:0])

	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})

	var getValue func() error
	var keyBuffer [DataKeyBufferSize]byte
	if i.IndexID == PrimaryIndexID {
		getValue = func() error {
			var record T
			if err := t.db.Serializer().Deserialize(iter.Value(), &record); err == nil {
				f(record)
				return nil
			} else {
				return err
			}
		}
	} else {
		getValue = func() error {
			tableKey := _KeyBytesToDataKeyBytes(
				iter.Key(),
				keyBuffer[:0],
			)

			valueData, closer, err := t.db.Get(tableKey)
			if err != nil {
				return err
			}

			defer func() { _ = closer.Close() }()

			var record T
			if err = t.db.Serializer().Deserialize(valueData, &record); err == nil {
				f(record)
				return nil
			} else {
				return err
			}
		}
	}

	for iter.SeekPrefixGE(prefix); iter.Valid(); iter.Next() {
		err := getValue()
		if err != nil {
			return err
		}
	}

	return nil
}
