package bond

import (
	"bytes"
	"sync"

	"github.com/cockroachdb/pebble"
)

type TableID uint8
type TableRecordKeyFunc[T any] func(t T) []byte

type Table[T any] struct {
	TableID TableID

	db *DB

	recordKeyFunc TableRecordKeyFunc[T]

	indexes map[IndexID]*Index[T]

	mutex sync.RWMutex
}

func NewTable[T any](db *DB, id TableID, trkFn TableRecordKeyFunc[T]) *Table[T] {
	t := &Table[T]{
		TableID:       id,
		db:            db,
		recordKeyFunc: trkFn,
		indexes:       make(map[IndexID]*Index[T]),
		mutex:         sync.RWMutex{},
	}

	t.AddIndexes([]*Index[T]{
		NewIndex[T](DefaultMainIndexID, func(t T) []byte { return []byte{} }),
	}, false)

	return t
}

func (t *Table[T]) AddIndexes(idxs []*Index[T], reIndex ...bool) {
	t.mutex.Lock()
	for _, idx := range idxs {
		t.indexes[idx.IndexID] = idx
	}
	t.mutex.Unlock()

	if len(reIndex) > 0 && reIndex[0] {
		// todo: build index
	}
}

func (t *Table[T]) Insert(tr T) error {
	t.mutex.RLock()
	keysForInsert := make([][]byte, 0, len(t.indexes))

	for _, idx := range t.indexes {
		if idx.IndexFilterFunction(tr) {
			keysForInsert = append(keysForInsert, t.compoundKey(idx, tr))
		}
	}
	t.mutex.RUnlock()

	// todo: insert to pebble shall indexes contain pointers to main index or duplicated data?
	data, _ := t.db.Serializer().Serialize(tr)
	for _, key := range keysForInsert {
		err := t.db.Set(key, data, pebble.Sync)
		if err != nil {
			// todo: handle rollback
			return err
		}
	}
	return nil
}

func (t *Table[T]) GetAll() ([]T, error) {
	lowerBound := t.compoundKeyDefaultIndex(DefaultMainIndexID)
	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
	})

	var ret []T
	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.Compare(lowerBound, iter.Key()[:len(lowerBound)]) != 0 {
			break
		}

		var record T
		err := t.db.Serializer().Deserialize(iter.Value(), &record)
		if err != nil {
			return nil, err
		}

		ret = append(ret, record)
	}

	return ret, nil
}

func (t *Table[T]) GetAllForIndex(idxID IndexID, tmpl T) ([]T, error) {
	t.mutex.RLock()
	lowerBound := t.tableKey(t.indexes[idxID], tmpl)
	t.mutex.RUnlock()

	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
	})

	var ret []T
	for iter.First(); iter.Valid(); iter.Next() {
		if bytes.Compare(lowerBound, iter.Key()[:len(lowerBound)]) != 0 {
			break
		}

		var record T
		err := t.db.Serializer().Deserialize(iter.Value(), &record)
		if err != nil {
			return nil, err
		}

		ret = append(ret, record)
	}

	return ret, nil
}

func (t *Table[T]) tableKey(idx *Index[T], tr T) []byte {
	compKey := []byte{byte(t.TableID)}
	compKey = append(compKey, idx.IndexKey(tr)...)
	compKey = append(compKey, []byte("-")...)
	return compKey
}

func (t *Table[T]) compoundKey(idx *Index[T], tr T) []byte {
	compKey := t.tableKey(idx, tr)
	compKey = append(compKey, t.recordKeyFunc(tr)...)
	return compKey
}

func (t *Table[T]) compoundKeyDefaultIndex(indexID IndexID) []byte {
	return []byte{byte(t.TableID), byte(indexID), byte('-')}
}
