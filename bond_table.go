package bond

import (
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
			keysForInsert = append(keysForInsert, t.tableKey(idx, tr))
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

func (t *Table[T]) Query() Query[T] {
	t.mutex.RLock()
	mainIndex := t.indexes[0]
	t.mutex.RUnlock()

	return newQuery[T](t, mainIndex)
}

func (t *Table[T]) Scan(tr *[]T) error {
	t.mutex.RLock()
	mainIndex := t.indexes[0]
	t.mutex.RUnlock()

	return t.ScanIndex(tr, mainIndex, make([]T, 1)[0])
}

func (t *Table[T]) ScanIndex(tr *[]T, i *Index[T], s T) error {
	return t.ScanIndexForEach(i, s, func(record T) {
		*tr = append(*tr, record)
	})
}

func (t *Table[T]) ScanForEach(f func(t T)) error {
	t.mutex.RLock()
	mainIndex := t.indexes[0]
	t.mutex.RUnlock()

	return t.ScanIndexForEach(mainIndex, make([]T, 1)[0], f)
}

func (t *Table[T]) ScanIndexForEach(i *Index[T], s T, f func(t T)) error {
	prefix := t.indexKey(i, s)

	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound:      prefix,
		RangeKeyMasking: pebble.RangeKeyMasking{},
	})

	iter.SeekPrefixGE(prefix)

	var getValue func() error
	if i.IndexID == DefaultMainIndexID {
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
		getValue = func() error { return nil }
	}

	for iter.First(); iter.Valid(); iter.Next() {
		err := getValue()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table[T]) indexKey(idx *Index[T], tr T) []byte {
	compKey := []byte{byte(t.TableID)}
	compKey = append(compKey, idx.IndexKey(tr)...)
	compKey = append(compKey, []byte("-")...)
	return compKey
}

func (t *Table[T]) tableKey(idx *Index[T], tr T) []byte {
	compKey := t.indexKey(idx, tr)
	compKey = append(compKey, t.recordKeyFunc(tr)...)
	return compKey
}

func (t *Table[T]) fromIndexKeyToTableKey(idxKey []byte) []byte {
	return []byte{byte(t.TableID), byte(DefaultMainIndexID), byte('-')}
}
