package playground

import (
	"encoding/json"
	"sync"

	"github.com/cockroachdb/pebble"
)

type TableID uint8
type TableRecordKeyFunc[T any] func(t T) []byte

type IndexID uint8
type IndexKeyFunction[T any] func(t T) []byte
type IndexFilterFunction[T any] func(t T) bool

const DefaultMainIndexID = IndexID(0)

type Index[T any] struct {
	IndexID             IndexID
	IndexKeyFunction    IndexKeyFunction[T]
	IndexFilterFunction IndexFilterFunction[T]
}

func NewIndex[T any](idxID IndexID, idxFn IndexKeyFunction[T], idxFFn ...IndexFilterFunction[T]) *Index[T] {
	idx := &Index[T]{
		IndexID:          idxID,
		IndexKeyFunction: idxFn,
		IndexFilterFunction: func(t T) bool {
			return true
		},
	}

	if len(idxFFn) > 0 {
		idx.IndexFilterFunction = idxFFn[0]
	}

	return idx
}

func (i *Index[T]) IndexKey(t T) []byte {
	return append([]byte{byte(i.IndexID)}, i.IndexKeyFunction(t)...)
}

type Table[T any] struct {
	TableID TableID

	db *pebble.DB

	recordKeyFunc TableRecordKeyFunc[T]

	indexes map[IndexID]*Index[T]

	mutex sync.RWMutex
}

func NewTable[T any](db *pebble.DB, id TableID, trkFn TableRecordKeyFunc[T]) *Table[T] {
	t := &Table[T]{
		TableID:       id,
		db:            db,
		recordKeyFunc: trkFn,
		indexes:       make(map[IndexID]*Index[T]),
		mutex:         sync.RWMutex{},
	}

	t.AddIndex(
		NewIndex[T](DefaultMainIndexID, func(t T) []byte { return []byte{} }),
		false,
	)

	return t
}

func (t *Table[T]) AddIndex(i *Index[T], reIndex ...bool) {
	t.mutex.Lock()
	t.indexes[i.IndexID] = i
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
	data, _ := json.Marshal(tr)
	for _, key := range keysForInsert {
		err := t.db.Set(key, data, pebble.Sync)
		if err != nil {
			// todo: handle rollback
			return err
		}
	}
	return nil
}

func (t *Table[T]) GetFromIndex(idxID IndexID, tmpl T) []T {
	return nil
}

func (t *Table[T]) GetAll() []T {
	return nil
}

func (t *Table[T]) compoundKey(idx *Index[T], tr T) []byte {
	compKey := []byte{byte(t.TableID)}
	compKey = append(compKey, idx.IndexKey(tr)...)
	compKey = append(compKey, []byte("-")...)
	compKey = append(compKey, t.recordKeyFunc(tr)...)
	return compKey
}
