package bond

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"golang.org/x/exp/maps"
)

type TableID uint8
type TableRecordKeyFunc[T any] func(t T) []byte

type Table[T any] struct {
	TableID TableID

	db *DB

	recordKeyFunc TableRecordKeyFunc[T]

	mainIndex    *Index[T]
	otherIndexes map[IndexID]*Index[T]

	mutex sync.RWMutex
}

func NewTable[T any](db *DB, id TableID, trkFn TableRecordKeyFunc[T]) *Table[T] {
	return &Table[T]{
		TableID:       id,
		db:            db,
		recordKeyFunc: trkFn,
		mainIndex:     NewIndex[T](MainIndexID, func(t T) []byte { return []byte{} }),
		otherIndexes:  make(map[IndexID]*Index[T]),
		mutex:         sync.RWMutex{},
	}
}

func (t *Table[T]) AddIndexes(idxs []*Index[T], reIndex ...bool) {
	t.mutex.Lock()
	for _, idx := range idxs {
		t.otherIndexes[idx.IndexID] = idx
	}
	t.mutex.Unlock()

	if len(reIndex) > 0 && reIndex[0] {
		// todo: build index
	}
}

func (t *Table[T]) Insert(tr []T) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.otherIndexes)
	t.mutex.RUnlock()

	batch := t.db.NewBatch()
	keysForIndexInsert := make([][]byte, 0, len(t.otherIndexes))

	keyBuffer := make([]byte, 0, 10240)

	for _, r := range tr {
		recordKey := t.recordKeyFunc(r)

		// index keys
		keysForIndexInsert = keysForIndexInsert[:0]
		for _, idx := range t.otherIndexes {
			if idx.IndexFilterFunction(r) {
				keysForIndexInsert = append(keysForIndexInsert,
					KeyEncode(Key{
						TableID:   t.TableID,
						IndexID:   idx.IndexID,
						IndexKey:  idx.IndexKey(r),
						RecordKey: recordKey,
					}),
					keyBuffer[len(keyBuffer):],
				)
			}
		}

		// serialize
		data, err := t.db.Serializer().Serialize(r)
		if err != nil {
			return err
		}

		// insert data
		keyRaw := KeyEncode(Key{
			TableID:   t.TableID,
			IndexID:   MainIndexID,
			IndexKey:  []byte{},
			RecordKey: recordKey,
		}, keyBuffer[len(keyBuffer):])

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

		keyBuffer = keyBuffer[:0]
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		_ = batch.Close()
		return err
	}

	return nil
}

func (t *Table[T]) Query() Query[T] {
	return newQuery[T](t, t.mainIndex)
}

func (t *Table[T]) Scan(tr *[]T) error {
	return t.ScanIndex(t.mainIndex, make([]T, 1)[0], tr)
}

func (t *Table[T]) ScanIndex(i *Index[T], s T, tr *[]T) error {
	return t.ScanIndexForEach(i, s, func(record T) {
		*tr = append(*tr, record)
	})
}

func (t *Table[T]) ScanForEach(f func(t T)) error {
	return t.ScanIndexForEach(t.mainIndex, make([]T, 1)[0], f)
}

func (t *Table[T]) ScanIndexForEach(i *Index[T], s T, f func(t T)) error {
	buffer := make([]byte, 0, 5120)

	prefix := KeyEncode(Key{
		TableID:   t.TableID,
		IndexID:   i.IndexID,
		IndexKey:  i.IndexKey(s),
		RecordKey: []byte{},
	}, buffer[:0])

	iter := t.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})

	var getValue func() error
	if i.IndexID == MainIndexID {
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
			tableKey := KeyEncode(
				KeyDecode(iter.Key()).ToTableKey(),
				buffer[:0],
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
