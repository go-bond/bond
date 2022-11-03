package bond

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
	"golang.org/x/exp/maps"
)

const PrimaryKeyBufferSize = 10240
const IndexKeyBufferSize = 10240
const DataKeyBufferSize = PrimaryKeyBufferSize + IndexKeyBufferSize + 6

const ReindexBatchSize = 10000

type TableID uint8
type TablePrimaryKeyFunc[T any] func(builder KeyBuilder, t T) []byte

func TableUpsertOnConflictReplace[T any](_, new T) T {
	return new
}

func primaryIndexKey[T any](_ KeyBuilder, _ T) []byte { return []byte{} }

type TableInfo interface {
	ID() TableID
	Name() string
	Indexes() []IndexInfo
	EntryType() reflect.Type
}

type TableR[T any] interface {
	TableInfo

	PrimaryIndex() *Index[T]
	SecondaryIndexes() []*Index[T]
	Serializer() Serializer[*T]

	Get(tr T, optBatch ...*pebble.Batch) (T, error)
	Exist(tr T, optBatch ...*pebble.Batch) bool
	Query() Query[T]

	Scan(ctx context.Context, tr *[]T, optBatch ...*pebble.Batch) error
	ScanIndex(ctx context.Context, i *Index[T], s T, tr *[]T, optBatch ...*pebble.Batch) error
	ScanForEach(ctx context.Context, f func(l Lazy[T]) (bool, error), optBatch ...*pebble.Batch) error
	ScanIndexForEach(ctx context.Context, idx *Index[T], s T, f func(t Lazy[T]) (bool, error), optBatch ...*pebble.Batch) error

	NewIter(options *pebble.IterOptions, optBatch ...*pebble.Batch) *pebble.Iterator
}

type TableW[T any] interface {
	AddIndex(idxs []*Index[T], reIndex ...bool) error

	Insert(ctx context.Context, trs []T, optBatch ...*pebble.Batch) error
	Update(ctx context.Context, trs []T, optBatch ...*pebble.Batch) error
	Upsert(ctx context.Context, trs []T, onConflict func(old, new T) T, optBatch ...*pebble.Batch) error
	Delete(ctx context.Context, trs []T, optBatch ...*pebble.Batch) error
}

type TableRW[T any] interface {
	TableR[T]
	TableW[T]
}

type TableOptions[T any] struct {
	DB *DB

	TableID             TableID
	TableName           string
	TablePrimaryKeyFunc TablePrimaryKeyFunc[T]
	Serializer          Serializer[*T]
}

type Table[T any] struct {
	id   TableID
	name string

	db *DB

	primaryKeyFunc TablePrimaryKeyFunc[T]

	primaryIndex     *Index[T]
	secondaryIndexes map[IndexID]*Index[T]

	serializer Serializer[*T]

	mutex sync.RWMutex
}

func NewTable[T any](opt TableOptions[T]) TableRW[T] {
	var serializer Serializer[*T] = &SerializerAnyWrapper[*T]{Serializer: opt.DB.Serializer()}
	if opt.Serializer != nil {
		serializer = opt.Serializer
	}

	// TODO: check if id == 0, and if so, return error that its reserved for bond

	return &Table[T]{
		db:             opt.DB,
		id:             opt.TableID,
		name:           opt.TableName,
		primaryKeyFunc: opt.TablePrimaryKeyFunc,
		primaryIndex: NewIndex(IndexOptions[T]{
			IndexID:        PrimaryIndexID,
			IndexName:      PrimaryIndexName,
			IndexKeyFunc:   primaryIndexKey[T],
			IndexOrderFunc: IndexOrderDefault[T],
		}),
		secondaryIndexes: make(map[IndexID]*Index[T]),
		serializer:       serializer,
		mutex:            sync.RWMutex{},
	}
}

func (t *Table[T]) ID() TableID {
	return t.id
}

func (t *Table[T]) Name() string {
	return t.name
}

func (t *Table[T]) Indexes() []IndexInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	indexInfos := []IndexInfo{t.primaryIndex}
	for _, idx := range t.secondaryIndexes {
		indexInfos = append(indexInfos, idx)
	}

	sort.Slice(indexInfos, func(i, j int) bool {
		return indexInfos[i].ID() < indexInfos[j].ID()
	})

	return indexInfos
}

func (t *Table[T]) EntryType() reflect.Type {
	return reflect.TypeOf(utils.MakeNew[T]())
}

func (t *Table[T]) PrimaryIndex() *Index[T] {
	return t.primaryIndex
}

func (t *Table[T]) SecondaryIndexes() []*Index[T] {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var indexes []*Index[T]
	for _, idx := range t.secondaryIndexes {
		indexes = append(indexes, idx)
	}

	return indexes
}

func (t *Table[T]) Serializer() Serializer[*T] {
	return t.serializer
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
			[]byte{byte(t.id), byte(idx.IndexID)},
			[]byte{byte(t.id), byte(idx.IndexID + 1)}, pebble.Sync)
		if err != nil {
			return fmt.Errorf("failed to delete index: %w", err)
		}
	}

	snap := t.db.NewSnapshot()

	var prefixBuffer [DataKeyBufferSize]byte
	prefix := t.keyPrefix(t.primaryIndex, utils.MakeNew[T](), prefixBuffer[:0])

	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})

	batch := t.db.NewBatch()

	counter := 0
	indexKeysBuffer := make([]byte, 0, (PrimaryKeyBufferSize+IndexKeyBufferSize)*len(idxs))
	indexKeys := make([][]byte, 0, len(t.secondaryIndexes))

	for iter.SeekPrefixGE(prefix); iter.Valid(); iter.Next() {
		var tr T

		err := t.serializer.Deserialize(iter.Value(), &tr)
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

func (t *Table[T]) Insert(ctx context.Context, trs []T, optBatch ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var (
		keyBuffer       [DataKeyBufferSize]byte
		indexKeysBuffer = make([]byte, 0, (PrimaryKeyBufferSize+IndexKeyBufferSize)*len(indexes))
		indexKeys       = make([][]byte, 0, len(t.secondaryIndexes))
	)

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// insert key
		key := t.key(tr, keyBuffer[:0])

		// check if exist
		if t.exist(key, batch) {
			return fmt.Errorf("record: %x already exist", key[_KeyPrefixSplitIndex(key):])
		}

		// serialize
		data, err := t.serializer.Serialize(&tr)
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

func (t *Table[T]) Update(ctx context.Context, trs []T, optBatch ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var (
		keyBuffer      [DataKeyBufferSize]byte
		indexKeyBuffer = make([]byte, DataKeyBufferSize*len(indexes)*2)
	)

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// update key
		key := t.key(tr, keyBuffer[:0])

		// old record
		oldTrData, closer, err := batch.Get(key)
		if err != nil {
			_ = batch.Close()
			return err
		}

		var oldTr T
		err = t.serializer.Deserialize(oldTrData, &oldTr)
		if err != nil {
			_ = batch.Close()
			return err
		}

		_ = closer.Close()

		// serialize
		data, err := t.serializer.Serialize(&tr)
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

func (t *Table[T]) Delete(ctx context.Context, trs []T, optBatch ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var (
		keyBuffer      [DataKeyBufferSize]byte
		indexKeyBuffer = make([]byte, DataKeyBufferSize*len(indexes))
		indexKeys      = make([][]byte, len(indexes))
	)

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

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

func (t *Table[T]) Upsert(ctx context.Context, trs []T, onConflict func(old, new T) T, optBatch ...*pebble.Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch *pebble.Batch
	var externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.NewIndexedBatch()
	}

	var (
		keyBuffer      [DataKeyBufferSize]byte
		indexKeyBuffer = make([]byte, DataKeyBufferSize*len(indexes)*2)

		indexKeys = make([][]byte, 0, len(indexes))
	)

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// update key
		key := t.key(tr, keyBuffer[:0])

		// old record
		var oldTr T
		oldTrData, closer, err := batch.Get(key)
		if err == nil {
			err = t.serializer.Deserialize(oldTrData, &oldTr)
			if err != nil {
				_ = batch.Close()
				return err
			}

			_ = closer.Close()
		}

		// handle upsert
		isUpdate := oldTrData != nil && len(oldTrData) > 0
		if isUpdate {
			tr = onConflict(oldTr, tr)
		}

		// serialize
		data, err := t.serializer.Serialize(&tr)
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
		var (
			toAddIndexKeys    [][]byte
			toRemoveIndexKeys [][]byte
		)

		if isUpdate {
			toAddIndexKeys, toRemoveIndexKeys = t.indexKeysDiff(tr, oldTr, indexes, indexKeyBuffer[:0])
		} else {
			toAddIndexKeys = t.indexKeys(tr, indexes, indexKeyBuffer[:0], indexKeys[:0])
		}

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

func (t *Table[T]) Exist(tr T, optBatch ...*pebble.Batch) bool {
	var batch *pebble.Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
	} else {
		batch = nil
	}

	var keyBuffer [DataKeyBufferSize]byte
	key := t.key(tr, keyBuffer[:0])
	return t.exist(key, batch)
}

func (t *Table[T]) exist(key []byte, batch *pebble.Batch) bool {
	_, closer, err := t.db.getKV(key, batch)
	if err != nil {
		return false
	}

	_ = closer.Close()
	return true
}

func (t *Table[T]) Get(tr T, optBatch ...*pebble.Batch) (T, error) {
	var batch *pebble.Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
	} else {
		batch = nil
	}

	var keyBuffer [DataKeyBufferSize]byte
	key := t.key(tr, keyBuffer[:0])
	return t.get(key, batch)
}

func (t *Table[T]) get(key []byte, batch *pebble.Batch) (T, error) {
	data, closer, err := t.db.getKV(key, batch)
	if err != nil {
		return utils.MakeNew[T](), fmt.Errorf("get failed: %w", err)
	}

	defer func() { _ = closer.Close() }()

	var tr T
	err = t.serializer.Deserialize(data, &tr)
	if err != nil {
		return utils.MakeNew[T](), fmt.Errorf("get failed to deserialize: %w", err)
	}

	return tr, nil
}

func (t *Table[T]) NewIter(options *pebble.IterOptions, optBatch ...*pebble.Batch) *pebble.Iterator {
	if options == nil {
		options = &pebble.IterOptions{}
	}

	selector := _KeyEncode(_Key{TableID: t.id}, nil)
	options.LowerBound = selector

	if len(optBatch) > 0 && optBatch[0] != nil {
		batch := optBatch[0]
		return batch.NewIter(options)
	} else {
		return t.db.NewIter(options)
	}
}

func (t *Table[T]) Query() Query[T] {
	return newQuery(t, t.primaryIndex)
}

func (t *Table[T]) Scan(ctx context.Context, tr *[]T, optBatch ...*pebble.Batch) error {
	return t.ScanIndex(ctx, t.primaryIndex, utils.MakeNew[T](), tr, optBatch...)
}

func (t *Table[T]) ScanIndex(ctx context.Context, i *Index[T], s T, tr *[]T, optBatch ...*pebble.Batch) error {
	return t.ScanIndexForEach(ctx, i, s, func(lazy Lazy[T]) (bool, error) {
		if record, err := lazy.Get(); err == nil {
			*tr = append(*tr, record)
			return true, nil
		} else {
			return false, err
		}
	}, optBatch...)
}

func (t *Table[T]) ScanForEach(ctx context.Context, f func(l Lazy[T]) (bool, error), optBatch ...*pebble.Batch) error {
	return t.ScanIndexForEach(ctx, t.primaryIndex, utils.MakeNew[T](), f, optBatch...)
}

func (t *Table[T]) ScanIndexForEach(ctx context.Context, idx *Index[T], s T, f func(t Lazy[T]) (bool, error), optBatch ...*pebble.Batch) error {
	var prefixBuffer [DataKeyBufferSize]byte

	selector := t.indexKey(s, idx, prefixBuffer[:0])

	var iter *pebble.Iterator
	var batch *pebble.Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
		iter = batch.NewIter(&pebble.IterOptions{
			LowerBound: selector,
		})
	} else {
		iter = t.db.NewIter(&pebble.IterOptions{
			LowerBound: selector,
		})
	}

	var getValue func() (T, error)
	var keyBuffer [DataKeyBufferSize]byte
	if idx.IndexID == PrimaryIndexID {
		getValue = func() (T, error) {
			var record T
			if err := t.serializer.Deserialize(iter.Value(), &record); err == nil {
				return record, nil
			} else {
				return utils.MakeNew[T](), err
			}
		}
	} else {
		getValue = func() (T, error) {
			tableKey := _KeyBytesToDataKeyBytes(
				iter.Key(),
				keyBuffer[:0],
			)

			valueData, closer, err := t.db.getKV(tableKey, batch)
			if err != nil {
				return utils.MakeNew[T](), err
			}

			defer func() { _ = closer.Close() }()

			var record T
			if err = t.serializer.Deserialize(valueData, &record); err == nil {
				return record, nil
			} else {
				return utils.MakeNew[T](), err
			}
		}
	}

	for iter.SeekPrefixGE(selector); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		if cont, err := f(Lazy[T]{getValue}); !cont || err != nil {
			break
		} else {
			if err != nil {
				_ = iter.Close()
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
		TableID:    t.id,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: primaryKey,
	}, buff[len(primaryKey):len(primaryKey)])
}

func (t *Table[T]) keyPrefix(idx *Index[T], s T, buff []byte) []byte {
	indexKey := idx.IndexKeyFunction(NewKeyBuilder(buff[:0]), s)

	return _KeyEncode(_Key{
		TableID:    t.id,
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
		IndexOrder{keyBuilder: NewKeyBuilder(indexKeyPart[len(indexKeyPart):])}, tr,
	).Bytes()

	return _KeyEncode(_Key{
		TableID:    t.id,
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
			if bytes.Equal(newKey, oldKey) {
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
			if bytes.Equal(oldKey, newKey) {
				found = true
			}
		}

		if !found {
			toRemove = append(toRemove, oldKey)
		}
	}

	return
}
