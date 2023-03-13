package bond

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
	"golang.org/x/exp/maps"
)

var _keyBufferPool = utils.NewPreAllocatedPool[any](func() any {
	return make([]byte, 0, KeyBufferInitialSize)
}, 5*persistentBatchSize) // 51 MB

var _multiKeyBufferPool = utils.NewPreAllocatedPool[any](func() any {
	return make([]byte, 0, KeyBufferInitialSize*1000)
}, 25) // 51 MB

var _byteArraysPool = utils.NewPreAllocatedPool[any](func() any {
	return make([][]byte, 0, persistentBatchSize)
}, 500) // 20 MB

var _valueBufferPool = utils.NewPreAllocatedPool[any](func() any {
	return make([]byte, 0, ValueBufferInitialSize)
}, 100*DefaultScanPrefetchSize) // 20 MB

func _valueBuffersGet(numOfKeys int) [][]byte {
	keys := _byteArraysPool.Get().([][]byte)[:0]
	if cap(keys) < numOfKeys {
		keys = make([][]byte, 0, numOfKeys)
	}

	for i := 0; i < numOfKeys; i++ {
		keys = append(keys, _valueBufferPool.Get().([]byte)[:0])
	}
	return keys
}

func _valueBufferPoolCloser(values [][]byte) {
	for _, value := range values {
		_valueBufferPool.Put(value[:0])
	}
	_byteArraysPool.Put(values[:0])
}

func _keyBuffersGet(numOfKeys int) [][]byte {
	keys := _byteArraysPool.Get().([][]byte)[:0]
	if cap(keys) < numOfKeys {
		keys = make([][]byte, 0, numOfKeys)
	}

	for i := 0; i < numOfKeys; i++ {
		keys = append(keys, _keyBufferPool.Get().([]byte)[:0])
	}
	return keys
}

func _keyBufferClose(keys [][]byte) {
	for _, key := range keys {
		_keyBufferPool.Put(key[:0])
	}
	_byteArraysPool.Put(keys[:0])
}

var _indexKeyValue = []byte{}

const KeyBufferInitialSize = 2048
const ValueBufferInitialSize = 2048
const ReindexBatchSize = 10000

const DefaultScanPrefetchSize = 100

const persistentBatchSize = 5000

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

type TableGetter[T any] interface {
	Get(tr T, optBatch ...Batch) (T, error)
}

type TableExistChecker[T any] interface {
	Exist(tr T, optBatch ...Batch) bool
}

type TableQuerier[T any] interface {
	Query() Query[T]
}

type TableScanner[T any] interface {
	Scan(ctx context.Context, tr *[]T, optBatch ...Batch) error
	ScanIndex(ctx context.Context, i *Index[T], s T, tr *[]T, optBatch ...Batch) error
	ScanForEach(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[T]) (bool, error), optBatch ...Batch) error
	ScanIndexForEach(ctx context.Context, idx *Index[T], s T, f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), optBatch ...Batch) error
}

type TableIterationer[T any] interface {
	Iter(opt *IterOptions, optBatch ...Batch) Iterator
}

type TableReader[T any] interface {
	TableInfo

	PrimaryIndex() *Index[T]
	SecondaryIndexes() []*Index[T]
	Serializer() Serializer[*T]

	TableGetter[T]
	TableExistChecker[T]
	TableQuerier[T]

	TableScanner[T]
	TableIterationer[T]
}

type TableInserter[T any] interface {
	Insert(ctx context.Context, trs []T, optBatch ...Batch) error
}

type TableUpdater[T any] interface {
	Update(ctx context.Context, trs []T, optBatch ...Batch) error
}

type TableUpserter[T any] interface {
	Upsert(ctx context.Context, trs []T, onConflict func(old, new T) T, optBatch ...Batch) error
}

type TableDeleter[T any] interface {
	Delete(ctx context.Context, trs []T, optBatch ...Batch) error
}

type TableWriter[T any] interface {
	AddIndex(idxs []*Index[T], reIndex ...bool) error

	TableInserter[T]
	TableUpdater[T]
	TableUpserter[T]
	TableDeleter[T]
}

type Table[T any] interface {
	TableReader[T]
	TableWriter[T]
}

type TableOptions[T any] struct {
	DB DB

	TableID             TableID
	TableName           string
	TablePrimaryKeyFunc TablePrimaryKeyFunc[T]
	Serializer          Serializer[*T]

	ScanPrefetchSize int

	Filter Filter
}

type _table[T any] struct {
	id   TableID
	name string

	db DB

	primaryKeyFunc TablePrimaryKeyFunc[T]

	primaryIndex     *Index[T]
	secondaryIndexes map[IndexID]*Index[T]

	dataKeySpaceStart []byte
	dataKeySpaceEnd   []byte

	scanPrefetchSize int

	serializer *SerializerAnyWrapper[*T]

	filter Filter

	mutex sync.RWMutex
}

func NewTable[T any](opt TableOptions[T]) Table[T] {
	var serializer = &SerializerAnyWrapper[*T]{Serializer: opt.DB.Serializer()}
	if opt.Serializer != nil {
		serializer = &SerializerAnyWrapper[*T]{Serializer: opt.Serializer.(Serializer[any])}
	}

	scanPrefetchSize := DefaultScanPrefetchSize
	if opt.ScanPrefetchSize != 0 {
		scanPrefetchSize = opt.ScanPrefetchSize
	}

	// TODO: check if id == 0, and if so, return error that its reserved for bond

	table := &_table[T]{
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
		secondaryIndexes:  make(map[IndexID]*Index[T]),
		dataKeySpaceStart: []byte{byte(opt.TableID), 0x00, 0x00, 0x00, 0x00, 0x00},
		dataKeySpaceEnd:   []byte{byte(opt.TableID), 0x01, 0x00, 0x00, 0x00, 0x00},
		scanPrefetchSize:  scanPrefetchSize,
		serializer:        serializer,
		filter:            opt.Filter,
		mutex:             sync.RWMutex{},
	}

	return table
}

func (t *_table[T]) ID() TableID {
	return t.id
}

func (t *_table[T]) Name() string {
	return t.name
}

func (t *_table[T]) Indexes() []IndexInfo {
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

func (t *_table[T]) EntryType() reflect.Type {
	return reflect.TypeOf(utils.MakeNew[T]())
}

func (t *_table[T]) PrimaryIndex() *Index[T] {
	return t.primaryIndex
}

func (t *_table[T]) SecondaryIndexes() []*Index[T] {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var indexes []*Index[T]
	for _, idx := range t.secondaryIndexes {
		indexes = append(indexes, idx)
	}

	return indexes
}

func (t *_table[T]) Serializer() Serializer[*T] {
	return t.serializer
}

func (t *_table[T]) AddIndex(idxs []*Index[T], reIndex ...bool) error {
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

func (t *_table[T]) reindex(idxs []*Index[T]) error {
	idxsMap := make(map[IndexID]*Index[T])
	for _, idx := range idxs {
		idxsMap[idx.IndexID] = idx
		err := t.db.DeleteRange(
			[]byte{byte(t.id), byte(idx.IndexID)},
			[]byte{byte(t.id), byte(idx.IndexID + 1)}, Sync)
		if err != nil {
			return fmt.Errorf("failed to delete index: %w", err)
		}
	}

	var prefixBuffer [KeyBufferInitialSize]byte
	prefix := t.keyPrefix(t.primaryIndex, utils.MakeNew[T](), prefixBuffer[:0])
	selectorEnd := big.NewInt(0).Add(big.NewInt(0).SetBytes(prefix[0:_KeyPrefixSplitIndex(prefix)]), big.NewInt(1)).Bytes()

	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: selectorEnd,
		},
	})

	batch := t.db.Batch()
	defer func() {
		_ = batch.Close()
	}()

	counter := 0
	indexKeysBuffer := make([]byte, 0, (KeyBufferInitialSize)*len(idxs))
	indexKeys := make([][]byte, 0, len(t.secondaryIndexes))

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		var tr T

		err := t.serializer.Deserialize(iter.Value(), &tr)
		if err != nil {
			return fmt.Errorf("failed to deserialize during reindexing: %w", err)
		}

		indexKeys = t.indexKeys(tr, idxsMap, indexKeysBuffer[:0], indexKeys[:0], keyPartsBuffer[:0])

		for _, indexKey := range indexKeys {
			err = batch.Set(indexKey, _indexKeyValue, Sync)
			if err != nil {
				return fmt.Errorf("failed to set index key during reindexing: %w", err)
			}
		}

		counter++
		if counter >= ReindexBatchSize {
			counter = 0

			err = batch.Commit(Sync)
			if err != nil {
				return fmt.Errorf("failed to commit reindex batch: %w", err)
			}

			batch = t.db.Batch()
		}
	}

	err := batch.Commit(Sync)
	if err != nil {
		return fmt.Errorf("failed to commit reindex batch: %w", err)
	}

	_ = iter.Close()

	return nil
}

func (t *_table[T]) Insert(ctx context.Context, trs []T, optBatch ...Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var (
		batch         Batch
		batchCtx      context.Context
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch()
		defer batch.Close()
	}
	batchCtx = ContextWithBatch(ctx, batch)

	var (
		indexKeysBuffer = _multiKeyBufferPool.Get().([]byte)[:0]
		indexKeys       = _byteArraysPool.Get().([][]byte)[:0]
	)
	defer _multiKeyBufferPool.Put(indexKeysBuffer[:0])
	defer _byteArraysPool.Put(indexKeys[:0])

	// key buffers
	keysBuffer := _keyBuffersGet(minInt(len(trs), persistentBatchSize))
	defer _keyBufferClose(keysBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	// value
	value := _valueBufferPool.Get().([]byte)[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer _valueBufferPool.Put(value[:0])

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	err := batched[T](trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer, keyPartsBuffer[:0])

		// order keys
		keyOrder := t.sortKeys(keys)

		// iter
		iter := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: keys[0],
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batch)
		defer iter.Close()

		// process rows
		for i, key := range keys {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context done: %w", ctx.Err())
			default:
			}

			// check if exist
			if t.keyDuplicate(i, keys) || t.exist(key, batch, iter) {
				return fmt.Errorf("record: %x already exist", key[_KeyPrefixSplitIndex(key):])
			}

			// serialize
			tr := trs[keyOrder[i]]
			data, err := serialize(&tr)
			if err != nil {
				return err
			}

			err = batch.Set(key, data, Sync)
			if err != nil {
				return err
			}

			// index keys
			indexKeys = t.indexKeys(tr, indexes, indexKeysBuffer[:0], indexKeys[:0], keyPartsBuffer[:0])

			// update indexes
			for _, indexKey := range indexKeys {
				err = batch.Set(indexKey, _indexKeyValue, Sync)
				if err != nil {
					return err
				}
			}

			if t.filter != nil {
				t.filter.Add(batchCtx, key)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !externalBatch {
		err = batch.Commit(Sync)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *_table[T]) Update(ctx context.Context, trs []T, optBatch ...Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var (
		batch         Batch
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch()
		defer batch.Close()
	}

	var (
		indexKeysBuffer = _multiKeyBufferPool.Get().([]byte)[:0]
	)
	defer _multiKeyBufferPool.Put(indexKeysBuffer[:0])

	// key buffers
	keysBuffer := _keyBuffersGet(minInt(len(trs), persistentBatchSize))
	defer _keyBufferClose(keysBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	// value
	value := _valueBufferPool.Get().([]byte)[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer _valueBufferPool.Put(value[:0])

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	err := batched[T](trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer, keyPartsBuffer[:0])

		// order keys
		keyOrder := t.sortKeys(keys)

		// iter
		iter := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: keys[0],
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batch)
		defer iter.Close()

		for i, key := range keys {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context done: %w", ctx.Err())
			default:
			}

			// skip this records since the next record updating the
			// same primary key.
			if i < len(keys)-1 && t.keyDuplicate(i+1, keys) {
				continue
			}

			if !iter.SeekGE(key) || !bytes.Equal(iter.Key(), key) {
				return fmt.Errorf("record: %x not found", key[_KeyPrefixSplitIndex(key):])
			}

			var oldTr T
			err := t.serializer.Deserialize(iter.Value(), &oldTr)
			if err != nil {
				return err
			}

			tr := trs[keyOrder[i]]

			// serialize
			data, err := serialize(&tr)
			if err != nil {
				return err
			}

			// update entry
			err = batch.Set(key, data, Sync)
			if err != nil {
				return err
			}

			// indexKeys to add and remove
			toAddIndexKeys, toRemoveIndexKeys := t.indexKeysDiff(tr, oldTr, indexes, indexKeysBuffer[:0], keyPartsBuffer[:0])

			// update indexes
			for _, indexKey := range toAddIndexKeys {
				err = batch.Set(indexKey, _indexKeyValue, Sync)
				if err != nil {
					return err
				}
			}

			for _, indexKey := range toRemoveIndexKeys {
				err = batch.Delete(indexKey, Sync)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !externalBatch {
		err = batch.Commit(Sync)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *_table[T]) Delete(ctx context.Context, trs []T, optBatch ...Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var (
		batch         Batch
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch()
		defer batch.Close()
	}

	var (
		keyBuffer      = _keyBufferPool.Get().([]byte)[:0]
		indexKeyBuffer = _multiKeyBufferPool.Get().([]byte)[:0]
		indexKeys      = _byteArraysPool.Get().([][]byte)[:0]
	)
	defer _keyBufferPool.Put(keyBuffer[:0])
	defer _multiKeyBufferPool.Put(indexKeyBuffer[:0])
	defer _byteArraysPool.Put(indexKeys[:0])

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		var key = t.key(tr, keyBuffer[:0], keyPartsBuffer[:0])
		indexKeys = t.indexKeys(tr, indexes, indexKeyBuffer[:0], indexKeys[:0], keyPartsBuffer[:0])

		err := batch.Delete(key, Sync)
		if err != nil {
			return err
		}

		for _, indexKey := range indexKeys {
			err = batch.Delete(indexKey, Sync)
			if err != nil {
				return err
			}
		}
	}

	if !externalBatch {
		err := batch.Commit(Sync)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *_table[T]) Upsert(ctx context.Context, trs []T, onConflict func(old, new T) T, optBatch ...Batch) error {
	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var (
		batch         Batch
		batchCtx      context.Context
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch()
		defer batch.Close()
	}
	batchCtx = ContextWithBatch(ctx, batch)

	var (
		indexKeysBuffer = _multiKeyBufferPool.Get().([]byte)[:0]
		indexKeys       = _byteArraysPool.Get().([][]byte)[:0]
	)
	defer _multiKeyBufferPool.Put(indexKeysBuffer[:0])
	defer _byteArraysPool.Put(indexKeys[:0])

	// key buffers
	keysBuffer := _keyBuffersGet(minInt(len(trs), persistentBatchSize))
	defer _keyBufferClose(keysBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	// value
	value := _valueBufferPool.Get().([]byte)[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer _valueBufferPool.Put(value[:0])

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	err := batched[T](trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer, keyPartsBuffer)

		// order keys
		keyOrder := t.sortKeys(keys)

		// iter
		iter := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: keys[0],
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batch)
		defer iter.Close()

		for i := 0; i < len(keys); {
			tr := trs[keyOrder[i]]
			select {
			case <-ctx.Done():
				return fmt.Errorf("context done: %w", ctx.Err())
			default:
			}

			// update key
			key := keys[i]

			// old record
			var (
				oldTr    T
				isUpdate bool
				err      error
			)
			if t.exist(key, batch, iter) {
				err := t.serializer.Deserialize(iter.Value(), &oldTr)
				if err != nil {
					return err
				}
				isUpdate = true
			}

			// handle upsert
			if isUpdate {
				tr = onConflict(oldTr, tr)
			}

			// apply conficts recursively if duplicate exist
			for i < len(keys)-1 && t.keyDuplicate(i+1, keys) {
				oldTr = tr
				i++
				tr = onConflict(oldTr, trs[i])
				continue
			}

			// serialize
			data, err := serialize(&tr)
			if err != nil {
				return err
			}

			// update entry
			err = batch.Set(key, data, Sync)
			if err != nil {
				return err
			}

			// indexKeys to add and remove
			var (
				toAddIndexKeys    [][]byte
				toRemoveIndexKeys [][]byte
			)

			if isUpdate {
				toAddIndexKeys, toRemoveIndexKeys = t.indexKeysDiff(tr, oldTr, indexes, indexKeysBuffer[:0], keyPartsBuffer)
			} else {
				toAddIndexKeys = t.indexKeys(tr, indexes, indexKeysBuffer[:0], indexKeys[:0], keyPartsBuffer)
			}

			// update indexes
			for _, indexKey := range toAddIndexKeys {
				err = batch.Set(indexKey, _indexKeyValue, Sync)
				if err != nil {
					return err
				}
			}

			for _, indexKey := range toRemoveIndexKeys {
				err = batch.Delete(indexKey, Sync)
				if err != nil {
					return err
				}
			}

			if t.filter != nil && !isUpdate {
				t.filter.Add(batchCtx, key)
			}
			i++
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !externalBatch {
		err = batch.Commit(Sync)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *_table[T]) Exist(tr T, optBatch ...Batch) bool {
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
	} else {
		batch = nil
	}

	keyBuffer := _keyBufferPool.Get().([]byte)[:0]
	defer _keyBufferPool.Put(keyBuffer[:0])

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	key := t.key(tr, keyBuffer[:0], keyPartsBuffer[:0])

	bCtx := ContextWithBatch(context.Background(), batch)
	if t.filter != nil && !t.filter.MayContain(bCtx, key) {
		return false
	}

	_, closer, err := t.db.Get(key, batch)
	if err != nil {
		return false
	}

	_ = closer.Close()
	return true
}

func (t *_table[T]) exist(key []byte, batch Batch, iter Iterator) bool {
	if t.filter != nil && !t.filter.MayContain(context.TODO(), key) {
		return false
	}

	if iter == nil {
		iter = t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: key,
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batch)
		defer iter.Close()
	}

	return iter.SeekGE(key) && bytes.Equal(iter.Key(), key)
}

func (t *_table[T]) Get(tr T, optBatch ...Batch) (T, error) {
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
	} else {
		batch = nil
	}

	keyBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	key := t.key(tr, keyBuffer[:0], keyPartsBuffer[:0])

	bCtx := ContextWithBatch(context.Background(), batch)
	if t.filter != nil && !t.filter.MayContain(bCtx, key) {
		return utils.MakeNew[T](), fmt.Errorf("not found")
	}

	record, closer, err := t.db.Get(key, batch)
	if err != nil {
		return utils.MakeNew[T](), fmt.Errorf("not found")
	}
	defer closer.Close()

	var rtr T
	err = t.serializer.Deserialize(record, &rtr)
	if err != nil {
		return utils.MakeNew[T](), fmt.Errorf("get failed to deserialize: %w", err)
	}

	return rtr, nil
}

func (t *_table[T]) get(keys [][]byte, batch Batch, values [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	// sort keys so we get data from db efficiently
	originalOrder := t.sortKeys(keys)

	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: keys[0],
			UpperBound: t.dataKeySpaceEnd,
		},
	}, batch)
	defer iter.Close()

	for i := 0; i < len(keys); i++ {
		if !iter.SeekGE(keys[i]) || !bytes.Equal(iter.Key(), keys[i]) {
			return nil, fmt.Errorf("not found")
		}

		iterValue := iter.Value()
		value := values[i][:0]
		value = append(value, iterValue...)

		values[i] = value
	}

	// resize values
	values = values[:len(keys)]

	// restore original order on values
	t.reorderValues(values, originalOrder)

	return values, nil
}

func (t *_table[T]) Iter(opt *IterOptions, optBatch ...Batch) Iterator {
	if opt == nil {
		opt = &IterOptions{}
	}

	lower := KeyEncode(Key{TableID: t.id}, nil)
	upper := KeyEncode(Key{TableID: t.id + 1}, nil)
	opt.LowerBound = lower
	opt.UpperBound = upper

	if len(optBatch) > 0 && optBatch[0] != nil {
		batch := optBatch[0]
		return batch.Iter(opt)
	} else {
		return t.db.Iter(opt)
	}
}

func (t *_table[T]) Query() Query[T] {
	return newQuery(t, t.primaryIndex)
}

func (t *_table[T]) Scan(ctx context.Context, tr *[]T, optBatch ...Batch) error {
	return t.ScanIndex(ctx, t.primaryIndex, utils.MakeNew[T](), tr, optBatch...)
}

func (t *_table[T]) ScanIndex(ctx context.Context, i *Index[T], s T, tr *[]T, optBatch ...Batch) error {
	return t.ScanIndexForEach(ctx, i, s, func(keyBytes KeyBytes, lazy Lazy[T]) (bool, error) {
		if record, err := lazy.Get(); err == nil {
			*tr = append(*tr, record)
			return true, nil
		} else {
			return false, err
		}
	}, optBatch...)
}

func (t *_table[T]) ScanForEach(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[T]) (bool, error), optBatch ...Batch) error {
	return t.ScanIndexForEach(ctx, t.primaryIndex, utils.MakeNew[T](), f, optBatch...)
}

func (t *_table[T]) ScanIndexForEach(ctx context.Context, idx *Index[T], s T, f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), optBatch ...Batch) error {
	if idx.IndexID == PrimaryIndexID {
		return t.scanForEachPrimaryIndex(ctx, idx, s, f, optBatch...)
	} else {
		return t.scanForEachSecondaryIndex(ctx, idx, s, f, optBatch...)
	}
}

func (t *_table[T]) scanForEachPrimaryIndex(ctx context.Context, idx *Index[T], s T, f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), optBatch ...Batch) error {
	prefixBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(prefixBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	selector := t.indexKey(s, idx, prefixBuffer[:0], keyPartsBuffer[:0])
	selectorEnd := _keyBufferPool.Get().([]byte)[:0]
	selectorEnd = keySuccessor(selectorEnd, selector[0:_KeyPrefixSplitIndex(selector)])
	defer _keyBufferPool.Put(selectorEnd[:0])

	var iter Iterator
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
		iter = batch.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
				UpperBound: selectorEnd,
			},
		})
	} else {
		iter = t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
				UpperBound: selectorEnd,
			},
		})
	}

	getValue := func() (T, error) {
		var record T
		if err := t.serializer.Deserialize(iter.Value(), &record); err == nil {
			return record, nil
		} else {
			return utils.MakeNew[T](), err
		}
	}
	for iter.SeekGE(selector); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			_ = iter.Close()
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		if cont, err := f(iter.Key(), Lazy[T]{getValue}); !cont || err != nil {
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

func (t *_table[T]) scanForEachSecondaryIndex(ctx context.Context, idx *Index[T], s T, f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), optBatch ...Batch) error {
	prefixBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(prefixBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	selector := t.indexKey(s, idx, prefixBuffer[:0], keyPartsBuffer[:0])
	selectorEnd := _keyBufferPool.Get().([]byte)[:0]
	selectorEnd = keySuccessor(selectorEnd, selector[0:_KeyPrefixSplitIndex(selector)])
	defer _keyBufferPool.Put(selectorEnd[:0])

	var iter Iterator
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
		iter = batch.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
				UpperBound: selectorEnd,
			},
		})
	} else {
		iter = t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
				UpperBound: selectorEnd,
			},
		})
	}

	keys := _byteArraysPool.Get().([][]byte)[:0]
	indexKeys := _byteArraysPool.Get().([][]byte)[:0]
	multiKeyBuffer := _multiKeyBufferPool.Get().([]byte)[:0]
	valuesBuffer := _valueBuffersGet(t.scanPrefetchSize)
	defer _byteArraysPool.Put(keys[:0])
	defer _byteArraysPool.Put(indexKeys[:0])
	defer _multiKeyBufferPool.Put(multiKeyBuffer[:0])
	defer _valueBufferPoolCloser(valuesBuffer)

	var prefetchedValues [][]byte
	var prefetchedValuesIndex int

	var prefetchCloser func()
	defer func() {
		if prefetchCloser != nil {
			prefetchCloser()
			prefetchCloser = nil
		}
	}()

	getPrefetchedValue := func() (T, error) {
		var rtr T
		err := t.serializer.Deserialize(prefetchedValues[prefetchedValuesIndex], &rtr)
		if err != nil {
			return utils.MakeNew[T](), fmt.Errorf("get failed to deserialize: %w", err)
		}

		prefetchedValuesIndex++
		return rtr, nil
	}

	prefetchAndGetValue := func() (T, error) {
		// prefetch the required data keys.
		next := multiKeyBuffer
		for iter.Valid() {
			iterKey := iter.Key()

			indexKey := append(next[:0], iterKey...)
			indexKeys = append(indexKeys, indexKey)
			next = indexKey[len(indexKey):]

			key := KeyBytes(iterKey).ToDataKeyBytes(next[:0])
			keys = append(keys, key)

			next = key[len(key):]
			if len(keys) < t.scanPrefetchSize {
				iter.Next()
				continue
			}
			break
		}

		var err error
		prefetchedValues, err = t.get(keys, batch, valuesBuffer)
		if err != nil {
			return utils.MakeNew[T](), err
		}

		return getPrefetchedValue()
	}

	for iter.SeekGE(selector); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			_ = iter.Close()
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		cont, err := f(iter.Key(), Lazy[T]{GetFunc: prefetchAndGetValue})
		if !cont || err != nil {
			_ = iter.Close()
			return err
		}

		// iterate from prefetched entire if exists.
		for prefetchedValuesIndex < len(prefetchedValues) {
			cont, err = f(indexKeys[prefetchedValuesIndex], Lazy[T]{GetFunc: getPrefetchedValue})
			if !cont || err != nil {
				_ = iter.Close()
				return err
			}
		}

		prefetchedValuesIndex = 0
		prefetchedValues = nil
		if prefetchCloser != nil {
			prefetchCloser()
			prefetchCloser = nil
		}

		keys = keys[:0]
		indexKeys = indexKeys[:0]
		multiKeyBuffer = multiKeyBuffer[:0]
	}

	return iter.Close()
}

func (t *_table[T]) sortKeys(keys [][]byte) []int {
	keyOrder := utils.ArrayN(len(keys))
	sort.Sort(&utils.SortShim{
		Length: len(keys),
		SwapFn: func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
			keyOrder[i], keyOrder[j] = keyOrder[j], keyOrder[i]
		},
		LessFn: func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		},
	})
	return keyOrder
}

func (t *_table[T]) reorderValues(values [][]byte, keyOrder []int) {
	sort.Sort(&utils.SortShim{
		Length: len(values),
		SwapFn: func(i, j int) {
			values[i], values[j] = values[j], values[i]
			keyOrder[i], keyOrder[j] = keyOrder[j], keyOrder[i]
		},
		LessFn: func(i, j int) bool {
			return keyOrder[i] < keyOrder[j]
		},
	})
}

func (t *_table[T]) key(tr T, buff []byte, keyPartsBuffer []byte) []byte {
	var primaryKey = t.primaryKeyFunc(NewKeyBuilder(keyPartsBuffer[:0]), tr)

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    PrimaryIndexID,
		Index:      nil,
		IndexOrder: nil,
		PrimaryKey: primaryKey,
	}, buff[:0])
}

func (t *_table[T]) keysExternal(trs []T, keys [][]byte, keyPartsBuffer []byte) [][]byte {
	retKeys := keys[:len(trs)]
	for i, tr := range trs {
		key := t.key(tr, retKeys[i][:0], keyPartsBuffer)
		retKeys[i] = key
	}
	return retKeys
}

func (t *_table[T]) keyDuplicate(index int, keys [][]byte) bool {
	return index > 0 && bytes.Equal(keys[index], keys[index-1])
}

func (t *_table[T]) keyPrefix(idx *Index[T], s T, buff []byte) []byte {
	indexKey := idx.IndexKeyFunction(NewKeyBuilder(buff[:0]), s)

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    idx.IndexID,
		Index:      indexKey,
		IndexOrder: nil,
		PrimaryKey: nil,
	}, indexKey[len(indexKey):])
}

func keySuccessor(dst, src []byte) []byte {
	dst = append(dst, src...)
	for i := len(src) - 1; i > 0; i-- {
		if dst[i] != 0xFF {
			dst[i]++
			return dst
		}
	}
	return dst
}

func (t *_table[T]) indexKey(tr T, idx *Index[T], buff []byte, keyPartsBuffer []byte) []byte {
	primaryKey := t.primaryKeyFunc(NewKeyBuilder(keyPartsBuffer[:0]), tr)
	indexKeyPart := idx.IndexKeyFunction(NewKeyBuilder(primaryKey[len(primaryKey):]), tr)
	orderKeyPart := idx.IndexOrderFunction(
		IndexOrder{keyBuilder: NewKeyBuilder(indexKeyPart[len(indexKeyPart):])}, tr,
	).Bytes()

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    idx.IndexID,
		Index:      indexKeyPart,
		IndexOrder: orderKeyPart,
		PrimaryKey: primaryKey,
	}, buff[:0])
}

func (t *_table[T]) indexKeys(tr T, idxs map[IndexID]*Index[T], buff []byte, indexKeysBuff [][]byte, keyPartsBuffer []byte) [][]byte {
	indexKeys := indexKeysBuff[:0]

	for _, idx := range idxs {
		if idx.IndexFilterFunction(tr) {
			indexKey := t.indexKey(tr, idx, buff, keyPartsBuffer)
			indexKeys = append(indexKeys, indexKey)
			buff = indexKey[len(indexKey):]
		}
	}
	return indexKeys
}

func (t *_table[T]) indexKeysDiff(newTr T, oldTr T, idxs map[IndexID]*Index[T], buff []byte, keyPartsBuffer []byte) (toAdd [][]byte, toRemove [][]byte) {
	newTrKeys := t.indexKeys(newTr, idxs, buff[:0], [][]byte{}, keyPartsBuffer)
	if len(newTrKeys) != 0 {
		buff = newTrKeys[len(newTrKeys)-1]
		buff = buff[len(buff):]
	}

	oldTrKeys := t.indexKeys(oldTr, idxs, buff[:0], [][]byte{}, keyPartsBuffer)

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

func batched[T any](allItems []T, batchSize int, f func(batch []T) error) error {
	batchNum := 0
	allItemsLen := len(allItems)
	for batchNum*batchSize < allItemsLen {
		start := batchNum * batchSize
		end := start + batchSize
		if end > allItemsLen {
			end = start + allItemsLen%batchSize
		}

		err := f(allItems[start:end])
		if err != nil {
			return err
		}

		batchNum++
	}

	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
