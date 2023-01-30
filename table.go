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

var _keyBufferPool = sync.Pool{New: func() any {
	return make([]byte, 0, KeyBufferInitialSize)
}}

var _multiKeyBufferPool = sync.Pool{New: func() any {
	return make([]byte, 0, KeyBufferInitialSize*1000)
}}

const KeyBufferInitialSize = 10240
const ReindexBatchSize = 10000

const DefaultScanBatchSize = 100

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

	// TableScanner speeds up the scanning speed by prefetching records in batches.
	// ScanBatchSize is used to configure the batch size.
	ScanBatchSize int

	Filter Filter
}

type _table[T any] struct {
	id   TableID
	name string

	db DB

	primaryKeyFunc TablePrimaryKeyFunc[T]

	primaryIndex     *Index[T]
	secondaryIndexes map[IndexID]*Index[T]

	serializer Serializer[*T]

	filter Filter

	scanBatchSize int

	mutex sync.RWMutex
}

func NewTable[T any](opt TableOptions[T]) Table[T] {
	var serializer Serializer[*T] = &SerializerAnyWrapper[*T]{Serializer: opt.DB.Serializer()}
	if opt.Serializer != nil {
		serializer = opt.Serializer
	}

	scanBatchSize := opt.ScanBatchSize
	if scanBatchSize == 0 {
		scanBatchSize = DefaultScanBatchSize
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
		secondaryIndexes: make(map[IndexID]*Index[T]),
		serializer:       serializer,
		filter:           opt.Filter,
		mutex:            sync.RWMutex{},
		scanBatchSize:    scanBatchSize,
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

	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: prefix,
		},
	})

	batch := t.db.Batch()
	defer func() {
		_ = batch.Close()
	}()

	counter := 0
	indexKeysBuffer := make([]byte, 0, (KeyBufferInitialSize)*len(idxs))
	indexKeys := make([][]byte, 0, len(t.secondaryIndexes))

	for iter.SeekPrefixGE(prefix); iter.Valid(); iter.Next() {
		var tr T

		err := t.serializer.Deserialize(iter.Value(), &tr)
		if err != nil {
			return fmt.Errorf("failed to deserialize during reindexing: %w", err)
		}

		indexKeys = t.indexKeys(tr, idxsMap, indexKeysBuffer[:0], indexKeys[:0])

		for _, indexKey := range indexKeys {
			err = batch.Set(indexKey, []byte{}, Sync)
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

	// batch
	var (
		keyBatch      Batch
		keyBatchCtx   context.Context
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
		indexKeyBatch = t.db.Batch()
	)
	if externalBatch {
		keyBatch = optBatch[0]
	} else {
		keyBatch = t.db.Batch()
	}
	keyBatchCtx = ContextWithBatch(ctx, keyBatch)

	defer func() {
		if !externalBatch {
			_ = keyBatch.Close()
		}
		_ = indexKeyBatch.Close()
	}()

	// keys
	var (
		keys, keysCloser = t.keys(trs)
		indexKeysBuffer  = _keyBufferPool.Get().([]byte)
		indexKeys        = make([][]byte, 0, len(t.secondaryIndexes))
	)
	defer keysCloser()
	defer _keyBufferPool.Put(indexKeysBuffer)

	// exist iter
	keyOrder := t.sortKeys(keys)
	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			KeyTypes: pebble.IterKeyTypePointsOnly,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&PrimaryKeyFilter{ID: t.id, Keys: keys},
			},
		},
	}, keyBatch)
	defer func() { _ = iter.Close() }()

	// process rows
	for i, key := range keys {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// check if exist
		if t.keyDuplicate(i, keys) || t.exist(key, keyBatch, iter) {
			return fmt.Errorf("record: %x already exist", key[_KeyPrefixSplitIndex(key):])
		}

		tr := trs[keyOrder[i]]
		// serialize
		data, err := t.serializer.Serialize(&tr)
		if err != nil {
			return err
		}

		err = keyBatch.Set(key, data, Sync)
		if err != nil {
			return err
		}

		// index keys
		indexKeys = t.indexKeys(tr, indexes, indexKeysBuffer[:0], indexKeys[:0])

		// update indexes
		for _, indexKey := range indexKeys {
			err = indexKeyBatch.Set(indexKey, []byte{}, Sync)
			if err != nil {
				return err
			}
		}

		if t.filter != nil {
			t.filter.Add(keyBatchCtx, key)
		}
	}

	err := keyBatch.Apply(indexKeyBatch, Sync)
	if err != nil {
		return err
	}

	if !externalBatch {
		err = keyBatch.Commit(Sync)
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
		keyBatch      Batch
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
		indexKeyBatch = t.db.Batch()
	)
	if externalBatch {
		keyBatch = optBatch[0]
	} else {
		keyBatch = t.db.Batch()
	}

	defer func() {
		if !externalBatch {
			_ = keyBatch.Close()
		}
		_ = indexKeyBatch.Close()
	}()

	var (
		keys, keysCloser = t.keys(trs)
		indexKeyBuffer   = _keyBufferPool.Get().([]byte)
	)
	defer keysCloser()
	defer _keyBufferPool.Put(indexKeyBuffer)

	keyOrder := t.sortKeys(keys)
	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			KeyTypes: pebble.IterKeyTypePointsOnly,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&PrimaryKeyFilter{ID: t.id, Keys: keys},
			},
		},
	}, keyBatch)
	defer func() { _ = iter.Close() }()

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
		data, err := t.serializer.Serialize(&tr)
		if err != nil {
			return err
		}

		// update entry
		err = keyBatch.Set(key, data, Sync)
		if err != nil {
			return err
		}

		// indexKeys to add and remove
		toAddIndexKeys, toRemoveIndexKeys := t.indexKeysDiff(tr, oldTr, indexes, indexKeyBuffer[:0])

		// update indexes
		for _, indexKey := range toAddIndexKeys {
			err = indexKeyBatch.Set(indexKey, []byte{}, Sync)
			if err != nil {
				return err
			}
		}

		for _, indexKey := range toRemoveIndexKeys {
			err = indexKeyBatch.Delete(indexKey, Sync)
			if err != nil {
				return err
			}
		}
	}

	err := keyBatch.Apply(indexKeyBatch, Sync)
	if err != nil {
		return err
	}

	if !externalBatch {
		err = keyBatch.Commit(Sync)
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
		keyBatch      Batch
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
		indexKeyBatch = t.db.Batch()
	)
	if externalBatch {
		keyBatch = optBatch[0]
	} else {
		keyBatch = t.db.Batch()
	}

	defer func() {
		if !externalBatch {
			_ = keyBatch.Close()
		}
		_ = indexKeyBatch.Close()
	}()

	var (
		keyBuffer      = _keyBufferPool.Get().([]byte)
		indexKeyBuffer = _keyBufferPool.Get().([]byte)
		indexKeys      = make([][]byte, len(indexes))
	)
	defer _keyBufferPool.Put(keyBuffer)
	defer _keyBufferPool.Put(indexKeyBuffer)

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		var key = t.key(tr, keyBuffer[:0])
		indexKeys = t.indexKeys(tr, indexes, indexKeyBuffer[:0], indexKeys[:0])

		err := keyBatch.Delete(key, Sync)
		if err != nil {
			return err
		}

		for _, indexKey := range indexKeys {
			err = keyBatch.Delete(indexKey, Sync)
			if err != nil {
				return err
			}
		}
	}

	err := keyBatch.Apply(indexKeyBatch, Sync)
	if err != nil {
		return err
	}

	if !externalBatch {
		err = keyBatch.Commit(Sync)
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
		keyBatch      Batch
		keyBatchCtx   context.Context
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
		indexKeyBatch = t.db.Batch()
	)
	if externalBatch {
		keyBatch = optBatch[0]
	} else {
		keyBatch = t.db.Batch()
	}
	keyBatchCtx = ContextWithBatch(ctx, keyBatch)

	defer func() {
		if !externalBatch {
			_ = keyBatch.Close()
		}
		_ = indexKeyBatch.Close()
	}()

	var (
		keys, keysCloser = t.keys(trs)
		indexKeyBuffer   = _keyBufferPool.Get().([]byte)

		indexKeys = make([][]byte, 0, len(indexes))
	)
	defer keysCloser()
	defer _keyBufferPool.Put(indexKeyBuffer)

	keyOrder := t.sortKeys(keys)
	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			KeyTypes: pebble.IterKeyTypePointsOnly,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&PrimaryKeyFilter{ID: t.id, Keys: keys},
			},
		},
	}, keyBatch)
	defer func() { _ = iter.Close() }()

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
		if t.exist(key, keyBatch, iter) {
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
		data, err := t.serializer.Serialize(&tr)
		if err != nil {
			return err
		}

		// update entry
		err = keyBatch.Set(key, data, Sync)
		if err != nil {
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
			err = indexKeyBatch.Set(indexKey, []byte{}, Sync)
			if err != nil {
				return err
			}
		}

		for _, indexKey := range toRemoveIndexKeys {
			err = indexKeyBatch.Delete(indexKey, Sync)
			if err != nil {
				return err
			}
		}

		if t.filter != nil && !isUpdate {
			t.filter.Add(keyBatchCtx, key)
		}
		i++
	}

	err := keyBatch.Apply(indexKeyBatch, Sync)
	if err != nil {
		return err
	}

	if !externalBatch {
		err = keyBatch.Commit(Sync)
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

	keyBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyBuffer)
	key := t.key(tr, keyBuffer[:0])
	return t.exist(key, batch, nil)
}

func (t *_table[T]) exist(key []byte, batch Batch, iter Iterator) bool {
	bCtx := ContextWithBatch(context.Background(), batch)
	if t.filter != nil && !t.filter.MayContain(bCtx, key) {
		return false
	}

	if iter == nil {
		blockFilter := PrimaryKeyFilter{ID: t.id, Keys: [][]byte{key}}
		pointKeyFilters := []pebble.BlockPropertyFilter{&blockFilter}
		iterOptions := IterOptions{
			IterOptions: pebble.IterOptions{
				KeyTypes:        pebble.IterKeyTypePointsOnly,
				PointKeyFilters: pointKeyFilters,
			},
		}

		iter = t.db.Iter(&iterOptions, batch)
		defer func() {
			_ = iter.Close()
		}()
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
	key := t.key(tr, keyBuffer[:0])

	bCtx := ContextWithBatch(context.Background(), batch)
	if t.filter != nil && !t.filter.MayContain(bCtx, key) {
		return utils.MakeNew[T](), fmt.Errorf("not found")
	}

	records, err := t.get([][]byte{key}, batch)
	if err != nil {
		return utils.MakeNew[T](), err
	}
	return records[0], nil
}

func (t *_table[T]) get(keys [][]byte, batch Batch) ([]T, error) {
	if len(keys) == 0 {
		return make([]T, 0), nil
	}

	keyOrder := t.sortKeys(keys)
	blockFilter := PrimaryKeyFilter{ID: t.id, Keys: keys}
	pointKeyFilters := []pebble.BlockPropertyFilter{&blockFilter}
	iterOptions := IterOptions{
		IterOptions: pebble.IterOptions{
			KeyTypes:        pebble.IterKeyTypePointsOnly,
			PointKeyFilters: pointKeyFilters,
		},
	}

	iter := t.db.Iter(&iterOptions, batch)
	defer func() { _ = iter.Close() }()

	records := make([]T, len(keys))
	for i := 0; i < len(keys); i++ {
		if !iter.SeekGE(keys[i]) || !bytes.Equal(iter.Key(), keys[i]) {
			return nil, fmt.Errorf("not found")
		}
		var record T
		err := t.serializer.Deserialize(iter.Value(), &record)
		if err != nil {
			return nil, fmt.Errorf("get failed to deserialize: %w", err)
		}
		records[keyOrder[i]] = record
	}

	t.reorderKeys(keys, keyOrder)
	return records, nil
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

	selector := t.indexKey(s, idx, prefixBuffer[:0])

	var iter Iterator
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
		iter = batch.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
			},
		})
	} else {
		iter = t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
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
	for iter.SeekPrefixGE(selector); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
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

	selector := t.indexKey(s, idx, prefixBuffer[:0])

	var iter Iterator
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
		iter = batch.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
			},
		})
	} else {
		iter = t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: selector,
			},
		})
	}

	var prefetchedBatch []T
	var prefetchedBatchIndex int
	var keyBuffer = _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyBuffer)
	if len(keyBuffer) < 10 {
		keyBuffer = make([]byte, KeyBufferInitialSize)
	}
	utils.ClearBytes(keyBuffer, 10)
	keyBuffer[0] = byte(t.ID())

	keys := make([][]byte, 0, t.scanBatchSize)
	indexKeys := make([][]byte, 0, t.scanBatchSize)
	multiKeyBuffer := _multiKeyBufferPool.Get().([]byte)[:0]
	defer _multiKeyBufferPool.Put(multiKeyBuffer)

	getPrefetchedValue := func() (T, error) {
		return prefetchedBatch[prefetchedBatchIndex], nil
	}

	prefetchGetValue := func() (T, error) {
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
			if len(keys) <= t.scanBatchSize {
				iter.Next()
				continue
			}
			break
		}

		var err error
		prefetchedBatch, err = t.get(keys, batch)
		if err != nil {
			return utils.MakeNew[T](), err
		}
		prefetchedBatchIndex++
		return prefetchedBatch[prefetchedBatchIndex-1], nil
	}

	for iter.SeekPrefixGE(selector); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		cont, err := f(iter.Key(), Lazy[T]{GetFunc: prefetchGetValue})
		if !cont || err != nil {
			_ = iter.Close()
			return err
		}

		// iterate from prefetched entires if exist.
		for prefetchedBatchIndex < len(prefetchedBatch) {
			cont, err = f(indexKeys[prefetchedBatchIndex], Lazy[T]{GetFunc: getPrefetchedValue})
			if !cont || err != nil {
				_ = iter.Close()
				return err
			}
			prefetchedBatchIndex++
		}

		prefetchedBatchIndex = 0
		prefetchedBatch = nil
		keys = nil
		indexKeys = nil
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

func (t *_table[T]) reorderKeys(keys [][]byte, keyOrder []int) {
	sort.Sort(&utils.SortShim{
		Length: len(keys),
		SwapFn: func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
			keyOrder[i], keyOrder[j] = keyOrder[j], keyOrder[i]
		},
		LessFn: func(i, j int) bool {
			return keyOrder[i] < keyOrder[j]
		},
	})
}

func (t *_table[T]) key(tr T, buff []byte) []byte {
	var primaryKey = t.primaryKeyFunc(NewKeyBuilder(buff[:0]), tr)

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    PrimaryIndexID,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: primaryKey,
	}, primaryKey[len(primaryKey):])
}

func (t *_table[T]) keys(trs []T) ([][]byte, func()) {
	keys := make([][]byte, len(trs))
	multiKeyBuffer := _multiKeyBufferPool.Get().([]byte)[:0]

	next := multiKeyBuffer
	for i, tr := range trs {
		key := t.key(tr, next)
		keys[i] = key

		next = key[len(key):]
	}

	closer := func() {
		_multiKeyBufferPool.Put(multiKeyBuffer)
	}
	return keys, closer
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
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}, indexKey[len(indexKey):])
}

func (t *_table[T]) indexKey(tr T, idx *Index[T], buff []byte) []byte {
	primaryKey := t.primaryKeyFunc(NewKeyBuilder(buff[:0]), tr)
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
	}, orderKeyPart[len(orderKeyPart):])
}

func (t *_table[T]) indexKeys(tr T, idxs map[IndexID]*Index[T], buff []byte, indexKeysBuff [][]byte) [][]byte {
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

func (t *_table[T]) indexKeysDiff(newTr T, oldTr T, idxs map[IndexID]*Index[T], buff []byte) (toAdd [][]byte, toRemove [][]byte) {
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
