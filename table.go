package bond

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

	mutex sync.RWMutex
}

func NewTable[T any](opt TableOptions[T]) Table[T] {
	var serializer Serializer[*T] = &SerializerAnyWrapper[*T]{Serializer: opt.DB.Serializer()}
	if opt.Serializer != nil {
		serializer = opt.Serializer
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

	var prefixBuffer [DataKeyBufferSize]byte
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

	var (
		keyBatch      Batch
		keyBatchCtx   context.Context
		externalBatch = len(optBatch) > 0 && optBatch[0] != nil
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
	}()

	for _, tr := range trs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// serialize
		data, err := t.serializer.Serialize(&tr)
		if err != nil {
			return err
		}

		info := t.keySize(tr)
		set := keyBatch.SetDeferred(info.Total, len(data))
		// insert key
		set.Key = t.encodeKey(tr, info, PrimaryIndexID, set.Key, nil)
		copy(set.Value, data)

		// check if exist
		if t.exist(set.Key, keyBatch) {
			return fmt.Errorf("record: %x already exist", set.Key[_KeyPrefixSplitIndex(set.Key):])
		}

		err = set.Finish()
		if err != nil {
			return err
		}

		if t.filter != nil {
			t.filter.Add(keyBatchCtx, utils.Copy(set.Key))
		}

		for _, idx := range indexes {
			// verify whether this record should be indexed or not.
			if idx.IndexFilterFunction(tr) {
				info = t.indexKeySize(idx, tr)
				set = keyBatch.SetDeferred(info.Total, 0)
				set.Key = t.encodeKey(tr, info, idx.IndexID, set.Key, idx)
				err = set.Finish()
				if err != nil {
					return err
				}
			}

		}
	}

	if !externalBatch {
		err := keyBatch.Commit(Sync)
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
		oldTrData, closer, err := keyBatch.Get(key)
		if err != nil {
			return err
		}

		var oldTr T
		err = t.serializer.Deserialize(oldTrData, &oldTr)
		if err != nil {
			return err
		}

		_ = closer.Close()

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
		var (
			oldTr     T
			oldTrData []byte
			closer    io.Closer
			err       error
		)
		if t.exist(key, keyBatch) {
			oldTrData, closer, err = keyBatch.Get(key)
			if err == nil {
				err = t.serializer.Deserialize(oldTrData, &oldTr)
				if err != nil {
					return err
				}

				_ = closer.Close()
			}
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

	var keyBuffer [DataKeyBufferSize]byte
	key := t.key(tr, keyBuffer[:0])
	return t.exist(key, batch)
}

func (t *_table[T]) exist(key []byte, batch Batch) bool {
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

func (t *_table[T]) Get(tr T, optBatch ...Batch) (T, error) {
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
	} else {
		batch = nil
	}

	var keyBuffer [DataKeyBufferSize]byte
	key := t.key(tr, keyBuffer[:0])

	bCtx := ContextWithBatch(context.Background(), batch)
	if t.filter != nil && !t.filter.MayContain(bCtx, key) {
		return utils.MakeNew[T](), fmt.Errorf("not found")
	}

	return t.get(key, batch)
}

func (t *_table[T]) get(key []byte, batch Batch) (T, error) {
	data, closer, err := t.db.Get(key, batch)
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
	var prefixBuffer [DataKeyBufferSize]byte

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
			tableKey := KeyBytes(iter.Key()).ToDataKeyBytes(keyBuffer[:0])

			valueData, closer, err := t.db.Get(tableKey, batch)
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

func (t *_table[T]) key(tr T, buff []byte) []byte {
	var primaryKey = t.primaryKeyFunc(NewKeyBuilder(buff[:0], false), tr)

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: primaryKey,
	}, buff[len(primaryKey):len(primaryKey)])
}

// encodes `key` into the `buff` without using additional space.
func (t *_table[T]) encodeKey(tr T, info KeySizeInfo, id IndexID, buff []byte, idx *Index[T]) []byte {
	if idx != nil {
		_ = idx.IndexKeyFunction(NewKeyBuilder(buff[info.IndexPos:], false), tr)
		_ = idx.IndexOrderFunction(
			IndexOrder{keyBuilder: NewKeyBuilder(buff[info.IndexOrderPos:], false)}, tr,
		)
	}
	_ = t.primaryKeyFunc(NewKeyBuilder(buff[info.PrimaryPos:], false), tr)

	return KeyEncodePebble(KeyV2{TableID: t.id, IndexID: id, Info: info}, buff)
}

func (t *_table[T]) keySize(tr T) KeySizeInfo {
	var primarySize = utils.SliceToInt(t.primaryKeyFunc(NewKeyBuilder([]byte{}, true), tr))

	return KeySize(int(primarySize), 0, 0)
}

func (t *_table[T]) indexKeySize(idx *Index[T], tr T) KeySizeInfo {
	var primarySize = utils.SliceToInt(t.primaryKeyFunc(NewKeyBuilder([]byte{}, true), tr))
	var indexSize = utils.SliceToInt(idx.IndexKeyFunction(NewKeyBuilder([]byte{}, true), tr))
	var orderSize = utils.SliceToInt(idx.IndexOrderFunction(
		IndexOrder{keyBuilder: NewKeyBuilder([]byte{}, true)}, tr,
	).Bytes())

	return KeySize(primarySize, indexSize, orderSize)
}

func (t *_table[T]) keyPrefix(idx *Index[T], s T, buff []byte) []byte {
	indexKey := idx.IndexKeyFunction(NewKeyBuilder(buff[:0], false), s)

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    idx.IndexID,
		IndexKey:   indexKey,
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}, indexKey[len(indexKey):])
}

func (t *_table[T]) indexKey(tr T, idx *Index[T], buff []byte) []byte {
	primaryKey := t.primaryKeyFunc(NewKeyBuilder(buff[:0], false), tr)
	indexKeyPart := idx.IndexKeyFunction(NewKeyBuilder(primaryKey[len(primaryKey):], false), tr)
	orderKeyPart := idx.IndexOrderFunction(
		IndexOrder{keyBuilder: NewKeyBuilder(indexKeyPart[len(indexKeyPart):], false)}, tr,
	).Bytes()

	return KeyEncode(Key{
		TableID:    t.id,
		IndexID:    idx.IndexID,
		IndexKey:   indexKeyPart,
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
