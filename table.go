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

var _indexKeyValue = []byte{}

const ReindexBatchSize = 10_000

const DefaultScanPrefetchSize = 100

const persistentBatchSize = 5000

type TableID uint8
type TablePrimaryKeyFunc[T any] func(builder KeyBuilder, t T) []byte

func TableUpsertOnConflictReplace[T any](_, new T) T {
	return new
}

func primaryIndexKey[T any](b KeyBuilder, _ T) []byte { return b.Bytes() }

type TableInfo interface {
	ID() TableID
	Name() string
	Indexes() []IndexInfo
	EntryType() reflect.Type
	SelectorPointType() reflect.Type
}

type TableGetter[T any] interface {
	Get(ctx context.Context, sel Selector[T], optBatch ...Batch) ([]T, error)
	GetPoint(ctx context.Context, sel T, optBatch ...Batch) (T, error)
}

type TableExistChecker[T any] interface {
	Exist(tr T, optBatch ...Batch) bool
}

type TableQuerier[T any] interface {
	Query() Query[T]
}

type TableScanner[T any] interface {
	Scan(ctx context.Context, tr *[]T, reverse bool, optBatch ...Batch) error
	ScanIndex(ctx context.Context, i *Index[T], s Selector[T], tr *[]T, reverse bool, optBatch ...Batch) error
	ScanForEach(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[T]) (bool, error), reverse bool, optBatch ...Batch) error
	ScanIndexForEach(ctx context.Context, idx *Index[T], s Selector[T], f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), reverse bool, optBatch ...Batch) error
}

type TableIterable[T any] interface {
	Iter(sel Selector[T], optBatch ...Batch) Iterator
}

type TableReader[T any] interface {
	TableInfo

	DB() DB
	PrimaryKey(builder KeyBuilder, tr T) []byte
	PrimaryIndex() *Index[T]
	SecondaryIndexes() []*Index[T]
	Serializer() Serializer[*T]

	TableGetter[T]
	TableExistChecker[T]
	TableQuerier[T]

	TableScanner[T]
	TableIterable[T]
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
	ReIndex(idxs []*Index[T]) error

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
	Serializer          Serializer[any]

	ScanPrefetchSize int

	// TODOXXX: should we always require a bloom filter for every table..?
	// currently we use this on Insert .. maybe, maybe not..
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
	valueEmpty        T
	valueNil          T

	scanPrefetchSize int

	serializer *SerializerAnyWrapper[*T]

	filter Filter

	mutex sync.RWMutex
}

func NewTable[T any](opt TableOptions[T]) Table[T] {
	// TODOXXX: what do we need this "AnyWrapper" for ..?
	var serializer = &SerializerAnyWrapper[*T]{Serializer: opt.DB.Serializer()}
	if opt.Serializer != nil {
		serializer = &SerializerAnyWrapper[*T]{Serializer: opt.Serializer}
	}

	scanPrefetchSize := DefaultScanPrefetchSize
	if opt.ScanPrefetchSize != 0 {
		scanPrefetchSize = opt.ScanPrefetchSize
	}

	// TODO: perhaps switch to error instead of a panic.
	if opt.TableID == 0 {
		panic(fmt.Errorf("bond: table id 0 is reserved for bond"))
	}

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
		valueEmpty:        utils.MakeNew[T](),
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
	return reflect.TypeOf(t.valueEmpty)
}

func (t *_table[T]) SelectorPointType() reflect.Type {
	return reflect.TypeOf(NewSelectorPoint(t.valueEmpty))
}

func (t *_table[T]) DB() DB {
	return t.db
}

func (t *_table[T]) PrimaryKey(builder KeyBuilder, tr T) []byte {
	return t.primaryKeyFunc(builder, tr)
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
		_, ok := t.secondaryIndexes[idx.IndexID]
		if ok {
			return fmt.Errorf("found duplicate IndexID %d", idx.IndexID)
		}
		t.secondaryIndexes[idx.IndexID] = idx
	}
	t.mutex.Unlock()

	if len(reIndex) > 0 && reIndex[0] {
		return t.ReIndex(idxs)
	}
	return nil
}

func (t *_table[T]) ReIndex(idxs []*Index[T]) error {
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

	var prefixBuffer [DefaultKeyBufferSize]byte
	var prefixSuccessorBuffer [DefaultKeyBufferSize]byte
	prefix := keyPrefix(t.id, t.primaryIndex, t.valueEmpty, prefixBuffer[:0])
	prefixSuccessor := keySuccessor(prefix, prefixSuccessorBuffer[:0])

	iter := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixSuccessor,
		},
	})

	batch := t.db.Batch(BatchTypeWriteOnly)
	defer func() {
		_ = batch.Close()
	}()

	counter := 0
	indexKeyBuffer := make([]byte, 0, (DefaultKeyBufferSize)*len(idxs))

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		var tr T

		// deserialize row
		err := t.serializer.Deserialize(iter.Value(), &tr)
		if err != nil {
			return fmt.Errorf("failed to deserialize during reindexing: %w", err)
		}

		// update indexes
		for _, idx := range idxs {
			err = idx.OnInsert(t, tr, batch, indexKeyBuffer)
			if err != nil {
				return err
			}
		}

		// batch handling
		counter++
		if counter >= ReindexBatchSize {
			counter = 0

			err = batch.Commit(Sync)
			if err != nil {
				return fmt.Errorf("failed to commit reindex batch: %w", err)
			}

			batch = t.db.Batch(BatchTypeWriteOnly)
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
		batch          Batch
		batchReadWrite Batch
		externalBatch  = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch(BatchTypeWriteOnly)
		defer batch.Close()
	}

	if batch.Type() == BatchTypeReadWrite {
		batchReadWrite = batch
	}

	var (
		indexKeyBuffer = t.db.getKeyBufferPool().Get()[:0]
	)
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer[:0]) // TODOXXX: defer .. hmm.. not great, too many defers

	// key buffers
	keysBuffer := t.db.getKeyArray(minInt(len(trs), persistentBatchSize))
	defer t.db.putKeyArray(keysBuffer)

	// value
	value := t.db.getValueBufferPool().Get()[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer t.db.getValueBufferPool().Put(value[:0])

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	// TODOXXX: review "persistentBatchSize" value
	err := batched(trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer)

		// order keys
		keyOrder := t.sortKeys(keys)

		// iter
		iter := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: keys[0],
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batchReadWrite)
		defer iter.Close()

		// process rows
		for i, key := range keys {

			// Check every 100 iterations if context is done, with micro-optimization.
			if i%100 == 0 {
				select {
				case <-ctx.Done():
					return fmt.Errorf("context done: %w", ctx.Err())
				default:
				}
			}

			// check if exist efficiently (via bloom filter)
			if t.keyDuplicate(i, keys) || t.exist(key, batch, iter) {
				return fmt.Errorf("record: %x already exist", key[_KeyPrefixSplitIndex(key):])
			}

			// serialize the row
			tr := trs[keyOrder[i]]
			data, err := serialize(&tr)
			if err != nil {
				return err
			}

			// write the row to the batch for key
			err = batch.Set(key, data, Sync)
			if err != nil {
				return err
			}

			// index keys by writing index entries for this row, in this model
			// we add index entries at the same time we write the row to the batch.
			for _, idx := range indexes {
				err = idx.OnInsert(t, tr, batch, indexKeyBuffer[:0])
				if err != nil {
					return err
				}
			}

			// add object to the bloom filter
			if t.filter != nil {
				t.filter.Add(ctx, key)
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
		batch          Batch
		batchReadWrite Batch
		externalBatch  = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch(BatchTypeWriteOnly)
		defer batch.Close()
	}

	if batch.Type() == BatchTypeReadWrite {
		batchReadWrite = batch
	}

	var (
		indexKeyBuffer  = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer2 = t.db.getKeyBufferPool().Get()[:0]
	)
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer[:0])
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer2[:0])

	// key buffers
	keysBuffer := t.db.getKeyArray(minInt(len(trs), persistentBatchSize))
	defer t.db.putKeyArray(keysBuffer)

	// value
	value := t.db.getValueBufferPool().Get()[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer t.db.getValueBufferPool().Put(value[:0])

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	// reusable object
	var oldTr T

	err := batched(trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer)

		// order keys
		keyOrder := t.sortKeys(keys)

		// iter
		iter := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: keys[0],
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batchReadWrite)
		defer iter.Close()

		for i, key := range keys {
			// Check every 100 iterations if context is done, with micro-optimization.
			if i%100 == 0 {
				select {
				case <-ctx.Done():
					return fmt.Errorf("context done: %w", ctx.Err())
				default:
				}
			}

			// skip this records since the next record updating the
			// same primary key.
			if i < len(keys)-1 && t.keyDuplicate(i+1, keys) {
				continue
			}

			if !iter.SeekGE(key) || !bytes.Equal(iter.Key(), key) {
				return fmt.Errorf("record: %x not found", key[_KeyPrefixSplitIndex(key):])
			}

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

			// update indexes
			for _, idx := range indexes {
				err = idx.OnUpdate(t, oldTr, tr, batch, indexKeyBuffer[:0], indexKeyBuffer2[:0])
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
		batch = t.db.Batch(BatchTypeWriteOnly)
		defer batch.Close()
	}

	var (
		keyBuffer      = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer = t.db.getKeyBufferPool().Get()[:0]
	)
	defer t.db.getKeyBufferPool().Put(keyBuffer[:0])
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer[:0])

	for i, tr := range trs {
		// Check every 100 iterations if context is done, with micro-optimization.
		if i%100 == 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context done: %w", ctx.Err())
			default:
			}
		}

		// data key
		var key = t.key(tr, keyBuffer[:0])

		// delete
		err := batch.Delete(key, Sync)
		if err != nil {
			return err
		}

		// delete from index
		for _, idx := range indexes {
			err = idx.OnDelete(t, tr, batch, indexKeyBuffer[:0])
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
		batch          Batch
		batchReadWrite Batch
		externalBatch  = len(optBatch) > 0 && optBatch[0] != nil
	)

	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch(BatchTypeWriteOnly)
		defer batch.Close()
	}

	if batch.Type() == BatchTypeReadWrite {
		batchReadWrite = batch
	}

	var (
		indexKeyBuffer  = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer2 = t.db.getKeyBufferPool().Get()[:0]
	)
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer[:0])
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer2[:0])

	// key buffers
	keysBuffer := t.db.getKeyArray(minInt(len(trs), persistentBatchSize))
	defer t.db.putKeyArray(keysBuffer)

	// value
	value := t.db.getValueBufferPool().Get()[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer t.db.getValueBufferPool().Put(value[:0])

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	// reusable object
	var oldTr T

	err := batched(trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer)

		// order keys
		keyOrder := t.sortKeys(keys)

		// iter
		iter := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: keys[0],
				UpperBound: t.dataKeySpaceEnd,
			},
		}, batchReadWrite)
		defer iter.Close()

		for i := 0; i < len(keys); {
			tr := trs[keyOrder[i]]

			// Check every 100 iterations if context is done, with micro-optimization.
			if i%100 == 0 {
				select {
				case <-ctx.Done():
					return fmt.Errorf("context done: %w", ctx.Err())
				default:
				}
			}

			// update key
			key := keys[i]

			// old record
			var (
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

			// update indexes
			if isUpdate {
				for _, idx := range indexes {
					err = idx.OnUpdate(t, oldTr, tr, batch, indexKeyBuffer[:0], indexKeyBuffer2[:0])
					if err != nil {
						return err
					}
				}
			} else {
				for _, idx := range indexes {
					err = idx.OnInsert(t, tr, batch, indexKeyBuffer[:0])
					if err != nil {
						return err
					}
				}
			}

			// add to bloom filter
			if t.filter != nil && !isUpdate {
				t.filter.Add(ctx, key)
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

	keyBuffer := t.db.getKeyBufferPool().Get()[:0]
	defer t.db.getKeyBufferPool().Put(keyBuffer[:0])

	key := t.key(tr, keyBuffer[:0])

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
	// search the bloom filter first
	if t.filter != nil && !t.filter.MayContain(context.Background(), key) {
		return false
	}

	// create an iterator if not provided, which is a slow operation,
	// we want to avoid this.
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

func (t *_table[T]) GetPoint(ctx context.Context, in T, optBatch ...Batch) (T, error) {
	var tr T
	trs, err := t.Get(ctx, NewSelectorPoint(in), optBatch...)
	if err != nil {
		return tr, err
	}
	if len(trs) == 0 {
		return tr, ErrNotFound
	}
	return trs[0], nil
}

func (t *_table[T]) Get(ctx context.Context, sel Selector[T], optBatch ...Batch) ([]T, error) {
	var batch Batch
	if len(optBatch) > 0 && optBatch[0] != nil {
		batch = optBatch[0]
	} else {
		batch = nil
	}

	switch sel.Type() {

	case SelectorTypePoint:
		tr := sel.(SelectorPoint[T]).Point()

		keyBuffer := t.db.getKeyBufferPool().Get()[:0]
		defer t.db.getKeyBufferPool().Put(keyBuffer[:0])

		key := t.key(tr, keyBuffer[:0])

		bCtx := ContextWithBatch(context.Background(), batch)
		if t.filter != nil && !t.filter.MayContain(bCtx, key) {
			return nil, ErrNotFound
		}

		record, closer, err := t.db.Get(key, batch)
		if err != nil {
			return nil, ErrNotFound
		}
		defer closer.Close()

		var rtr T
		err = t.serializer.Deserialize(record, &rtr)
		if err != nil {
			return nil, fmt.Errorf("get failed to deserialize: %w", err)
		}

		return []T{rtr}, nil

	case SelectorTypePoints:
		selPoints := sel.(SelectorPoints[T]).Points()

		keyArray := t.db.getKeyArray(len(selPoints))
		defer t.db.putKeyArray(keyArray)

		valueArray := t.db.getKeyArray(len(selPoints))
		defer t.db.putKeyArray(valueArray)

		var trs []T
		err := batched(selPoints, len(selPoints), func(selPoints []T) error {
			keys := t.keysExternal(selPoints, keyArray)

			values, err := t.get(keys, batch, valueArray, false)
			if err != nil {
				return err
			}

			for _, value := range values {
				if len(value) == 0 {
					trs = append(trs, t.valueNil)
					continue
				}

				var tr T
				err = t.serializer.Deserialize(value, &tr)
				if err != nil {
					return err
				}

				trs = append(trs, tr)
			}

			return nil
		})
		if err != nil {
			return nil, err
		}

		return trs, nil

	case SelectorTypeRange, SelectorTypeRanges:
		var trs []T

		err := t.ScanIndex(ctx, t.primaryIndex, sel, &trs, false, optBatch...)
		if err != nil {
			return nil, err
		}

		return trs, nil

	default:
		return nil, fmt.Errorf("invalid selector type")
	}
}

func (t *_table[T]) get(keys [][]byte, batch Batch, values [][]byte, errorOnNotExist bool) ([][]byte, error) {
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
		if t.filter != nil && !t.filter.MayContain(context.Background(), keys[i]) {
			if errorOnNotExist {
				return nil, ErrNotFound
			} else {
				values[i] = values[i][:0]
				continue
			}
		}

		if !iter.SeekGE(keys[i]) || !bytes.Equal(iter.Key(), keys[i]) {
			if errorOnNotExist {
				return nil, ErrNotFound
			} else {
				values[i] = values[i][:0]
				continue
			}
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

func (t *_table[T]) Iter(sel Selector[T], optBatch ...Batch) Iterator {
	return t.primaryIndex.Iter(t, sel, optBatch...)
}

func (t *_table[T]) Query() Query[T] {
	return newQuery(t, t.primaryIndex)
}

func (t *_table[T]) Scan(ctx context.Context, tr *[]T, reverse bool, optBatch ...Batch) error {
	return t.ScanIndex(ctx, t.primaryIndex, NewSelectorPoint(t.valueEmpty), tr, reverse, optBatch...)
}

func (t *_table[T]) ScanIndex(ctx context.Context, i *Index[T], s Selector[T], tr *[]T, reverse bool, optBatch ...Batch) error {
	return t.ScanIndexForEach(ctx, i, s, func(keyBytes KeyBytes, lazy Lazy[T]) (bool, error) {
		if record, err := lazy.Get(); err == nil {
			*tr = append(*tr, record)
			return true, nil
		} else {
			return false, err
		}
	}, reverse, optBatch...)
}

func (t *_table[T]) ScanForEach(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[T]) (bool, error), reverse bool, optBatch ...Batch) error {
	return t.ScanIndexForEach(ctx, t.primaryIndex, NewSelectorPoint(t.valueEmpty), f, reverse, optBatch...)
}

func (t *_table[T]) ScanIndexForEach(ctx context.Context, idx *Index[T], s Selector[T], f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), reverse bool, optBatch ...Batch) error {
	if idx.IndexID == PrimaryIndexID {
		return t.scanForEachPrimaryIndex(ctx, idx, s, f, reverse, optBatch...)
	} else {
		return t.scanForEachSecondaryIndex(ctx, idx, s, f, reverse, optBatch...)
	}
}

// TODOXXX: performance review..?
func (t *_table[T]) scanForEachPrimaryIndex(ctx context.Context, idx *Index[T], s Selector[T], f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), reverse bool, optBatch ...Batch) error {
	it := idx.Iter(t, s, optBatch...)

	first := func() bool {
		if reverse {
			return it.Last()
		} else {
			return it.First()
		}
	}

	next := func() bool {
		if reverse {
			return it.Prev()
		} else {
			return it.Next()
		}
	}

	getValue := func() (T, error) {
		var record T
		err := t.serializer.Deserialize(it.Value(), &record)
		if err != nil {
			return t.valueNil, fmt.Errorf("get failed to deserialize: %w", err)
		}

		return record, nil
	}
	for first(); it.Valid(); next() {
		select {
		case <-ctx.Done():
			_ = it.Close()
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		cont, err := f(it.Key(), Lazy[T]{GetFunc: getValue})
		if !cont || err != nil {
			_ = it.Close()
			return err
		}
	}

	return it.Close()
}

// TODOXXX: performance review..?
func (t *_table[T]) scanForEachSecondaryIndex(ctx context.Context, idx *Index[T], s Selector[T], f func(keyBytes KeyBytes, t Lazy[T]) (bool, error), reverse bool, optBatch ...Batch) error {
	it := idx.Iter(t, s, optBatch...)

	first := func() bool {
		if reverse {
			return it.Last()
		} else {
			return it.First()
		}
	}

	next := func() bool {
		if reverse {
			return it.Prev()
		} else {
			return it.Next()
		}
	}

	var batch Batch
	if len(optBatch) > 0 {
		batch = optBatch[0]
	}

	keys := t.db.getBytesArrayPool().Get()[:0]
	indexKeys := t.db.getBytesArrayPool().Get()[:0]
	multiKeyBuffer := t.db.getMultiKeyBufferPool().Get()[:0]
	valuesBuffer := t.db.getValueArray(t.scanPrefetchSize)
	defer t.db.getBytesArrayPool().Put(keys[:0])
	defer t.db.getBytesArrayPool().Put(indexKeys[:0])
	defer t.db.getMultiKeyBufferPool().Put(multiKeyBuffer[:0])
	defer t.db.putValueArray(valuesBuffer)

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
			return t.valueNil, fmt.Errorf("get failed to deserialize: %w", err)
		}

		prefetchedValuesIndex++
		return rtr, nil
	}

	prefetchAndGetValue := func() (T, error) {
		// prefetch the required data keys.
		nextBuff := multiKeyBuffer
		for it.Valid() {
			iterKey := it.Key()

			indexKey := append(nextBuff[:0], iterKey...)
			indexKeys = append(indexKeys, indexKey)
			nextBuff = indexKey[len(indexKey):]

			key := KeyBytes(iterKey).ToDataKeyBytes(nextBuff[:0])
			keys = append(keys, key)

			nextBuff = key[len(key):]
			if len(keys) < t.scanPrefetchSize {
				next()
				continue
			}
			break
		}

		var err error
		prefetchedValues, err = t.get(keys, batch, valuesBuffer, true)
		if err != nil {
			return t.valueNil, err
		}

		return getPrefetchedValue()
	}

	for first(); it.Valid(); next() {
		select {
		case <-ctx.Done():
			_ = it.Close()
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		cont, err := f(it.Key(), Lazy[T]{GetFunc: prefetchAndGetValue})
		if !cont || err != nil {
			_ = it.Close()
			return err
		}

		// iterate from prefetched entire if exists.
		for prefetchedValuesIndex < len(prefetchedValues) {
			cont, err = f(indexKeys[prefetchedValuesIndex], Lazy[T]{GetFunc: getPrefetchedValue})
			if !cont || err != nil {
				_ = it.Close()
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

	return it.Close()
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

func (t *_table[T]) key(tr T, buff []byte) []byte {
	return KeyEncodeRaw(
		t.id,
		PrimaryIndexID,
		nil,
		nil,
		func(b []byte) []byte {
			return t.primaryKeyFunc(NewKeyBuilder(b), tr)
		},
		buff[:0])
}

func (t *_table[T]) keysExternal(trs []T, keys [][]byte) [][]byte {
	retKeys := keys[:len(trs)]
	for i, tr := range trs {
		key := t.key(tr, retKeys[i][:0])
		retKeys[i] = key
	}
	return retKeys
}

func (t *_table[T]) keyDuplicate(index int, keys [][]byte) bool {
	return index > 0 && bytes.Equal(keys[index], keys[index-1])
}

func keyPrefix[T any](tableID TableID, idx *Index[T], s T, buff []byte) []byte {
	return KeyEncodeRaw(
		tableID,
		idx.IndexID,
		func(b []byte) []byte {
			return idx.IndexKeyFunction(NewKeyBuilder(b), s)
		},
		nil,
		nil,
		buff[:0],
	)
}

func keySuccessor(src, dst []byte) []byte {
	if dst != nil {
		dst = append(dst, src...)
	} else {
		dst = src
	}

	for i := len(src) - 1; i >= 0; i-- {
		if dst[i] != 0xFF {
			dst[i]++
			return dst
		}
	}
	return dst
}

func batched[T any](items []T, batchSize int, f func(batch []T) error) error {
	batchNum := 0
	itemsLen := len(items)
	for batchNum*batchSize < itemsLen {
		start := batchNum * batchSize
		end := start + batchSize
		if end > itemsLen {
			end = start + itemsLen%batchSize
		}

		err := f(items[start:end])
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
