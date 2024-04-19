package bond

import (
	"bytes"
	"context"
	"fmt"

	"golang.org/x/exp/maps"
)

// TableUnsafeUpdater provides access to UnsafeUpdate method that allows
// to reduce number of database calls if you already have original version
// of the entry.
//
// Warning: If you provide outdated rows the table indexes may be corrupted.
type TableUnsafeUpdater[T any] interface {
	UnsafeUpdate(ctx context.Context, trs []T, oldTrs []T, optBatch ...Batch) error
}

func (t *_table[T]) UnsafeUpdate(ctx context.Context, trs []T, oldTrs []T, optBatch ...Batch) error {
	if len(trs) != len(oldTrs) {
		return fmt.Errorf("params need to be of equal size")
	}

	t.mutex.RLock()
	indexes := make(map[IndexID]*Index[T])
	maps.Copy(indexes, t.secondaryIndexes)
	t.mutex.RUnlock()

	var batch Batch
	var externalBatch = len(optBatch) > 0 && optBatch[0] != nil
	if externalBatch {
		batch = optBatch[0]
	} else {
		batch = t.db.Batch(BatchTypeWriteOnly)
	}

	// key
	var (
		keyBuffer       = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer  = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer2 = t.db.getKeyBufferPool().Get()[:0]
	)
	defer t.db.getKeyBufferPool().Put(keyBuffer[:0])
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer[:0])
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer2[:0])

	// value
	value := t.db.getValueBufferPool().Get()[:0]
	valueBuffer := bytes.NewBuffer(value)
	defer t.db.getValueBufferPool().Put(value)

	// serializer
	var serialize = t.serializer.Serializer.Serialize
	if sw, ok := t.serializer.Serializer.(SerializerWithBuffer[any]); ok {
		serialize = sw.SerializeFuncWithBuffer(valueBuffer)
	}

	for i := 0; i < len(trs); i++ {
		tr := trs[i]
		oldTr := oldTrs[i]

		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// update key
		key := t.key(tr, keyBuffer[:0])

		// serialize
		data, err := serialize(&tr)
		if err != nil {
			return err
		}

		// update entry
		err = batch.Set(key, data, Sync)
		if err != nil {
			_ = batch.Close()
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

	if !externalBatch {
		err := batch.Commit(Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}
	}

	return nil
}

// TableUnsafeInserter provides access to UnsafeInsert method that allows to insert
// records wihout checking if they already exist in the database.

// Warning: The indices of the records won't be updated properly if the records already exist.
type TableUnsafeInserter[T any] interface {
	UnsafeInsert(ctx context.Context, trs []T, optBatch ...Batch) error
}

func (t *_table[T]) UnsafeInsert(ctx context.Context, trs []T, optBatch ...Batch) error {
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
		indexKeyBuffer = t.db.getKeyBufferPool().Get()[:0]
	)
	defer t.db.getKeyBufferPool().Put(indexKeyBuffer[:0])

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

	err := batched[T](trs, persistentBatchSize, func(trs []T) error {
		// keys
		keys := t.keysExternal(trs, keysBuffer)

		// process rows
		for i, key := range keys {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context done: %w", ctx.Err())
			default:
			}

			tr := trs[i]
			// serialize
			data, err := serialize(&tr)
			if err != nil {
				return err
			}

			err = batch.Set(key, data, Sync)
			if err != nil {
				return err
			}

			// index keys
			for _, idx := range indexes {
				err = idx.OnInsert(t, tr, batch, indexKeyBuffer[:0])
				if err != nil {
					return err
				}
			}

			// add to bloom filter
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
