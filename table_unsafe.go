package bond

import (
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
		batch = t.db.Batch()
	}

	var (
		keyBuffer      = _keyBufferPool.Get().([]byte)
		indexKeyBuffer = _keyBufferPool.Get().([]byte)
	)
	defer _keyBufferPool.Put(keyBuffer)
	defer _keyBufferPool.Put(indexKeyBuffer)

	keyPartsBuffer := _keyBufferPool.Get().([]byte)
	defer _keyBufferPool.Put(keyPartsBuffer)

	for i := 0; i < len(trs); i++ {
		tr := trs[i]
		oldTr := oldTrs[i]

		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		// update key
		key := t.key(tr, keyBuffer[:0], keyPartsBuffer[:0])

		// serialize
		data, err := t.serializer.Serialize(&tr)
		if err != nil {
			return err
		}

		// update entry
		err = batch.Set(key, data, Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}

		// indexKeys to add and remove
		toAddIndexKeys, toRemoveIndexKeys := t.indexKeysDiff(tr, oldTr, indexes, indexKeyBuffer[:0], keyPartsBuffer[:0])

		// update indexes
		for _, indexKey := range toAddIndexKeys {
			err = batch.Set(indexKey, []byte{}, Sync)
			if err != nil {
				_ = batch.Close()
				return err
			}
		}

		for _, indexKey := range toRemoveIndexKeys {
			err = batch.Delete(indexKey, Sync)
			if err != nil {
				_ = batch.Close()
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
