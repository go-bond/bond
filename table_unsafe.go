package bond

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"golang.org/x/exp/maps"
)

type TableUnsafeUpdate[T any] interface {
	UnsafeUpdate(ctx context.Context, trs []T, oldTrs []T, optBatch ...*pebble.Batch) error
}

func (t *_table[T]) UnsafeUpdate(ctx context.Context, trs []T, oldTrs []T, optBatch ...*pebble.Batch) error {
	if len(trs) != len(oldTrs) {
		return fmt.Errorf("params need to be of equal size")
	}

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
