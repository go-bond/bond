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
		batch = t.db.Batch()
	}

	// key
	var (
		keyBuffer       = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer  = t.db.getKeyBufferPool().Get()[:0]
		indexKeyBuffer2 = t.db.getKeyBufferPool().Get()[:0]
		recordKeyBuffer = t.db.getKeyBufferPool().Get()[:0]
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
		recordID, closer, err := t.db.Get(key, batch)
		if err != nil {
			return err
		}
		recordKey := t.recordKey(DecodeUint64(recordID), recordKeyBuffer[:0])
		closer.Close()
		// serialize
		data, err := serialize(&tr)
		if err != nil {
			return err
		}

		// update entry
		err = batch.Set(recordKey, data, Sync)
		if err != nil {
			_ = batch.Close()
			return err
		}

		// update indexes
		for _, idx := range indexes {
			err = idx.OnUpdate(t, oldTr, tr, recordKey[RecordPrefixSplit:], batch, indexKeyBuffer[:0], indexKeyBuffer2[:0])
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
