package bond

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

var sequenceId = NumberSequence{}

type Batch interface {
	ID() uint64
	Len() int
	Empty() bool
	Reset()

	Getter
	Setter
	Deleter
	DeleterWithRange
	Iterationer

	Apply(b Batch) error
	Commit(opt WriteOptions) error
	Close() error

	OnCommit(func(b Batch) error)
	OnCommitted(func(b Batch))
	OnError(func(b Batch, err error))
}

type _batch struct {
	*pebble.Batch

	id uint64

	onCommitCallbacks    []func(b Batch) error
	onCommittedCallbacks []func(b Batch)
	onErrorCallbacks     []func(b Batch, err error)
}

func newBatch(db *_db) Batch {
	id, _ := sequenceId.Next()
	return &_batch{
		Batch: db.pebble.NewIndexedBatch(),
		id:    id,
	}
}

func (b *_batch) ID() uint64 {
	return b.id
}

func (b *_batch) Reset() {
	b.Batch.Reset()

	b.id, _ = sequenceId.Next()

	b.onCommitCallbacks = nil
	b.onCommittedCallbacks = nil
	b.onErrorCallbacks = nil
}

func (b *_batch) Get(key []byte, _ ...Batch) (data []byte, closer io.Closer, err error) {
	return b.Batch.Get(key)
}

func (b *_batch) Set(key []byte, value []byte, opt WriteOptions, _ ...Batch) error {
	return b.Batch.Set(key, value, pebbleWriteOptions(opt))
}

func (b *_batch) Delete(key []byte, opts WriteOptions, _ ...Batch) error {
	return b.Batch.Delete(key, pebbleWriteOptions(opts))
}

func (b *_batch) DeleteRange(start []byte, end []byte, opt WriteOptions, _ ...Batch) error {
	return b.Batch.DeleteRange(start, end, pebbleWriteOptions(opt))
}

func (b *_batch) Iter(opt *IterOptions, _ ...Batch) Iterator {
	return b.NewIter(pebbleIterOptions(opt))
}

func (b *_batch) Apply(batch Batch) error {
	innerBatch, ok := batch.(*_batch)
	if !ok {
		return fmt.Errorf("incorrect param")
	}

	return b.Batch.Apply(innerBatch.Batch, pebble.Sync)
}

func (b *_batch) Commit(opt WriteOptions) error {
	if b.Empty() {
		return nil
	}

	err := b.notifyOnCommit()
	if err != nil {
		return err
	}

	err = b.Batch.Commit(pebbleWriteOptions(opt))
	if err != nil {
		b.notifyOnError(err)
		return err
	}

	b.notifyOnCommitted()
	return nil
}

func (b *_batch) OnCommit(f func(b Batch) error) {
	b.onCommitCallbacks = append(b.onCommitCallbacks, f)
}

func (b *_batch) OnCommitted(f func(b Batch)) {
	b.onCommittedCallbacks = append(b.onCommittedCallbacks, f)
}

func (b *_batch) OnError(f func(b Batch, err error)) {
	b.onErrorCallbacks = append(b.onErrorCallbacks, f)
}

func (b *_batch) notifyOnCommit() error {
	for _, f := range b.onCommitCallbacks {
		err := f(b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *_batch) notifyOnCommitted() {
	for _, f := range b.onCommittedCallbacks {
		f(b)
	}
}

func (b *_batch) notifyOnError(err error) {
	for _, f := range b.onErrorCallbacks {
		f(b, err)
	}
}
