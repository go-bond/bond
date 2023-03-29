package bond

import "github.com/cockroachdb/pebble"

type IterOptions struct {
	pebble.IterOptions

	releaseBufferOnClose func()
}

type Iterator interface {
	First() bool
	Last() bool
	Prev() bool
	Next() bool
	Valid() bool
	Error() error

	SeekGE(key []byte) bool
	SeekPrefixGE(key []byte) bool
	SeekLT(key []byte) bool

	Key() []byte
	Value() []byte

	Close() error
}

type _iterator struct {
	Iterator

	opts *IterOptions
}

func newIteratorFromDB(db *_db, opts *IterOptions) *_iterator {
	return &_iterator{Iterator: db.pebble.NewIter(&opts.IterOptions), opts: opts}
}

func newIteratorFromBatch(batch *pebble.Batch, opts *IterOptions) *_iterator {
	return &_iterator{Iterator: batch.NewIter(&opts.IterOptions), opts: opts}
}

func (it *_iterator) Close() error {
	defer func() {
		if it.opts.releaseBufferOnClose != nil {
			it.opts.releaseBufferOnClose()
		}
	}()

	err := it.Iterator.Close()
	if err != nil {
		return err
	}
	return nil
}
