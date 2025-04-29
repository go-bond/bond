package bond

import (
	"github.com/cockroachdb/pebble"
)

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

type _iterConstructor interface {
	NewIter(opts *pebble.IterOptions) Iterator
}

type _pebbleIterConstructor interface {
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

type _bondIterConstructor struct {
	pebbleConstructor _pebbleIterConstructor
}

func (bc *_bondIterConstructor) NewIter(opts *pebble.IterOptions) Iterator {
	itr, err := bc.pebbleConstructor.NewIter(opts)
	if err != nil {
		return &errIterator{err: err}
	}
	return itr
}

type _iterator struct {
	Iterator

	opts *IterOptions
}

func newIterator(itc _iterConstructor, opts *IterOptions) *_iterator {
	return &_iterator{Iterator: itc.NewIter(&opts.IterOptions), opts: opts}
}

func (it *_iterator) Close() error {
	defer func() {
		if it.opts.releaseBufferOnClose != nil {
			it.opts.releaseBufferOnClose()
			it.opts.releaseBufferOnClose = nil
		}
	}()

	return it.Iterator.Close()
}

type _iteratorMulti struct {
	iteratorOptions      []*IterOptions
	iteratorOptionsIndex int

	iteratorConstuctor Iterable
	iterator           Iterator
}

func newIteratorMulti(itc Iterable, opts []*IterOptions) *_iteratorMulti {
	return &_iteratorMulti{
		iteratorOptions:      opts,
		iteratorOptionsIndex: 0,
		iteratorConstuctor:   itc,
		iterator:             itc.Iter(childIteratorOptions(opts[0])),
	}
}

func (it *_iteratorMulti) First() bool {
	// find the valid iterator from the start of the list.
	for i := 0; i < len(it.iteratorOptions); i++ {
		_ = it.iterator.Close()
		it.iteratorOptionsIndex = i
		it.iterator = it.iteratorConstuctor.Iter(childIteratorOptions(it.iteratorOptions[it.iteratorOptionsIndex]))
		if it.iterator.First() {
			return true
		}
	}
	return false
}

func (it *_iteratorMulti) Last() bool {
	// find the valid iterator from the end of the list.
	for i := len(it.iteratorOptions) - 1; i >= 0; i-- {
		_ = it.iterator.Close()
		it.iteratorOptionsIndex = i
		it.iterator = it.iteratorConstuctor.Iter(childIteratorOptions(it.iteratorOptions[it.iteratorOptionsIndex]))
		if it.iterator.Last() {
			return true
		}
	}
	return false
}

func (it *_iteratorMulti) Prev() bool {
	for !it.iterator.Prev() {
		if it.iteratorOptionsIndex == 0 {
			return false
		}

		_ = it.iterator.Close()

		it.iteratorOptionsIndex--
		it.iterator = it.iteratorConstuctor.Iter(childIteratorOptions(it.iteratorOptions[it.iteratorOptionsIndex]))
		if it.iterator.Last() {
			break
		}
	}
	return true
}

func (it *_iteratorMulti) Next() bool {
	for !it.iterator.Next() {
		if it.iteratorOptionsIndex == len(it.iteratorOptions)-1 {
			return false
		}

		_ = it.iterator.Close()

		it.iteratorOptionsIndex++
		it.iterator = it.iteratorConstuctor.Iter(childIteratorOptions(it.iteratorOptions[it.iteratorOptionsIndex]))
		if it.iterator.First() {
			break
		}
	}
	return true
}

func (it *_iteratorMulti) Valid() bool {
	return it.iterator.Valid()
}

func (it *_iteratorMulti) Error() error {
	return it.iterator.Error()
}

func (it *_iteratorMulti) SeekGE(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (it *_iteratorMulti) SeekPrefixGE(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (it *_iteratorMulti) SeekLT(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (it *_iteratorMulti) Key() []byte {
	return it.iterator.Key()
}

func (it *_iteratorMulti) Value() []byte {
	return it.iterator.Value()
}

func (it *_iteratorMulti) Close() error {
	defer func() {
		for _, opts := range it.iteratorOptions {
			if opts.releaseBufferOnClose != nil {
				opts.releaseBufferOnClose()
				opts.releaseBufferOnClose = nil
			}
		}
	}()

	return it.iterator.Close()
}

func childIteratorOptions(opt *IterOptions) *IterOptions {
	subOpts := *opt
	subOpts.releaseBufferOnClose = nil
	return &subOpts
}

var _ Iterator = (*_iteratorMulti)(nil)

type errIterator struct {
	err error
}

func (e errIterator) First() bool {
	return false
}

func (e errIterator) Last() bool {
	return false
}

func (e errIterator) Prev() bool {
	return false
}

func (e errIterator) Next() bool {
	return false
}

func (e errIterator) Valid() bool {
	return false
}

func (e errIterator) Error() error {
	return e.err
}

func (e errIterator) SeekGE(key []byte) bool {
	return false
}

func (e errIterator) SeekPrefixGE(key []byte) bool {
	return false
}

func (e errIterator) SeekLT(key []byte) bool {
	return false
}

func (e errIterator) Key() []byte {
	return nil
}

func (e errIterator) Value() []byte {
	return nil
}

func (e errIterator) Close() error {
	return nil
}

var _ Iterator = (*errIterator)(nil)
