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

type _iterConstructor interface {
	NewIter(opts *pebble.IterOptions) *pebble.Iterator
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
		}
	}()

	err := it.Iterator.Close()
	if err != nil {
		return err
	}
	return nil
}

type _iteratorMulti struct {
	iteratorOptions      []*IterOptions
	iteratorOptionsIndex int

	iteratorConstuctor Iterationer
	iterator           Iterator
}

func newIteratorMulti(itc Iterationer, opts []*IterOptions) *_iteratorMulti {
	return &_iteratorMulti{
		iteratorOptions:      opts,
		iteratorOptionsIndex: 0,
		iteratorConstuctor:   itc,
		iterator:             itc.Iter(opts[0]),
	}
}

func (it *_iteratorMulti) First() bool {
	if it.iteratorOptionsIndex != 0 {
		_ = it.iterator.Close()

		it.iteratorOptionsIndex = 0
		it.iterator = it.iteratorConstuctor.Iter(it.iteratorOptions[it.iteratorOptionsIndex])
	}
	return it.iterator.First()
}

func (it *_iteratorMulti) Last() bool {
	if it.iteratorOptionsIndex != len(it.iteratorOptions)-1 {
		_ = it.iterator.Close()

		it.iteratorOptionsIndex = len(it.iteratorOptions) - 1
		it.iterator = it.iteratorConstuctor.Iter(it.iteratorOptions[it.iteratorOptionsIndex])
	}
	return it.iterator.Last()
}

func (it *_iteratorMulti) Prev() bool {
	if !it.iterator.Prev() {
		if it.iteratorOptionsIndex == 0 {
			return false
		}

		_ = it.iterator.Close()

		it.iteratorOptionsIndex--
		it.iterator = it.iteratorConstuctor.Iter(it.iteratorOptions[it.iteratorOptionsIndex])
		return it.iterator.Last()
	}
	return true
}

func (it *_iteratorMulti) Next() bool {
	if !it.iterator.Next() {
		if it.iteratorOptionsIndex == len(it.iteratorOptions)-1 {
			return false
		}

		_ = it.iterator.Close()

		it.iteratorOptionsIndex++
		it.iterator = it.iteratorConstuctor.Iter(it.iteratorOptions[it.iteratorOptionsIndex])
		return it.iterator.First()
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
			}
		}
	}()

	return it.iterator.Close()
}

var _ Iterator = (*_iteratorMulti)(nil)
