package bond

import (
	"bytes"
	"context"

	"github.com/cockroachdb/pebble"
)

type IterOptions struct {
	pebble.IterOptions
	filter Filter
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

	SetOptions(opt *pebble.IterOptions)

	Exist(key []byte) bool

	Close() error
}

func pebbleIterOptions(opt *IterOptions) *pebble.IterOptions {
	if opt == nil {
		return &pebble.IterOptions{}
	} else {
		return &opt.IterOptions
	}
}

type BondIterator struct {
	*pebble.Iterator
	filter Filter
	batch  Batch
}

func (b *BondIterator) Exist(key []byte) bool {
	if b.batch != nil && b.filter != nil {
		bCtx := ContextWithBatch(context.Background(), b.batch)
		if !b.filter.MayContain(bCtx, key) {
			return false
		}
	}
	if !b.SeekPrefixGE(key) {
		return false
	}
	return bytes.Equal(b.Key(), key)
}
