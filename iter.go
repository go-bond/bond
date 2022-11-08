package bond

import "github.com/cockroachdb/pebble"

type IterOptions struct {
	pebble.IterOptions
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

func pebbleIterOptions(opt *IterOptions) *pebble.IterOptions {
	if opt == nil {
		return &pebble.IterOptions{}
	} else {
		return &opt.IterOptions
	}
}
