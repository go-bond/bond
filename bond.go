package bond

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

var KeyPrefixSeparator = []byte{'-'}

type Options struct {
	pebble.Options

	RecordSerializer Serializer
}

type DB struct {
	*pebble.DB

	serializer Serializer
}

func Open(dirname string, opts *Options) (*DB, error) {
	if opts.Comparer == nil {
		opts.Comparer = pebble.DefaultComparer
		opts.Comparer.Split = keyPrefixSplitFunc
	}

	pdb, err := pebble.Open(dirname, &opts.Options)
	if err != nil {
		return nil, err
	}

	var serializer Serializer
	if opts.RecordSerializer != nil {
		serializer = opts.RecordSerializer
	} else {
		serializer = &JsonSerializer{}
	}

	return &DB{DB: pdb, serializer: serializer}, nil
}

func (db *DB) Serializer() Serializer {
	return db.serializer
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func keyPrefixSplitFunc(a []byte) int {
	for i := len(a) - 1; i > 0; i-- {
		if bytes.Compare(a[i:i+len(KeyPrefixSeparator)], KeyPrefixSeparator) == 0 {
			return i
		}
	}
	return 0
}
