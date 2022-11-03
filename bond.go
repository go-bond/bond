package bond

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/serializers"
)

type DB struct {
	*pebble.DB

	serializer Serializer[any]
}

func Open(dirname string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if opts.PebbleOptions == nil {
		opts.PebbleOptions = DefaultPebbleOptions()
	}

	opts.PebbleOptions.Comparer = DefaultPebbleComparer()

	pdb, err := pebble.Open(dirname, opts.PebbleOptions)
	if err != nil {
		return nil, err
	}

	var serializer Serializer[any]
	if opts.Serializer != nil {
		serializer = opts.Serializer
	} else {
		serializer = &serializers.JsonSerializer{}
	}

	db := &DB{DB: pdb, serializer: serializer}

	if db.Version() == 0 {
		if err := db.initVersion(); err != nil {
			return nil, err
		}
	} else if db.Version() != BOND_DB_DATA_VERSION {
		return nil, fmt.Errorf("bond db version is %d but expecting %d", db.Version(), BOND_DB_DATA_VERSION)
	}

	return db, nil
}

func (db *DB) Serializer() Serializer[any] {
	return db.serializer
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func (db *DB) getKV(key []byte, batch *pebble.Batch) (data []byte, closer io.Closer, err error) {
	if batch != nil {
		data, closer, err = batch.Get(key)
	} else {
		data, closer, err = db.Get(key)
	}
	return
}
