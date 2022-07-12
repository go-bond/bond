package bond

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

type Options struct {
	pebble.Options

	RecordSerializer Serializer
}

type DB struct {
	*pebble.DB

	recordSerializer Serializer

	lastTableID uint32
}

func Open(dirname string, opts *Options) (*DB, error) {
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

	return &DB{DB: pdb, recordSerializer: serializer}, nil
}

func (db *DB) RecordSerializer() Serializer {
	return db.recordSerializer
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func (db *DB) nextTableId() TableID {
	return TableID(atomic.AddUint32(&db.lastTableID, 1))
}
