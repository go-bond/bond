package bond

import "github.com/cockroachdb/pebble"

type DB struct {
	*pebble.DB
}

func Open(dirname string, opts *pebble.Options) (*DB, error) {
	pdb, err := pebble.Open(dirname, opts)
	if err != nil {
		return nil, err
	}

	db := &DB{DB: pdb}
	return db, nil
}

func (db *DB) Close() error {
	return db.Close()
}
