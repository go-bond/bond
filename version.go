package bond

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	// BOND_DB_DATA_VERSION ..
	BOND_DB_DATA_VERSION = 1
)

func (db *_db) Version() int {
	value, closer, err := db.pebble.Get(bondDataVersionKey())
	if err != nil {
		return 0
	}
	defer func() { _ = closer.Close() }()

	ver, _ := strconv.ParseInt(string(value), 10, 32)
	return int(ver)
}

func (db *_db) initVersion() error {
	if db.Version() > 0 {
		return nil
	}
	ver := fmt.Sprintf("%d", BOND_DB_DATA_VERSION)
	return db.pebble.Set(bondDataVersionKey(), []byte(ver), pebble.Sync)
}

func bondDataVersionKey() []byte {
	return KeyEncode(Key{
		BOND_DB_DATA_TABLE_ID,
		0,
		[]byte{},
		[]byte{},
		[]byte("__bond_db_data_version__"),
	})
}

func bondTableKey(tableID TableID, key string) []byte {
	return KeyEncode(Key{
		BOND_DB_DATA_TABLE_ID,
		IndexID(tableID),
		[]byte{},
		[]byte{},
		[]byte(key),
	})
}
