package bond

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	// BOND_DB_DATA_VERSION ..
	BOND_DB_DATA_VERSION = 1

	// BOND_DB_DATA_TABLE_ID ..
	BOND_DB_DATA_TABLE_ID = 0x0
)

func (db *DB) Version() int {
	value, _, err := db.Get(bondDataVersionKey())
	if err != nil {
		return 0
	}
	ver, _ := strconv.ParseInt(string(value), 10, 32)
	return int(ver)
}

func (db *DB) initVersion() error {
	if db.Version() > 0 {
		return nil
	}
	ver := fmt.Sprintf("%d", BOND_DB_DATA_VERSION)
	return db.Set(bondDataVersionKey(), []byte(ver), pebble.Sync)
}

func bondDataVersionKey() []byte {
	return _KeyEncode(_Key{
		BOND_DB_DATA_TABLE_ID,
		0,
		[]byte{},
		[]byte{},
		[]byte("__bond_db_data_version__"),
	})
}
