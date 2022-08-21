package bond

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	// BOND_DB_DATA_VERSION ..
	BOND_DB_DATA_VERSION = 1

	// BOND_DB_DATA_VERSION_KEY ..
	BOND_DB_DATA_VERSION_KEY = "__bond_db_version__"
)

func (db *DB) Version() int {
	value, _, err := db.Get([]byte(BOND_DB_DATA_VERSION_KEY))
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
	return db.Set([]byte(BOND_DB_DATA_VERSION_KEY), []byte(ver), pebble.Sync)
}
