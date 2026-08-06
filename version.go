package bond

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	// BOND_DB_DATA_VERSION ..
	BOND_DB_DATA_VERSION             = 1
	bondInitializationPendingVersion = "1"
)

func (db *_db) Version() int {
	version, exists, err := readBondDataVersion(db.pebble)
	if err != nil || !exists {
		return 0
	}
	return version
}

func readBondDataVersion(db *pebble.DB) (version int, exists bool, err error) {
	value, closer, err := db.Get(bondDataVersionKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("bond: read database version: %w", err)
	}
	data := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		return 0, false, fmt.Errorf("bond: close database version value: %w", err)
	}
	parsed, err := strconv.ParseInt(string(data), 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("bond: parse database version %q: %w", data, err)
	}
	return int(parsed), true, nil
}

func readBondInitializationPending(db *pebble.DB) (bool, error) {
	value, closer, err := db.Get(bondInitializationPendingKey())
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("bond: read pending database initialization: %w", err)
	}
	data := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		return false, fmt.Errorf("bond: close pending database initialization value: %w", err)
	}
	if string(data) != bondInitializationPendingVersion {
		return false, fmt.Errorf("bond: unsupported pending database initialization version %q", data)
	}
	return true, nil
}

func markBondInitializationPending(db *pebble.DB) error {
	if err := db.Set(
		bondInitializationPendingKey(),
		[]byte(bondInitializationPendingVersion),
		pebble.Sync,
	); err != nil {
		return fmt.Errorf("bond: mark pending database initialization: %w", err)
	}
	return nil
}

func initializeBondMetadata(db *pebble.DB, catalog *Catalog) error {
	batch := db.NewBatch()
	defer batch.Close()
	if catalog != nil {
		definition, err := encodeCatalogDefinition(catalog)
		if err != nil {
			return err
		}
		if err := batch.Set(catalogDefinitionKey(), definition, pebble.NoSync); err != nil {
			return fmt.Errorf("bond: initialize catalog definition: %w", err)
		}
	}
	version := []byte(strconv.Itoa(BOND_DB_DATA_VERSION))
	if err := batch.Set(bondDataVersionKey(), version, pebble.NoSync); err != nil {
		return fmt.Errorf("bond: initialize database version: %w", err)
	}
	if err := batch.Delete(bondInitializationPendingKey(), pebble.NoSync); err != nil {
		return fmt.Errorf("bond: clear pending database initialization: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("bond: initialize database metadata: %w", err)
	}
	return nil
}

func bondInitializationPendingKey() []byte {
	return KeyEncode(Key{
		BOND_DB_DATA_TABLE_ID,
		0,
		[]byte{},
		[]byte{},
		[]byte("__bond_db_initialization_pending__"),
	})
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
