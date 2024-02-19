package bond

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

const dbName = "test_db"

func setupDatabase(serializer ...Serializer[any]) DB {
	return setupDB(dbName, serializer...)
}

func setupDB(name string, serializer ...Serializer[any]) DB {
	options := &Options{}
	if len(serializer) > 0 && serializer[0] != nil {
		options.Serializer = serializer[0]
	}

	db, _ := Open(name, options)
	return db
}

func tearDownDatabase(db DB) {
	tearDownDB(dbName, db)
}

func tearDownDB(name string, db DB) {
	_ = db.Close()
	_ = os.RemoveAll(name)
}

func TestBond_Open(t *testing.T) {
	db, err := Open(dbName, &Options{})
	defer func() { _ = os.RemoveAll(dbName) }()

	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}

func TestBond_VersionCheck(t *testing.T) {
	defer func() { _ = os.RemoveAll(dbName) }()

	pebbleOpts := DefaultPebbleOptions()
	pebbleOpts.FormatMajorVersion = pebble.FormatPrePebblev1MarkedCompacted
	opts := DefaultOptions()
	opts.PebbleOptions = pebbleOpts

	db, err := Open(dbName, opts)
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)

	// simluate the db where VERSION file don't exist.
	pebbelVersionPath := filepath.Join(dbName, "bond", "PEBBLE_FORMAT_VERSION")
	err = os.Remove(pebbelVersionPath)
	require.NoError(t, err)

	// opening db should create a version file
	db, err = Open(dbName, opts)
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)

	_, err = os.Stat(pebbelVersionPath)
	require.NoError(t, err)
	buf, err := os.ReadFile(pebbelVersionPath)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", opts.PebbleOptions.FormatMajorVersion), string(buf))

	// rewrite the version with some other version.
	err = os.Remove(pebbelVersionPath)
	require.NoError(t, err)
	err = os.WriteFile(pebbelVersionPath,
		[]byte(fmt.Sprintf("%d", opts.PebbleOptions.FormatMajorVersion-1)), os.ModePerm)
	require.NoError(t, err)

	// throw an error since db is written in different version.
	_, err = Open(dbName, opts)
	require.Error(t, err)
}
