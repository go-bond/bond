package bond

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const dbName = "test_db"

func setupDatabase(serializer ...Serializer[any]) DB {
	options := DefaultOptions()
	if len(serializer) > 0 && serializer[0] != nil {
		options.Serializer = serializer[0]
	}

	db, _ := Open(dbName, options)
	return db
}

func tearDownDatabase(db DB) {
	_ = db.Close()
	_ = os.RemoveAll(dbName)
}

func TestBond_Open(t *testing.T) {
	db, err := Open(dbName, &Options{})
	defer func() { _ = os.RemoveAll(dbName) }()

	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}
