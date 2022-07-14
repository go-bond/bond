package tests

import (
	"os"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

const dbName = "demo"

func setupDatabase(serializer ...bond.Serializer) *bond.DB {
	options := &bond.Options{}
	if len(serializer) > 0 && serializer[0] != nil {
		options.Serializer = serializer[0]
	}

	db, _ := bond.Open(dbName, options)
	return db
}

func tearDownDatabase(db *bond.DB) {
	_ = db.Close()
	_ = os.RemoveAll(dbName)
}

func TestBond_Open(t *testing.T) {
	db, err := bond.Open(dbName, &bond.Options{})
	defer func() { _ = os.RemoveAll(dbName) }()

	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}
