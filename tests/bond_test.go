package tests

import (
	"os"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

const dbName = "demo"

func setupDatabase() *bond.DB {
	db, _ := bond.Open(dbName, &bond.Options{})
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
