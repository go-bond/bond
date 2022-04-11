package bond_test

import (
	"log"
	mrand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	DB *bond.DB
)

func init() {
	mrand.Seed(time.Now().UnixNano())
}

func TestMain(m *testing.M) {
	var err error
	var dir string
	// dir, err = ioutil.TempDir("db", "bond")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer os.RemoveAll(dir)
	// fmt.Println("==> dir", dir)
	dir = "demo"

	DB, err = bond.Open(dir, &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer DB.Close()

	code := m.Run()
	os.Exit(code)
}

func TestSimpleKV(t *testing.T) {
	key := []byte("simplek")
	val := []byte("simplev")

	err := DB.Set(key, val, pebble.Sync)
	require.NoError(t, err)

	val2, _, err := DB.Get(key)
	assert.NoError(t, err)
	require.Equal(t, val, val2)
}

// TODO:
// so.. lets make a table of records..
// then, we need an inverted index for some of the columns, etc.
// we'll need some compactation thing too.. and lets see what else..
