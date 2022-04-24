package bond_test

import (
	"fmt"
	"log"
	mrand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/davecgh/go-spew/spew"
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

	valChk, _, err := DB.Get(key)
	assert.NoError(t, err)
	require.Equal(t, val, valChk)
}

func TestRand(t *testing.T) {
	DB.Set([]byte("15"), []byte("1"), pebble.Sync)
	DB.Set([]byte("2"), []byte("1"), pebble.Sync)
	DB.Set([]byte("12"), []byte("1"), pebble.Sync)
	DB.Set([]byte("20"), []byte("1"), pebble.Sync)
	DB.Set([]byte("1"), []byte("1"), pebble.Sync)
	DB.Set([]byte("30"), []byte("1"), pebble.Sync)
	DB.Set([]byte("3"), []byte("1"), pebble.Sync)

	iter := DB.NewIter(&pebble.IterOptions{
		// LowerBound: []byte("1"), UpperBound: []byte("10"),
		OnlyReadGuaranteedDurable: true,
	})
	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Println(string(iter.Key()))
	}

}

func TestRecord(t *testing.T) {
	account := &Account{
		ID:      1,
		Name:    "Peter",
		Address: "0xabc",
	}

	key := account.Key()
	val, err := account.Value()
	require.NoError(t, err)

	err = DB.Set(key, val, pebble.Sync)
	require.NoError(t, err)

	valChk, _, err := DB.Get(key)
	assert.NoError(t, err)
	require.Equal(t, val, valChk)

	spew.Dump(string(valChk))
}

// TODO: lets make an account generator with an Id.. the name will be random chars..

// TODO: lets add the concept of a table though..
// think,, "insert into table" ..
// the idea of a table is its the metaphor, and maybe we have bond.Table or, bond.Store, etc.
// and we have .Set and .Get() and then .Query(), which will use indexes etc.
// we need the bond.Store metaphor, because,

func TestCreate(t *testing.T) {
	makeRecords(t, 1000, 1500)
}

func TestGetRecords(t *testing.T) {
	for i := 1000; i < 1500; i++ {
		val, _, err := DB.Get(accountKey(uint32(i)))
		require.NoError(t, err)
		require.NotEmpty(t, val)
		// fmt.Println("=>", string(val))
	}
}

func TestIter(t *testing.T) {
	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil // no upper-bound
	}

	// fmt.Println("??", string(keyUpperBound([]byte("account/"))))
	// fmt.Println("ok1", []byte("/"))
	// fmt.Println("ok2", []byte("0"))
	// return

	iter := DB.NewIter(&pebble.IterOptions{
		LowerBound: []byte("account/"),
		// UpperBound: []byte("account0"),
		UpperBound: keyUpperBound([]byte("account/")),
	})

	// iter := DB.NewIter(nil)

	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Printf("%s\n", iter.Key())
		// TODO: can stop the scan based on key, or whatever, etc.
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

func TestIterSeek(t *testing.T) {
	iter := DB.NewIter(nil)
	if iter.SeekGE([]byte("account/1002")); iter.Valid() {
		fmt.Printf("%s\n", iter.Key())
	}
	for iter.Next() {
		fmt.Printf("next %s\n", iter.Key())
	}
	// iter.SeekGEWithLimit()
}

func TestIterIdx(t *testing.T) {
	iter := DB.NewIter(nil)
	if iter.SeekGE([]byte("idx/account/name/*1469*")); iter.Valid() {
		fmt.Printf("%s\n", iter.Key())
	}
	for iter.Next() {
		fmt.Printf("next %s\n", iter.Key())
	}

	// iter.SeekGEWithLimit()
}

func makeRecords(t *testing.T, start, end int) {
	batch := DB.NewBatch()

	for i := start; i < end; i++ {
		account := &Account{
			ID:   uint32(i),
			Name: fmt.Sprintf("*%d*", i),
		}
		key := account.Key()
		val, err := account.Value()
		require.NoError(t, err)

		// err = DB.Set(key, val, pebble.NoSync)
		err = batch.Set(key, val, nil)
		require.NoError(t, err)

		// also create the index at same time
		// .. hmm.. for each record, could be a bunch of indexes, yes, its true..
		idxKey1 := []byte(fmt.Sprintf("idx/account/name/%s", account.Name))
		idxVal1 := []byte(fmt.Sprintf("%d", account.ID))
		err = batch.Set(idxKey1, idxVal1, nil)
		require.NoError(t, err)
	}

	err := batch.Commit(pebble.Sync)
	require.NoError(t, err)
}

func TestIndex(t *testing.T) {
	fmt.Println("hi")

	// idx1
	idx := bond.Index{
		Table:     0,
		EntryType: 1,
		Column:    []byte("accountName/"),
		Value:     []byte("peter"),
		ID:        []byte{1},
	}
	err := DB.Set(idx.Key(), nil, pebble.Sync)
	require.NoError(t, err)
	// spew.Dump(idx.Key())

	// idx2
	idx2 := bond.Index{
		Table:     0,
		EntryType: 1,
		Column:    []byte("accountName/"),
		Value:     []byte("julia"),
		ID:        []byte{2},
	}
	err = DB.Set(idx2.Key(), nil, pebble.Sync)
	require.NoError(t, err)
	// spew.Dump(idx.Key())

	// idx3
	idx3 := bond.Index{
		Table:     0,
		EntryType: 1,
		Column:    []byte("contractAddress/"),
		Value:     []byte("0xa"),
		ID:        []byte{1},
	}
	err = DB.Set(idx3.Key(), nil, pebble.Sync)
	require.NoError(t, err)

	// idx4
	idx4 := bond.Index{
		Table:     0,
		EntryType: 1,
		Column:    []byte("contractAddress/"),
		Value:     []byte("0xb"),
		ID:        []byte{2},
	}
	err = DB.Set(idx4.Key(), nil, pebble.Sync)
	require.NoError(t, err)

	//--

	// z := seekIndex(0, []byte("contractAddress/"))
	z := seekIndex(0, []byte("accountAddress/"))
	z2 := seekIndex(0, []byte("b"))

	// iter := DB.NewIter(nil)
	iter := DB.NewIter(&pebble.IterOptions{
		LowerBound: z, UpperBound: z2,
		OnlyReadGuaranteedDurable: true,
	})

	if iter.SeekGE(z); iter.Valid() {
		// fmt.Printf("%s\n", iter.Key())
		spew.Dump(iter.Key())
	}
	for iter.Next() {
		// fmt.Printf("next %s\n", iter.Key())
		spew.Dump(iter.Key())
	}
}

func seekIndex(tableID bond.TableID, column []byte) []byte {
	b := []byte{byte(tableID), 1}
	b = append(b, column...)
	return b
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}
