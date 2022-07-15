package tests

import (
	"os"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
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

func TestBond_Key_Encode_Decode(t *testing.T) {
	key := bond.Key{
		TableID:   1,
		IndexID:   1,
		IndexKey:  []byte("indexKey"),
		RecordKey: []byte("recordKey"),
	}

	keyRaw := bond.KeyEncode(key)
	keyReconstructed := bond.KeyDecode(keyRaw)

	assert.Equal(t, key, keyReconstructed)
}

func TestBond_Key_ToPrefix(t *testing.T) {
	key := bond.Key{
		TableID:   1,
		IndexID:   1,
		IndexKey:  []byte("indexKey"),
		RecordKey: []byte("recordKey"),
	}

	expectedPrefixKey := bond.Key{
		TableID:   1,
		IndexID:   1,
		IndexKey:  []byte("indexKey"),
		RecordKey: []byte{},
	}

	expectedPrefix := append([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x08}, []byte("indexKey")...)

	keyPrefix := key.ToPrefix()

	assert.Equal(t, expectedPrefixKey, keyPrefix)
	assert.Equal(t, false, keyPrefix.IsTableKey())
	assert.Equal(t, true, keyPrefix.IsIndexKey())
	assert.Equal(t, true, keyPrefix.IsPrefix())
	assert.Equal(t, expectedPrefix, bond.KeyEncode(keyPrefix))
}

func TestBond_Key_ToTableKey(t *testing.T) {
	key := bond.Key{
		TableID:   1,
		IndexID:   1,
		IndexKey:  []byte("indexKey"),
		RecordKey: []byte("recordKey"),
	}

	expectedTableKey := bond.Key{
		TableID:   1,
		IndexID:   bond.MainIndexID,
		IndexKey:  []byte{},
		RecordKey: []byte("recordKey"),
	}

	expectedTableKeyRaw := append([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte("recordKey")...)

	tableKey := key.ToTableKey()

	assert.Equal(t, expectedTableKey, tableKey)
	assert.Equal(t, true, tableKey.IsTableKey())
	assert.Equal(t, false, tableKey.IsIndexKey())
	assert.Equal(t, false, tableKey.IsPrefix())
	assert.Equal(t, expectedTableKeyRaw, bond.KeyEncode(tableKey))
}
