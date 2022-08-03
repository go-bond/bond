package bond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKey_Encode_Decode(t *testing.T) {
	key := _Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte{},
		PrimaryKey: []byte("recordKey"),
	}

	keyRaw := _KeyEncode(key)
	keyReconstructed := _KeyDecode(keyRaw)

	assert.Equal(t, key, keyReconstructed)
}

func TestKey_ToKeyPrefix(t *testing.T) {
	key := _Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte("orderKey"),
		PrimaryKey: []byte("recordKey"),
	}

	expectedPrefixKey := _Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}

	expectedKeyPrefix := append([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x08}, []byte("indexKey")...)

	keyPrefix := key.ToKeyPrefix()

	assert.Equal(t, expectedPrefixKey, keyPrefix)
	assert.Equal(t, false, keyPrefix.IsDataKey())
	assert.Equal(t, true, keyPrefix.IsIndexKey())
	assert.Equal(t, true, keyPrefix.IsKeyPrefix())
	assert.Equal(t, expectedKeyPrefix, _KeyEncode(keyPrefix))
}

func TestKey_ToDataKey(t *testing.T) {
	key := _Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte("orderKey"),
		PrimaryKey: []byte("recordKey"),
	}

	expectedTableKey := _Key{
		TableID:    1,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("recordKey"),
	}

	expectedTableKeyRaw := append([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte("recordKey")...)

	tableKey := key.ToDataKey()

	assert.Equal(t, expectedTableKey, tableKey)
	assert.Equal(t, true, tableKey.IsDataKey())
	assert.Equal(t, false, tableKey.IsIndexKey())
	assert.Equal(t, false, tableKey.IsKeyPrefix())
	assert.Equal(t, expectedTableKeyRaw, _KeyEncode(tableKey))
}

func Benchmark_KeyBuilder(b *testing.B) {
	buffer := make([]byte, 0, 512)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewKeyBuilder(buffer[:0]).
			AddUint32Field(uint32(1)).
			AddBytesField([]byte("0xacd12312jasjjjasjdbasbdsabdab")).
			AddBytesField([]byte("0xacd32121jasjjjasjdbasbdsabdab")).
			Bytes()
	}
}
