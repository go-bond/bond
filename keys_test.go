package bond

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyBuilder_AddInt16Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddInt16Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddInt16Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddInt16Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddInt32Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddInt32Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddInt32Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddInt32Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddInt64Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddInt64Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddInt64Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddInt64Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint16Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddUint16Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint32Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddUint32Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint64Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddUint64Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddStringField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddStringField("abc")

	assert.Equal(t, []byte{0x01, 'a', 'b', 'c'}, kb.Bytes())
}

func TestKeyBuilder_AddByteField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddByteField(0xF1)

	assert.Equal(t, []byte{0x01, 0xF1}, kb.Bytes())
}

func TestKeyBuilder_AddBytesField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddBytesField([]byte{0xF1, 0x1F})

	assert.Equal(t, []byte{0x01, 0xF1, 0x1F}, kb.Bytes())
}

func TestKeyBuilder_AddBigIntField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb = kb.AddBigIntField(big.NewInt(1), 32)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x01}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddBigIntField(big.NewInt(0), 32)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddBigIntField(big.NewInt(-1), 32)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF - 0x01}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddBigIntField(big.NewInt(1), 256)

	expected := append([]byte{0x01, 0x02}, make([]byte, 32)...)
	expected[33] = 0x01

	assert.Equal(t, expected, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddBigIntField(big.NewInt(0), 256)

	expected = append([]byte{0x01, 0x01}, make([]byte, 32)...)
	expected[33] = 0x00

	assert.Equal(t, expected, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb = kb.AddBigIntField(big.NewInt(-1), 256)

	expected = append([]byte{0x01, 0x00}, make([]byte, 32)...)
	for i := 2; i < len(expected)-1; i++ {
		expected[i] = 0xFF
	}
	expected[33] = 0xFF - 0x01

	assert.Equal(t, expected, kb.Bytes())
}

func TestKey_Encode_Decode(t *testing.T) {
	key := Key{
		TableID:    1,
		IndexID:    1,
		Index:      []byte("indexKey"),
		IndexOrder: []byte{},
		PrimaryKey: []byte("recordKey"),
	}

	keyRaw := KeyEncode(key)
	keyRawDirect := KeyEncodeRaw(
		key.TableID,
		key.IndexID,
		func(buff []byte) []byte {
			return append(buff, key.Index...)
		},
		func(buff []byte) []byte {
			return append(buff, key.IndexOrder...)
		},
		func(buff []byte) []byte {
			return append(buff, key.PrimaryKey...)
		},
	)
	keyReconstructed, _ := KeyDecode(keyRaw)

	assert.Equal(t, keyRaw, keyRawDirect)
	assert.Equal(t, key, keyReconstructed)
}

func TestKey_ToKeyPrefix(t *testing.T) {
	key := Key{
		TableID:    1,
		IndexID:    1,
		Index:      []byte("indexKey"),
		IndexOrder: []byte("orderKey"),
		PrimaryKey: []byte("recordKey"),
	}

	expectedPrefixKey := Key{
		TableID:    1,
		IndexID:    1,
		Index:      []byte("indexKey"),
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}

	expectedKeyPrefix := append([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x08}, []byte("indexKey")...)

	keyPrefix := key.ToKeyPrefix()

	assert.Equal(t, expectedPrefixKey, keyPrefix)
	assert.Equal(t, false, keyPrefix.IsDataKey())
	assert.Equal(t, true, keyPrefix.IsIndexKey())
	assert.Equal(t, true, keyPrefix.IsKeyPrefix())
	assert.Equal(t, expectedKeyPrefix, KeyEncode(keyPrefix))
	assert.Equal(t, expectedKeyPrefix, KeyEncodeRaw(
		key.TableID, key.IndexID, func(buff []byte) []byte {
			return append(buff, key.Index...)
		}, nil, nil))
}

func TestKey_ToDataKey(t *testing.T) {
	key := Key{
		TableID:    1,
		IndexID:    1,
		Index:      []byte("indexKey"),
		IndexOrder: []byte("orderKey"),
		PrimaryKey: []byte("recordKey"),
	}

	expectedTableKey := Key{
		TableID:    1,
		IndexID:    PrimaryIndexID,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("recordKey"),
	}

	expectedTableKeyRaw := append([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte("recordKey")...)

	tableKey := key.ToDataKey()

	assert.Equal(t, expectedTableKey, tableKey)
	assert.Equal(t, true, tableKey.IsDataKey())
	assert.Equal(t, false, tableKey.IsIndexKey())
	assert.Equal(t, false, tableKey.IsKeyPrefix())
	assert.Equal(t, expectedTableKeyRaw, KeyEncode(tableKey))
}

func TestKeyBytes(t *testing.T) {
	keyStruct := Key{
		TableID:    1,
		IndexID:    2,
		Index:      []byte{0x01, 0x02},
		IndexOrder: []byte{},
		PrimaryKey: []byte{0x02, 0x01},
	}

	keyBytes := KeyBytes(KeyEncode(keyStruct))

	assert.Equal(t, TableID(1), keyBytes.TableID())
	assert.Equal(t, IndexID(2), keyBytes.IndexID())
	assert.Equal(t, []byte{0x01, 0x02}, keyBytes.Index())
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
