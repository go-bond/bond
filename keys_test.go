package bond

import (
	"math/big"
	"testing"

	"github.com/go-bond/bond/utils"
	"github.com/stretchr/testify/assert"
)

func TestKeyBuilder_AddInt16Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt16Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt16Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt16Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuildSizeEstimation(t *testing.T) {
	kb := NewKeyBuilder([]byte{}, true)

	kb = kb.AddByteField(2)
	assert.Equal(t, 2, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddBytesField([]byte{1, 2})
	assert.Equal(t, 5, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddStringField("bond")
	assert.Equal(t, 10, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddInt16Field(1)
	assert.Equal(t, 14, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddInt32Field(2)
	assert.Equal(t, 20, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddInt64Field(10)
	assert.Equal(t, 30, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddUint64Field(20)
	assert.Equal(t, 39, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddUint32Field(21)
	assert.Equal(t, 44, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddUint16Field(22)
	assert.Equal(t, 47, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddBigIntField(big.NewInt(1), 32)
	assert.Equal(t, 53, utils.SliceToInt(kb.Bytes()))

	kb = kb.AddBigIntField(big.NewInt(1), 256)
	assert.Equal(t, 87, utils.SliceToInt(kb.Bytes()))
}

func TestKeyBuilder_AddInt32Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt32Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt32Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt32Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddInt64Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt64Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt64Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddInt64Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint16Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddUint16Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint32Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddUint32Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint64Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddUint64Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddStringField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddStringField("abc")

	assert.Equal(t, []byte{0x01, 'a', 'b', 'c'}, kb.Bytes())
}

func TestKeyBuilder_AddByteField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddByteField(0xF1)

	assert.Equal(t, []byte{0x01, 0xF1}, kb.Bytes())
}

func TestKeyBuilder_AddBytesField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBytesField([]byte{0xF1, 0x1F})

	assert.Equal(t, []byte{0x01, 0xF1, 0x1F}, kb.Bytes())
}

func TestKeyBuilder_AddBigIntField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBigIntField(big.NewInt(1), 32)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x01}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBigIntField(big.NewInt(0), 32)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBigIntField(big.NewInt(-1), 32)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF - 0x01}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBigIntField(big.NewInt(1), 256)

	expected := append([]byte{0x01, 0x02}, make([]byte, 32)...)
	expected[33] = 0x01

	assert.Equal(t, expected, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBigIntField(big.NewInt(0), 256)

	expected = append([]byte{0x01, 0x01}, make([]byte, 32)...)
	expected[33] = 0x00

	assert.Equal(t, expected, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0], false)
	kb = kb.AddBigIntField(big.NewInt(-1), 256)

	expected = append([]byte{0x01, 0x00}, make([]byte, 32)...)
	for i := 2; i < len(expected)-1; i++ {
		expected[i] = 0xFF
	}
	expected[33] = 0xFF - 0x01

	assert.Equal(t, expected, kb.Bytes())
}

func TestKeyEncode_Pebble(t *testing.T) {
	testSubjects := []KeySizeInfo{
		KeySize(10, 54, 32),
		KeySize(23, 21, 83),
		KeySize(65, 21, 34),
		KeySize(74, 56, 43),
		KeySize(98, 26, 52),
		KeySize(45, 83, 75),
		KeySize(54, 32, 38),
		KeySize(65, 53, 94),
	}

	for _, testSubject := range testSubjects {
		// normal encoding
		primary := make([]byte, testSubject.Total-(testSubject.IndexOrderPos+testSubject.IndexOrderSize))
		index := make([]byte, testSubject.IndexSize)
		order := make([]byte, testSubject.IndexOrderSize)
		v1 := KeyEncode(Key{
			TableID:    1,
			IndexID:    1,
			IndexKey:   index,
			IndexOrder: order,
			PrimaryKey: primary,
		})

		// pebble encoding
		buf := make([]byte, testSubject.Total)
		v2 := KeyEncodePebble(KeyV2{
			TableID: 1,
			IndexID: 1,
			Info:    testSubject,
		}, buf)

		assert.Equal(t, v1, v2)
	}
}

func TestKey_Encode_Decode(t *testing.T) {
	key := Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte{},
		PrimaryKey: []byte("recordKey"),
	}

	keyRaw := KeyEncode(key)
	keyReconstructed := KeyDecode(keyRaw)

	assert.Equal(t, key, keyReconstructed)
}

func TestKey_ToKeyPrefix(t *testing.T) {
	key := Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte("orderKey"),
		PrimaryKey: []byte("recordKey"),
	}

	expectedPrefixKey := Key{
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
	assert.Equal(t, expectedKeyPrefix, KeyEncode(keyPrefix))
}

func TestKey_ToDataKey(t *testing.T) {
	key := Key{
		TableID:    1,
		IndexID:    1,
		IndexKey:   []byte("indexKey"),
		IndexOrder: []byte("orderKey"),
		PrimaryKey: []byte("recordKey"),
	}

	expectedTableKey := Key{
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
	assert.Equal(t, expectedTableKeyRaw, KeyEncode(tableKey))
}

func TestKeyBytes(t *testing.T) {
	keyStruct := Key{
		TableID:    1,
		IndexID:    2,
		IndexKey:   []byte{0x01, 0x02},
		IndexOrder: []byte{},
		PrimaryKey: []byte{0x02, 0x01},
	}

	keyBytes := KeyBytes(KeyEncode(keyStruct))

	assert.Equal(t, TableID(1), keyBytes.TableID())
	assert.Equal(t, IndexID(2), keyBytes.IndexID())
	assert.Equal(t, []byte{0x01, 0x02}, keyBytes.IndexKey())
}

func Benchmark_KeyBuilder(b *testing.B) {
	buffer := make([]byte, 0, 512)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewKeyBuilder(buffer[:0], false).
			AddUint32Field(uint32(1)).
			AddBytesField([]byte("0xacd12312jasjjjasjdbasbdsabdab")).
			AddBytesField([]byte("0xacd32121jasjjjasjdbasbdsabdab")).
			Bytes()
	}
}
