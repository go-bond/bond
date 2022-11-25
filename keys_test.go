package bond

import (
	"math/big"
	"testing"

	"github.com/go-bond/bond/utils"
	"github.com/stretchr/testify/assert"
)

func TestKeyBuilder_AddInt16Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddInt16Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x0a}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb.AddInt16Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00}, kb.Bytes())

	kb = NewKeyBuilder(buffer[:0])
	kb.AddInt16Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuildSizeEstimation(t *testing.T) {
	kb := NewKeyBuilder([]byte{}, true)

	kb.AddByteField(2)
	assert.Equal(t, 2, utils.SliceToInt(kb.Bytes()))

	kb.AddBytesField([]byte{1, 2})
	assert.Equal(t, 5, utils.SliceToInt(kb.Bytes()))

	kb.AddStringField("bond")
	assert.Equal(t, 10, utils.SliceToInt(kb.Bytes()))

	kb.AddInt16Field(1)
	assert.Equal(t, 14, utils.SliceToInt(kb.Bytes()))

	kb.AddInt32Field(2)
	assert.Equal(t, 20, utils.SliceToInt(kb.Bytes()))

	kb.AddInt64Field(10)
	assert.Equal(t, 30, utils.SliceToInt(kb.Bytes()))

	kb.AddUint64Field(20)
	assert.Equal(t, 39, utils.SliceToInt(kb.Bytes()))

	kb.AddUint32Field(21)
	assert.Equal(t, 44, utils.SliceToInt(kb.Bytes()))

	kb.AddUint16Field(22)
	assert.Equal(t, 47, utils.SliceToInt(kb.Bytes()))

	kb.AddBigIntField(big.NewInt(1), 32)
	assert.Equal(t, 53, utils.SliceToInt(kb.Bytes()))

	kb.AddBigIntField(big.NewInt(1), 256)
	assert.Equal(t, 87, utils.SliceToInt(kb.Bytes()))
}

func TestKeyBuilder_AddInt32Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddInt32Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddInt32Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddInt32Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddInt64Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddInt64Field(10)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddInt64Field(0)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddInt64Field(-10)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF - 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint16Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddUint16Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint32Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddUint32Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddUint64Field(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddUint64Field(10)

	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}, kb.Bytes())
}

func TestKeyBuilder_AddStringField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddStringField("abc")

	assert.Equal(t, []byte{0x01, 'a', 'b', 'c'}, kb.Bytes())
}

func TestKeyBuilder_AddByteField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddByteField(0xF1)

	assert.Equal(t, []byte{0x01, 0xF1}, kb.Bytes())
}

func TestKeyBuilder_AddBytesField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddBytesField([]byte{0xF1, 0x1F})

	assert.Equal(t, []byte{0x01, 0xF1, 0x1F}, kb.Bytes())
}

func TestKeyBuilder_AddBigIntField(t *testing.T) {
	var buffer [1024]byte

	kb := NewKeyBuilder(buffer[:0])
	kb.AddBigIntField(big.NewInt(1), 32)

	assert.Equal(t, []byte{0x01, 0x02, 0x00, 0x00, 0x00, 0x01}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddBigIntField(big.NewInt(0), 32)

	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddBigIntField(big.NewInt(-1), 32)

	assert.Equal(t, []byte{0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF - 0x01}, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddBigIntField(big.NewInt(1), 256)

	expected := append([]byte{0x01, 0x02}, make([]byte, 32)...)
	expected[33] = 0x01

	assert.Equal(t, expected, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddBigIntField(big.NewInt(0), 256)

	expected = append([]byte{0x01, 0x01}, make([]byte, 32)...)
	expected[33] = 0x00

	assert.Equal(t, expected, kb.Bytes())

	NewKeyBuilder(buffer[:0])
	kb.AddBigIntField(big.NewInt(-1), 256)

	expected = append([]byte{0x01, 0x00}, make([]byte, 32)...)
	for i := 2; i < len(expected)-1; i++ {
		expected[i] = 0xFF
	}
	expected[33] = 0xFF - 0x01

	assert.Equal(t, expected, kb.Bytes())
}

func TestKeyEncode_Pebble(t *testing.T) {
	testSubjects := []struct {
		PrimarySize    int
		IndexSize      int
		IndexOrderSize int
	}{
		{PrimarySize: 2, IndexSize: 1, IndexOrderSize: 1},
		{PrimarySize: 23, IndexSize: 21, IndexOrderSize: 83},
		{PrimarySize: 65, IndexSize: 21, IndexOrderSize: 34},
		{PrimarySize: 74, IndexSize: 56, IndexOrderSize: 43},
		{PrimarySize: 98, IndexSize: 26, IndexOrderSize: 52},
		{PrimarySize: 45, IndexSize: 83, IndexOrderSize: 75},
		{PrimarySize: 54, IndexSize: 32, IndexOrderSize: 38},
		{PrimarySize: 65, IndexSize: 53, IndexOrderSize: 94},
	}

	for _, testSubject := range testSubjects {
		// normal encoding
		primary := make([]byte, testSubject.PrimarySize)
		index := make([]byte, testSubject.IndexSize)
		order := make([]byte, testSubject.IndexOrderSize)
		v1 := KeyEncode(Key{
			TableID:    1,
			IndexID:    1,
			IndexKey:   index,
			IndexOrder: order,
			PrimaryKey: primary,
		})

		// key bytes encoding
		size := KeySize(testSubject.PrimarySize, testSubject.IndexSize, testSubject.IndexOrderSize)
		buf := KeyBytes(make([]byte, size))
		opt := KeyEncodeOption{
			TableID: 1,
			IndexID: 1,
			EncodePrimaryKey: func(kb KeyBytes) KeyBytes {
				kb = kb[:0]
				kb = append(kb, make([]byte, testSubject.PrimarySize)...)
				return kb
			},
			EncodeIndexKey: func(kb KeyBytes) KeyBytes {
				kb = kb[:0]
				kb = append(kb, make([]byte, testSubject.IndexSize)...)
				return kb
			},
			EncodeIndexOrder: func(kb KeyBytes) KeyBytes {
				kb = kb[:0]
				kb = append(kb, make([]byte, testSubject.IndexOrderSize)...)
				return kb
			},
		}
		v2 := buf.Encode(opt)

		assert.Equal(t, v1, []byte(v2))
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
		kb := NewKeyBuilder(buffer[:0])
		kb.AddUint32Field(uint32(1))
		kb.AddBytesField([]byte("0xacd12312jasjjjasjdbasbdsabdab"))
		kb.AddBytesField([]byte("0xacd32121jasjjjasjdbasbdsabdab"))
		kb.Bytes()
	}
}
