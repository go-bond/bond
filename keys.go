package bond

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
)

type KeyBuilderSpec interface {
	AddInt64Field(i int64)
	AddInt32Field(i int32)
	AddInt16Field(i int16)
	AddUint64Field(i uint64)
	AddUint32Field(i uint32)
	AddUint16Field(i uint16)
	AddByteField(btt byte)
	AddStringField(s string)
	AddBytesField(bs []byte)
	AddBigIntField(bi *big.Int, bits int)

	Bytes() []byte
	Reset(buf []byte)
}

var keyBuilderPool = sync.Pool{
	New: func() any {
		return NewKeyBuilder([]byte{})
	},
}

var keySizeBuilderPool = sync.Pool{
	New: func() any {
		return NewKeySizeBuilder()
	},
}

type KeySizeBuilder struct {
	size int
}

var _ KeyBuilderSpec = &KeySizeBuilder{}

func NewKeySizeBuilder() *KeySizeBuilder {
	return &KeySizeBuilder{}
}

func (b *KeySizeBuilder) Reset(buf []byte) {
	b.size = 0
}

func (b *KeySizeBuilder) AddInt64Field(i int64) {
	// type + int64
	b.addSize(1 + 1 + 8)
}

func (b *KeySizeBuilder) AddInt32Field(i int32) {
	// type + int32
	b.addSize(1 + 1 + 4)
}

func (b *KeySizeBuilder) AddInt16Field(i int16) {
	// type + int16
	b.addSize(1 + 1 + 2)
}

func (b *KeySizeBuilder) AddUint64Field(i uint64) {
	// uint64
	b.addSize(1 + 8)
}

func (b *KeySizeBuilder) AddUint32Field(i uint32) {
	// uint32
	b.addSize(1 + 4)
}

func (b *KeySizeBuilder) AddUint16Field(i uint16) {
	// uint16
	b.addSize(1 + 2)
}

func (b *KeySizeBuilder) AddByteField(btt byte) {
	// byte
	b.addSize(1 + 1)
}

func (b *KeySizeBuilder) AddStringField(s string) {
	// string
	b.addSize(1 + len(s))
}

func (b *KeySizeBuilder) AddBytesField(bs []byte) {
	// bytes
	b.addSize(1 + len(bs))
}

func (b *KeySizeBuilder) AddBigIntField(bi *big.Int, bits int) {
	bytesLen := bits / 8
	// sign + bytesLen
	b.addSize(1 + 1 + bytesLen)
}

func (b *KeySizeBuilder) addSize(size int) {
	b.size += size
}

func (b *KeySizeBuilder) Bytes() []byte {
	return utils.IntToSlice(b.size)
}

type KeyBuilder struct {
	buff *utils.Buffer
	fid  byte
}

var _ KeyBuilderSpec = &KeyBuilder{}

func NewKeyBuilder(buff []byte, estimateSizeOnly ...bool) *KeyBuilder {
	return &KeyBuilder{buff: utils.NewBuffer(buff[:0])}
}

func (b *KeyBuilder) Reset(buf []byte) {
	b.buff.UseNewSrc(buf)
	b.fid = 0
}

func (b *KeyBuilder) AddInt64Field(i int64) {
	b.putFieldID()

	if i > 0 {
		_ = b.buff.WriteByte(0x02)
	} else if i == 0 {
		_ = b.buff.WriteByte(0x01)
	} else {
		_ = b.buff.WriteByte(0x00)
		i = ^-i
	}

	binary.BigEndian.PutUint64(b.buff.Extend(8), uint64(i))
}

func (b *KeyBuilder) AddInt32Field(i int32) {
	b.putFieldID()

	if i > 0 {
		_ = b.buff.WriteByte(0x02)
	} else if i == 0 {
		_ = b.buff.WriteByte(0x01)
	} else {
		_ = b.buff.WriteByte(0x00)
		i = ^-i
	}

	binary.BigEndian.PutUint32(b.buff.Extend(4), uint32(i))
}

func (b *KeyBuilder) AddInt16Field(i int16) {
	b.putFieldID()

	if i > 0 {
		_ = b.buff.WriteByte(0x02)
	} else if i == 0 {
		_ = b.buff.WriteByte(0x01)
	} else {
		_ = b.buff.WriteByte(0x00)
		i = ^-i
	}

	binary.BigEndian.PutUint16(b.buff.Extend(2), uint16(i))
}

func (b *KeyBuilder) AddUint64Field(i uint64) {
	b.putFieldID()
	binary.BigEndian.PutUint64(b.buff.Extend(8), i)
}

func (b *KeyBuilder) AddUint32Field(i uint32) {
	b.putFieldID()
	binary.BigEndian.PutUint32(b.buff.Extend(4), i)
}

func (b *KeyBuilder) AddUint16Field(i uint16) {
	b.putFieldID()
	binary.BigEndian.PutUint16(b.buff.Extend(2), i)
}

func (b *KeyBuilder) AddByteField(btt byte) {
	b.putFieldID()
	_ = b.buff.WriteByte(btt)
}

func (b *KeyBuilder) AddStringField(s string) {
	b.putFieldID()
	b.buff.WriteString(s)
}

func (b *KeyBuilder) AddBytesField(bs []byte) {
	b.putFieldID()
	_, _ = b.buff.Write(bs)
}

func (b *KeyBuilder) AddBigIntField(bi *big.Int, bits int) {
	b.putFieldID()

	bytesLen := bits / 8
	sign := bi.Sign() + 1
	b.buff.WriteByte(byte(sign)) // 0 - negative, 1 - zero, 2 - positive

	if sign == 0 {
		bi = big.NewInt(0).Sub(big.NewInt(0), bi)
	}

	bi.FillBytes(b.buff.Extend(bytesLen))

	if sign == 0 {
		srcBuf := b.buff.Bytes()
		index := b.buff.Len()
		for i := 0; i < bytesLen; i++ {
			srcBuf[index-i-1] = 0xFF - srcBuf[index-i-1]
		}
	}

}

func (b *KeyBuilder) putFieldID() {
	_ = b.buff.WriteByte(b.fid)
	b.fid = +1
}

func (b *KeyBuilder) Bytes() []byte {
	return b.buff.Bytes()
}

type Key struct {
	TableID    TableID
	IndexID    IndexID
	IndexKey   []byte
	IndexOrder []byte
	PrimaryKey []byte
}

func NewUserKey(key string) KeyBytes {
	return KeyEncode(Key{
		TableID:    BOND_DB_DATA_TABLE_ID,
		IndexID:    BOND_DB_DATA_USER_SPACE_INDEX_ID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte(key),
	})
}

func (k Key) ToDataKey() Key {
	return Key{
		TableID:    k.TableID,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: k.PrimaryKey,
	}
}

func (k Key) ToKeyPrefix() Key {
	return Key{
		TableID:    k.TableID,
		IndexID:    k.IndexID,
		IndexKey:   k.IndexKey,
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}
}

func (k Key) IsDataKey() bool {
	return k.IndexID == PrimaryIndexID && len(k.IndexKey) == 0
}

func (k Key) IsIndexKey() bool {
	return k.IndexID != PrimaryIndexID && len(k.IndexKey) != 0
}

func (k Key) IsKeyPrefix() bool {
	return len(k.PrimaryKey) == 0
}

func KeyEncode(key Key, rawBuffs ...[]byte) []byte {
	var rawBuff []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		rawBuff = rawBuffs[0]
	}

	buff := bytes.NewBuffer(rawBuff)
	buff.Write([]byte{byte(key.TableID)})
	buff.Write([]byte{byte(key.IndexID)})

	var indexLenBuff [4]byte
	binary.BigEndian.PutUint32(indexLenBuff[:4], uint32(len(key.IndexKey)))
	buff.Write(indexLenBuff[:4])
	buff.Write(key.IndexKey)

	if !key.IsKeyPrefix() {
		binary.BigEndian.PutUint32(indexLenBuff[:4], uint32(len(key.IndexOrder)))
		buff.Write(indexLenBuff[:4])
		buff.Write(key.IndexOrder)

		buff.Write(key.PrimaryKey)
	}

	return buff.Bytes()
}

func KeySize(primarySize, indexSize, indexOrderSize int) int {
	// tableID + indexID + indexKeyLen + index + indexOrderSize + indexOrder + primary
	size := 1 + 1 + 4 + indexSize + 4 + indexOrderSize + primarySize
	return size
}

func KeyDecode(keyBytes []byte) Key {
	buff := bytes.NewBuffer(keyBytes)

	tableID, _ := buff.ReadByte()
	indexID, _ := buff.ReadByte()

	indexLenBuff := make([]byte, 4)
	_, _ = buff.Read(indexLenBuff)
	indexLen := binary.BigEndian.Uint32(indexLenBuff)

	indexKey := make([]byte, indexLen)
	_, _ = buff.Read(indexKey)

	indexOrderLenBuff := make([]byte, 4)
	_, _ = buff.Read(indexOrderLenBuff)
	indexOrderLen := binary.BigEndian.Uint32(indexOrderLenBuff)

	indexOrder := make([]byte, indexOrderLen)
	_, _ = buff.Read(indexOrder)

	primaryKeyLen := len(keyBytes) - int(indexLen) - int(indexOrderLen) - 10
	primaryKey := make([]byte, primaryKeyLen)
	_, _ = buff.Read(primaryKey)

	return Key{
		TableID:    TableID(tableID),
		IndexID:    IndexID(indexID),
		IndexKey:   indexKey,
		IndexOrder: indexOrder,
		PrimaryKey: primaryKey,
	}
}

type KeyBytes []byte

type KeyEncodeOption struct {
	TableID          TableID
	IndexID          IndexID
	EncodePrimaryKey func(KeyBytes) KeyBytes
	EncodeIndexKey   func(KeyBytes) KeyBytes
	EncodeIndexOrder func(KeyBytes) KeyBytes
}

func (key KeyBytes) Encode(opt KeyEncodeOption) KeyBytes {
	// key format:
	// +---------+---------+---------------------------------------------------------------+
	// | TableID | IndexID | IndexLen | IndexKey | IndexOrderLen | IndexOrder | PrimaryKey |
	// +---------+---------+---------------------------------------------------------------+

	key[0] = byte(opt.TableID)
	key[1] = byte(opt.IndexID)

	pos := 2
	size := 0
	if opt.EncodeIndexKey != nil {
		size = len(opt.EncodeIndexKey(key[pos+4:]))
	}
	binary.BigEndian.PutUint32(key[pos:pos+4], uint32(size))

	pos += 4 + size
	if opt.EncodeIndexOrder != nil {
		size = len(opt.EncodeIndexOrder(key[pos+4:]))
	}
	binary.BigEndian.PutUint32(key[pos:pos+4], uint32(size))

	pos += 4 + size
	opt.EncodePrimaryKey(key[pos:])
	return key
}

func (key KeyBytes) ToDataKeyBytes(rawBuffs ...[]byte) KeyBytes {
	var rawBuff []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		rawBuff = rawBuffs[0]
	}

	buff := bytes.NewBuffer(rawBuff)

	buff.WriteByte(key[0])
	buff.WriteByte(0)

	buff.Write([]byte{0, 0, 0, 0})
	buff.Write([]byte{0, 0, 0, 0})

	indexKeyLen := int(binary.BigEndian.Uint32(key[2:6]))
	indexOrderLen := int(binary.BigEndian.Uint32(key[6+indexKeyLen : 10+indexKeyLen]))

	buff.Write(key[10+indexKeyLen+indexOrderLen:])

	return buff.Bytes()
}

func (key KeyBytes) TableID() TableID {
	return TableID(key[0])
}

func (key KeyBytes) IndexID() IndexID {
	return IndexID(key[1])
}

func (key KeyBytes) IndexKey() []byte {
	keyLen := binary.BigEndian.Uint32(key[2:6])
	return key[6 : 6+keyLen]
}

func (key KeyBytes) ToKey() Key {
	return KeyDecode(key)
}

func DefaultKeyComparer() *pebble.Comparer {
	comparer := *pebble.DefaultComparer
	comparer.Split = _KeyPrefixSplitIndex
	return &comparer
}

func _KeyPrefixSplitIndex(rawKey []byte) int {
	return 6 + int(binary.BigEndian.Uint32(rawKey[2:6]))
}
