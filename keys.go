package bond

import (
	"bytes"
	"encoding/binary"
	"math/big"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/utils"
)

type KeyBuilder struct {
	buff             []byte
	fid              byte
	estimateSizeOnly bool // size will be estimated without building the key.
	size             int
}

func NewKeyBuilder(buff []byte, estimateSizeOnly ...bool) KeyBuilder {
	estimateSize := false
	if len(estimateSizeOnly) != 0 {
		estimateSize = estimateSizeOnly[0]
	}
	return KeyBuilder{buff: buff[:0], estimateSizeOnly: estimateSize, size: 0}
}

func (b KeyBuilder) AddInt64Field(i int64) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// type + int64
		return bt.addSize(1 + 8)
	}

	if i > 0 {
		bt.buff = append(bt.buff, 0x02)
	} else if i == 0 {
		bt.buff = append(bt.buff, 0x01)
	} else {
		bt.buff = append(bt.buff, 0x00)
		i = ^-i
	}

	bt.buff = append(bt.buff, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
	binary.BigEndian.PutUint64(bt.buff[len(bt.buff)-8:], uint64(i))
	return bt
}

func (b KeyBuilder) AddInt32Field(i int32) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// type + int32
		return bt.addSize(1 + 4)
	}

	if i > 0 {
		bt.buff = append(bt.buff, 0x02)
	} else if i == 0 {
		bt.buff = append(bt.buff, 0x01)
	} else {
		bt.buff = append(bt.buff, 0x00)
		i = ^-i
	}

	bt.buff = append(bt.buff, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(bt.buff[len(bt.buff)-4:], uint32(i))
	return bt
}

func (b KeyBuilder) AddInt16Field(i int16) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// type + int16
		return bt.addSize(1 + 2)
	}

	if i > 0 {
		bt.buff = append(bt.buff, 0x02)
	} else if i == 0 {
		bt.buff = append(bt.buff, 0x01)
	} else {
		bt.buff = append(bt.buff, 0x00)
		i = ^-i
	}

	bt.buff = append(bt.buff, []byte{0, 0}...)
	binary.BigEndian.PutUint16(bt.buff[len(bt.buff)-2:], uint16(i))
	return bt
}

func (b KeyBuilder) AddUint64Field(i uint64) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// uint64
		return bt.addSize(8)
	}

	bt.buff = append(bt.buff, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
	binary.BigEndian.PutUint64(bt.buff[len(bt.buff)-8:], i)
	return bt
}

func (b KeyBuilder) AddUint32Field(i uint32) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// uint32
		return bt.addSize(4)
	}

	bt.buff = append(bt.buff, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(bt.buff[len(bt.buff)-4:], i)
	return bt
}

func (b KeyBuilder) AddUint16Field(i uint16) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// uint16
		return bt.addSize(2)
	}

	bt.buff = append(bt.buff, []byte{0, 0}...)
	binary.BigEndian.PutUint16(bt.buff[len(bt.buff)-2:], i)
	return bt
}

func (b KeyBuilder) AddByteField(btt byte) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// byte
		return bt.addSize(1)
	}

	bt.buff = append(bt.buff, btt)
	return bt
}

func (b KeyBuilder) AddStringField(s string) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// string
		return bt.addSize(len(s))
	}

	bt.buff = append(bt.buff, []byte(s)...)
	return bt
}

func (b KeyBuilder) AddBytesField(bs []byte) KeyBuilder {
	bt := b.putFieldID()
	if b.estimateSizeOnly {
		// bytes
		return bt.addSize(len(bs))
	}

	bt.buff = append(bt.buff, bs...)
	return bt
}

func (b KeyBuilder) AddBigIntField(bi *big.Int, bits int) KeyBuilder {
	bt := b.putFieldID()
	bytesLen := bits / 8
	if b.estimateSizeOnly {
		// sign + bytesLen
		return bt.addSize(1 + bytesLen)
	}

	sign := bi.Sign() + 1
	bt.buff = append(bt.buff, byte(sign)) // 0 - negative, 1 - zero, 2 - positive

	if sign == 0 {
		bi = big.NewInt(0).Sub(big.NewInt(0), bi)
	}

	for i := 0; i < bytesLen; i++ {
		bt.buff = append(bt.buff, 0x00)
	}
	bi.FillBytes(bt.buff[len(bt.buff)-bytesLen:])

	if sign == 0 {
		index := len(bt.buff)
		for i := 0; i < bytesLen; i++ {
			bt.buff[index-i-1] = 0xFF - bt.buff[index-i-1]
		}
	}

	return bt
}

func (b KeyBuilder) addSize(size int) KeyBuilder {
	return KeyBuilder{
		estimateSizeOnly: true,
		size:             b.size + size,
	}
}

func (b KeyBuilder) putFieldID() KeyBuilder {
	if b.estimateSizeOnly {
		return b.addSize(1)
	}

	return KeyBuilder{
		buff: append(b.buff, b.fid+1),
		fid:  b.fid + 1,
	}
}

func (b KeyBuilder) Bytes() []byte {
	if b.estimateSizeOnly {
		return utils.IntToSlice(b.size)
	}

	return b.buff
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
