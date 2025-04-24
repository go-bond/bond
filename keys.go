package bond

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble"
)

type KeyBuilder struct {
	buff []byte
	fid  byte
}

func NewKeyBuilder(buff []byte) KeyBuilder {
	return KeyBuilder{buff: buff}
}

func (b KeyBuilder) AddInt64Field(i int64) KeyBuilder {
	bt := b.putFieldID()

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
	bt.buff = append(bt.buff, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
	binary.BigEndian.PutUint64(bt.buff[len(bt.buff)-8:], i)
	return bt
}

func (b KeyBuilder) AddUint32Field(i uint32) KeyBuilder {
	bt := b.putFieldID()
	bt.buff = append(bt.buff, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(bt.buff[len(bt.buff)-4:], i)
	return bt
}

func (b KeyBuilder) AddUint16Field(i uint16) KeyBuilder {
	bt := b.putFieldID()
	bt.buff = append(bt.buff, []byte{0, 0}...)
	binary.BigEndian.PutUint16(bt.buff[len(bt.buff)-2:], i)
	return bt
}

func (b KeyBuilder) AddByteField(btt byte) KeyBuilder {
	bt := b.putFieldID()
	bt.buff = append(bt.buff, btt)
	return bt
}

func (b KeyBuilder) AddStringField(s string) KeyBuilder {
	bt := b.putFieldID()
	bt.buff = append(bt.buff, []byte(s)...)
	return bt
}

func (b KeyBuilder) AddBytesField(bs []byte) KeyBuilder {
	bt := b.putFieldID()
	bt.buff = append(bt.buff, bs...)
	return bt
}

func (b KeyBuilder) AddBigIntField(bi *big.Int, bits int) KeyBuilder {
	bt := b.putFieldID()

	sign := bi.Sign() + 1
	bt.buff = append(bt.buff, byte(sign)) // 0 - negative, 1 - zero, 2 - positive

	if sign == 0 {
		bi = big.NewInt(0).Sub(big.NewInt(0), bi)
	}

	bytesLen := bits / 8
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

func (b KeyBuilder) putFieldID() KeyBuilder {
	return KeyBuilder{
		buff: append(b.buff, b.fid+1),
		fid:  b.fid + 1,
	}
}

func (b KeyBuilder) Bytes() []byte {
	return b.buff
}

type Key struct {
	TableID    TableID
	IndexID    IndexID
	Index      []byte
	IndexOrder []byte
	PrimaryKey []byte
}

func NewUserKey(key string) KeyBytes {
	return KeyEncode(Key{
		TableID:    BOND_DB_DATA_TABLE_ID,
		IndexID:    BOND_DB_DATA_USER_SPACE_INDEX_ID,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte(key),
	})
}

func (k Key) ToDataKey() Key {
	return Key{
		TableID:    k.TableID,
		IndexID:    PrimaryIndexID,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: k.PrimaryKey,
	}
}

func (k Key) ToKeyPrefix() Key {
	return Key{
		TableID:    k.TableID,
		IndexID:    k.IndexID,
		Index:      k.Index,
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}
}

func (k Key) IsDataKey() bool {
	return k.IndexID == PrimaryIndexID && len(k.Index) == 0
}

func (k Key) IsIndexKey() bool {
	return k.IndexID != PrimaryIndexID && len(k.Index) != 0
}

func (k Key) IsKeyPrefix() bool {
	return len(k.PrimaryKey) == 0
}

func KeyEncode(key Key, rawBuffs ...[]byte) []byte {
	var rawBuff []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		rawBuff = rawBuffs[0] // assumes buffer is already cleared
	} else {
		// Estimate required capacity to avoid reallocations
		capacity := 2 + 4 + len(key.Index)
		if !key.IsKeyPrefix() {
			capacity += 4 + len(key.IndexOrder) + len(key.PrimaryKey)
		}
		rawBuff = make([]byte, 0, capacity)
	}

	// Avoid using bytes.Buffer which has overhead for small buffers
	rawBuff = append(rawBuff, byte(key.TableID))
	rawBuff = append(rawBuff, byte(key.IndexID))

	// Use a fixed-size array for length encoding
	var lenBuff [4]byte
	binary.BigEndian.PutUint32(lenBuff[:], uint32(len(key.Index)))
	rawBuff = append(rawBuff, lenBuff[:]...)
	rawBuff = append(rawBuff, key.Index...)

	if !key.IsKeyPrefix() {
		binary.BigEndian.PutUint32(lenBuff[:], uint32(len(key.IndexOrder)))
		rawBuff = append(rawBuff, lenBuff[:]...)
		rawBuff = append(rawBuff, key.IndexOrder...)
		rawBuff = append(rawBuff, key.PrimaryKey...)
	}

	return rawBuff
}

func KeyEncodeRaw(tableID TableID, indexID IndexID, indexFunc, indexOrderFunc, primaryKeyFunc func(buff []byte) []byte, buffs ...[]byte) []byte {
	var buff []byte
	if len(buffs) > 0 && buffs[0] != nil {
		buff = buffs[0]
	}

	buff = append(buff, byte(tableID))
	buff = append(buff, byte(indexID))

	// placeholder for len
	buff = append(buff, []byte{0x00, 0x00, 0x00, 0x00}...)
	lenIndex := len(buff) - 4

	if indexFunc != nil {
		buff = indexFunc(buff)
		binary.BigEndian.PutUint32(buff[lenIndex:lenIndex+4], uint32(len(buff)-(lenIndex+4)))
	}

	if primaryKeyFunc != nil {
		// placeholder for len
		buff = append(buff, []byte{0x00, 0x00, 0x00, 0x00}...)
		lenIndex = len(buff) - 4

		if indexOrderFunc != nil {
			buff = indexOrderFunc(buff)
			binary.BigEndian.PutUint32(buff[lenIndex:lenIndex+4], uint32(len(buff)-(lenIndex+4)))
		}

		buff = primaryKeyFunc(buff)
	}

	return buff
}

func KeyDecode(keyBytes []byte) (Key, error) {
	if len(keyBytes) < 10 { // Minimum length: TableID(1) + IndexID(1) + IndexLen(4) + IndexOrderLen(4)
		return Key{}, fmt.Errorf("key too short: %d bytes", len(keyBytes))
	}

	// Use direct indexing instead of slicing where possible
	tableID := TableID(keyBytes[0])
	indexID := IndexID(keyBytes[1])

	indexLen := binary.BigEndian.Uint32(keyBytes[2:6])
	idxStart := 6
	idxEnd := idxStart + int(indexLen)
	if idxEnd > len(keyBytes) {
		return Key{}, fmt.Errorf("invalid index length: %d exceeds key bounds %d", indexLen, len(keyBytes)-idxStart)
	}

	// Avoid unnecessary allocations by using slices
	index := keyBytes[idxStart:idxEnd]

	indexOrderLenStart := idxEnd
	indexOrderLenEnd := indexOrderLenStart + 4
	if indexOrderLenEnd > len(keyBytes) {
		return Key{}, fmt.Errorf("key too short to read index order length at offset %d", indexOrderLenStart)
	}

	indexOrderLen := binary.BigEndian.Uint32(keyBytes[indexOrderLenStart:indexOrderLenEnd])

	indexOrderStart := indexOrderLenEnd
	indexOrderEnd := indexOrderStart + int(indexOrderLen)
	if indexOrderEnd > len(keyBytes) {
		return Key{}, fmt.Errorf("invalid index order length: %d exceeds key bounds %d", indexOrderLen, len(keyBytes)-indexOrderStart)
	}
	indexOrder := keyBytes[indexOrderStart:indexOrderEnd]

	primaryKeyStart := indexOrderEnd
	primaryKey := keyBytes[primaryKeyStart:] // The rest is the primary key

	return Key{
		TableID:    tableID,
		IndexID:    indexID,
		Index:      index,
		IndexOrder: indexOrder,
		PrimaryKey: primaryKey,
	}, nil
}

/*func KeyDecode(keyBytes []byte) Key {
	buff := bytes.NewBuffer(keyBytes)

	tableID, _ := buff.ReadByte()
	indexID, _ := buff.ReadByte()

	indexLenBuff := make([]byte, 4)
	_, _ = buff.Read(indexLenBuff)
	indexLen := binary.BigEndian.Uint32(indexLenBuff)

	index := make([]byte, indexLen)
	_, _ = buff.Read(index)

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
		Index:      index,
		IndexOrder: indexOrder,
		PrimaryKey: primaryKey,
	}
}*/

type KeyBytes []byte

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

	indexLen := int(binary.BigEndian.Uint32(key[2:6]))
	indexOrderLen := int(binary.BigEndian.Uint32(key[6+indexLen : 10+indexLen]))

	buff.Write(key[10+indexLen+indexOrderLen:])

	return buff.Bytes()
}

func (key KeyBytes) TableID() TableID {
	return TableID(key[0])
}

func (key KeyBytes) IndexID() IndexID {
	return IndexID(key[1])
}

func (key KeyBytes) Index() []byte {
	keyLen := binary.BigEndian.Uint32(key[2:6])
	return key[6 : 6+keyLen]
}

func (key KeyBytes) IsDataKey() bool {
	return key.IndexID() == PrimaryIndexID
}

func (key KeyBytes) IsIndexKey() bool {
	return key.IndexID() != PrimaryIndexID
}

func (key KeyBytes) ToKey() Key {
	k, err := KeyDecode(key)
	if err != nil {
		panic(err) // unexpected
	}
	return k
}

func DefaultKeyComparer() *pebble.Comparer {
	comparer := *pebble.DefaultComparer
	comparer.Split = _KeyPrefixSplitIndex
	return &comparer
}

func _KeyPrefixSplitIndex(rawKey []byte) int {
	return 6 + int(binary.BigEndian.Uint32(rawKey[2:6]))
}
