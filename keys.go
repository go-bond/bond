package bond

import (
	"bytes"
	"encoding/binary"
	"math/big"
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
		bi = bi.Sub(big.NewInt(0), bi)
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

type _Key struct {
	TableID    TableID
	IndexID    IndexID
	IndexKey   []byte
	IndexOrder []byte
	PrimaryKey []byte
}

func (k _Key) ToDataKey() _Key {
	return _Key{
		TableID:    k.TableID,
		IndexID:    PrimaryIndexID,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: k.PrimaryKey,
	}
}

func (k _Key) ToKeyPrefix() _Key {
	return _Key{
		TableID:    k.TableID,
		IndexID:    k.IndexID,
		IndexKey:   k.IndexKey,
		IndexOrder: []byte{},
		PrimaryKey: []byte{},
	}
}

func (k _Key) IsDataKey() bool {
	return k.IndexID == PrimaryIndexID && len(k.IndexKey) == 0
}

func (k _Key) IsIndexKey() bool {
	return k.IndexID != PrimaryIndexID && len(k.IndexKey) != 0
}

func (k _Key) IsKeyPrefix() bool {
	return len(k.PrimaryKey) == 0
}

func _KeyEncode(key _Key, rawBuffs ...[]byte) []byte {
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

func _KeyDecode(keyBytes []byte) _Key {
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

	return _Key{
		TableID:    TableID(tableID),
		IndexID:    IndexID(indexID),
		IndexKey:   indexKey,
		IndexOrder: indexOrder,
		PrimaryKey: primaryKey,
	}
}

func _KeyBytesToDataKeyBytes(key []byte, rawBuffs ...[]byte) []byte {
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

func _KeyPrefixSplitIndex(rawKey []byte) int {
	return 6 + int(binary.BigEndian.Uint32(rawKey[2:6]))
}
