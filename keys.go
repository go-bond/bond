package bond

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble"
)

func NewKeyBuilder(buff []byte) KeyBuilder {
	return &keyBuilder{buff: buff}
}

type KeyBuilder interface {
	Reset() KeyBuilder
	AddInt64Field(i int64) KeyBuilder
	AddInt32Field(i int32) KeyBuilder
	AddInt16Field(i int16) KeyBuilder
	AddUint64Field(i uint64) KeyBuilder
	AddUint32Field(i uint32) KeyBuilder
	AddUint16Field(i uint16) KeyBuilder
	AddByteField(btt byte) KeyBuilder
	AddStringField(s string) KeyBuilder
	AddBytesField(bs []byte) KeyBuilder
	AddBigIntField(bi *big.Int, bits int) KeyBuilder
	Bytes() []byte
}

type keyBuilder struct {
	buff []byte
	fid  byte
}

const (
	signNegative byte = 0x00
	signZero     byte = 0x01
	signPositive byte = 0x02
)

func (b *keyBuilder) Reset() KeyBuilder {
	b.buff = b.buff[:0] // Keep allocated memory, reset length
	b.fid = 0
	return b
}

func (b *keyBuilder) AddInt64Field(i int64) KeyBuilder {
	b.putFieldID()
	var sign byte
	u := uint64(i)
	if i > 0 {
		sign = signPositive
	} else if i == 0 {
		sign = signZero
	} else {
		sign = signNegative
		u = uint64(^-i)
	}
	b.buff = append(b.buff, sign)
	b.buff = binary.BigEndian.AppendUint64(b.buff, u)
	return b
}

func (b *keyBuilder) AddInt32Field(i int32) KeyBuilder {
	b.putFieldID()
	var sign byte
	u := uint32(i)
	if i > 0 {
		sign = signPositive
	} else if i == 0 {
		sign = signZero
	} else {
		sign = signNegative
		u = uint32(^-i)
	}
	b.buff = append(b.buff, sign)
	b.buff = binary.BigEndian.AppendUint32(b.buff, u)
	return b
}

func (b *keyBuilder) AddInt16Field(i int16) KeyBuilder {
	b.putFieldID()
	var sign byte
	u := uint16(i)
	if i > 0 {
		sign = signPositive
	} else if i == 0 {
		sign = signZero
	} else {
		sign = signNegative
		u = uint16(^-i)
	}
	b.buff = append(b.buff, sign)
	b.buff = binary.BigEndian.AppendUint16(b.buff, u)
	return b
}

func (b *keyBuilder) AddUint64Field(i uint64) KeyBuilder {
	b.putFieldID()
	b.buff = binary.BigEndian.AppendUint64(b.buff, i)
	return b
}

func (b *keyBuilder) AddUint32Field(i uint32) KeyBuilder {
	b.putFieldID()
	b.buff = binary.BigEndian.AppendUint32(b.buff, i)
	return b
}

func (b *keyBuilder) AddUint16Field(i uint16) KeyBuilder {
	b.putFieldID()
	b.buff = binary.BigEndian.AppendUint16(b.buff, i)
	return b
}

func (b *keyBuilder) AddByteField(btt byte) KeyBuilder {
	b.putFieldID()
	b.buff = append(b.buff, btt)
	return b
}

func (b *keyBuilder) AddStringField(s string) KeyBuilder {
	b.putFieldID()
	b.buff = append(b.buff, s...)
	return b
}

func (b *keyBuilder) AddBytesField(bs []byte) KeyBuilder {
	b.putFieldID()
	b.buff = append(b.buff, bs...)
	return b
}

func (b *keyBuilder) AddBigIntField(bi *big.Int, bits int) KeyBuilder {
	b.putFieldID()
	sign := bi.Sign() + 1 // -1,0,1  -> 0,1,2
	b.buff = append(b.buff, byte(sign))

	if sign == 0 { // negative
		tmp := big.NewInt(0).Neg(bi) // no new allocation after Go1.22
		bi = tmp
	}
	bytesLen := bits >> 3 // /8
	pos := len(b.buff)
	b.buff = append(b.buff, make([]byte, bytesLen)...)
	bi.FillBytes(b.buff[pos:])

	if sign == 0 { // two’s-complement flip
		for i := 0; i < bytesLen; i++ {
			b.buff[pos+i] = ^b.buff[pos+i]
		}
	}
	return b
}

func (b *keyBuilder) putFieldID() {
	b.buff = append(b.buff, b.fid+1)
	b.fid++
}

func (b *keyBuilder) Bytes() []byte {
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
	var b []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		b = rawBuffs[0][:0] // re-use caller’s storage
	}
	// upper-bound size ↓ (2 tag bytes + 4 len words + data)
	need := 2 + 4 + len(key.Index)
	if !key.IsKeyPrefix() {
		need += 4 + len(key.IndexOrder) + len(key.PrimaryKey)
	}
	if cap(b) < need {
		b = make([]byte, 0, need) // one exact allocation
	}

	b = append(b, byte(key.TableID), byte(key.IndexID))
	b = binary.BigEndian.AppendUint32(b, uint32(len(key.Index)))
	b = append(b, key.Index...)

	if !key.IsKeyPrefix() {
		b = binary.BigEndian.AppendUint32(b, uint32(len(key.IndexOrder)))
		b = append(b, key.IndexOrder...)
		b = append(b, key.PrimaryKey...)
	}

	return b
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
	if len(keyBytes) < 6 {
		return Key{}, fmt.Errorf("malformed key")
	}
	tableID, indexID := TableID(keyBytes[0]), IndexID(keyBytes[1])

	idxLen := int(binary.BigEndian.Uint32(keyBytes[2:6]))
	off := 6 + idxLen
	if len(keyBytes) < off {
		return Key{}, fmt.Errorf("malformed key")
	}
	index := keyBytes[6:off] // zero-copy reference

	if len(keyBytes) == off { // prefix key stops here
		return Key{TableID: tableID, IndexID: indexID, Index: index}, nil
	}

	idxOrdLen := int(binary.BigEndian.Uint32(keyBytes[off : off+4]))
	off += 4
	if len(keyBytes) < off+idxOrdLen {
		return Key{}, fmt.Errorf("malformed key")
	}
	indexOrd := keyBytes[off : off+idxOrdLen]
	primary := keyBytes[off+idxOrdLen:]

	return Key{
		TableID:    tableID,
		IndexID:    indexID,
		Index:      index,
		IndexOrder: indexOrd,
		PrimaryKey: primary,
	}, nil
}

type KeyBytes []byte

func (key KeyBytes) ToDataKeyBytes(rawBuffs ...[]byte) KeyBytes {
	var b []byte
	if len(rawBuffs) > 0 && rawBuffs[0] != nil {
		b = rawBuffs[0][:0]
	}

	idxLen := int(binary.BigEndian.Uint32(key[2:6]))
	ordLen := int(binary.BigEndian.Uint32(key[6+idxLen : 10+idxLen]))
	need := 2 + 4 + 4 + (len(key) - (10 + idxLen + ordLen))
	if cap(b) < need {
		b = make([]byte, 0, need)
	}

	b = append(b, key[0], 0)                 // TableID + PrimaryIndexID
	b = binary.BigEndian.AppendUint32(b, 0)  // empty Index
	b = binary.BigEndian.AppendUint32(b, 0)  // empty IndexOrder
	b = append(b, key[10+idxLen+ordLen:]...) // copy PK only
	return b
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
	comparer.Split = _KeyPrefixSplit
	return &comparer
}

// _KeyPrefixSplitIndex defines the prefix as TableID (1 byte) + IndexID (1 byte) + IndexLen (4 bytes) + Index (variable)
const _KeyPrefixSplitIndexOffset = 6

func _KeyPrefixSplit(rawKey []byte) int {
	if len(rawKey) < _KeyPrefixSplitIndexOffset {
		return len(rawKey)
	}

	if KeyBytes(rawKey).IndexID() != PrimaryIndexID {
		return _KeyPrefixSplitIndexOffset + int(binary.BigEndian.Uint32(rawKey[2:6]))
	}
	return len(rawKey)
}

func _KeyPrefix(rawKey []byte) int {
	if len(rawKey) < _KeyPrefixSplitIndexOffset {
		return len(rawKey)
	}

	return _KeyPrefixSplitIndexOffset + int(binary.BigEndian.Uint32(rawKey[2:6]))
}
