package bond

import (
	"encoding/binary"
)

type KeyBuilder struct {
	buff []byte
	fid  byte
}

func NewKeyBuilder(buff []byte) KeyBuilder {
	return KeyBuilder{buff: buff}
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

func (b KeyBuilder) putFieldID() KeyBuilder {
	return KeyBuilder{
		buff: append(b.buff, b.fid+1),
		fid:  b.fid + 1,
	}
}

func (b KeyBuilder) Bytes() []byte {
	return b.buff
}

type Lazy[T any] struct {
	getFunc func() (T, error)
}

func (l Lazy[T]) Get() (T, error) {
	return l.getFunc()
}
