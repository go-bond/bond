package bond

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"
)

type Serializer[T any] interface {
	Serialize(t T) ([]byte, error)
	Deserialize(b []byte, t T) error
}

type SerializerWithClosable[T any] interface {
	SerializerWithCloseable(t T) ([]byte, func(), error)
}

type SerializerAnyWrapper[T any] struct {
	Serializer Serializer[any]
}

func (s *SerializerAnyWrapper[T]) Serialize(t T) ([]byte, error) {
	return s.Serializer.Serialize(t)
}

func (s *SerializerAnyWrapper[T]) Deserialize(b []byte, t T) error {
	return s.Serializer.Deserialize(b, t)
}

type JsonSerializer struct {
}

func (s *JsonSerializer) Serialize(i interface{}) ([]byte, error) {
	return json.Marshal(i)
}

func (s *JsonSerializer) Deserialize(b []byte, i interface{}) error {
	return json.Unmarshal(b, i)
}

type MsgpackSerializer struct {
	Encoder    *SyncPoolWrapper[*msgpack.Encoder]
	Decoder    *SyncPoolWrapper[*msgpack.Decoder]
	BufferPool *SyncPoolWrapper[bytes.Buffer]
}

func (m *MsgpackSerializer) Serialize(i interface{}) ([]byte, error) {
	if m.Encoder != nil {
		var (
			enc  = m.Encoder.Get()
			buff = m.getBuffer()
		)

		enc.Reset(&buff)

		err := enc.Encode(i)
		if err != nil {
			return nil, err
		}

		m.Encoder.Put(enc)

		return buff.Bytes(), nil
	}
	return msgpack.Marshal(i)
}

func (m *MsgpackSerializer) SerializerWithCloseable(i interface{}) ([]byte, func(), error) {
	if m.Encoder != nil {
		var (
			enc  = m.Encoder.Get()
			buff = m.getBuffer()
		)

		enc.Reset(&buff)

		err := enc.Encode(i)
		if err != nil {
			return nil, nil, err
		}

		m.Encoder.Put(enc)

		closeable := func() {
			m.BufferPool.Put(buff)
		}

		return buff.Bytes(), closeable, nil
	}

	b, err := msgpack.Marshal(i)
	return b, func() {}, err
}

func (m *MsgpackSerializer) Deserialize(b []byte, i interface{}) error {
	return msgpack.Unmarshal(b, i)
}

func (m *MsgpackSerializer) getBuffer() bytes.Buffer {
	if m.BufferPool != nil {
		return m.BufferPool.Get()
	} else {
		return bytes.Buffer{}
	}
}

func (m *MsgpackSerializer) freeBuffer(buffer bytes.Buffer) {
	if m.BufferPool != nil {
		m.BufferPool.Put(buffer)
	}
}

type MsgpackGenSerializer struct {
	BufferPool *SyncPoolWrapper[bytes.Buffer]
}

func (m *MsgpackGenSerializer) Serialize(i interface{}) ([]byte, error) {
	e, ok := i.(msgp.Encodable)
	if !ok {
		if typ := reflect.TypeOf(i); typ.Kind() == reflect.Ptr {
			i := reflect.ValueOf(i).Elem().Interface()
			e, ok = i.(msgp.Encodable)
			if !ok {
				return nil, fmt.Errorf("interface does not implement msgp.Encodable")
			}
		} else {
			return nil, fmt.Errorf("interface does not implement msgp.Encodable")
		}
	}

	var buf bytes.Buffer

	err := msgp.Encode(&buf, e)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *MsgpackGenSerializer) SerializerWithCloseable(i interface{}) ([]byte, func(), error) {
	e, ok := i.(msgp.Encodable)
	if !ok {
		if typ := reflect.TypeOf(i); typ.Kind() == reflect.Ptr {
			i := reflect.ValueOf(i).Elem().Interface()
			e, ok = i.(msgp.Encodable)
			if !ok {
				return nil, nil, fmt.Errorf("interface does not implement msgp.Encodable")
			}
		} else {
			return nil, nil, fmt.Errorf("interface does not implement msgp.Encodable")
		}
	}

	buff := m.getBuffer()

	err := msgp.Encode(&buff, e)
	if err != nil {
		return nil, nil, err
	}

	closeable := func() {
		m.BufferPool.Put(buff)
	}

	return buff.Bytes(), closeable, nil
}

func (m *MsgpackGenSerializer) Deserialize(b []byte, i interface{}) error {
	newEntry := makeNewAny(i)
	root := findRootInterface(reflect.ValueOf(newEntry))
	d, ok := root.(msgp.Decodable)
	if !ok {
		return fmt.Errorf("interface does not implement msgp.Decodable")
	}

	err := msgp.Decode(bytes.NewBuffer(b), d)
	reflect.ValueOf(i).Elem().Set(reflect.ValueOf(newEntry).Elem())
	return err
}

func (m *MsgpackGenSerializer) getBuffer() bytes.Buffer {
	if m.BufferPool != nil {
		return m.BufferPool.Get()
	} else {
		return bytes.Buffer{}
	}
}

func (m *MsgpackGenSerializer) freeBuffer(buffer bytes.Buffer) {
	if m.BufferPool != nil {
		m.BufferPool.Put(buffer)
	}
}
