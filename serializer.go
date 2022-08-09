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
}

func (m *MsgpackSerializer) Serialize(i interface{}) ([]byte, error) {
	return msgpack.Marshal(i)
}

func (m *MsgpackSerializer) Deserialize(b []byte, i interface{}) error {
	return msgpack.Unmarshal(b, i)
}

type MsgpackGenSerializer struct {
}

func (m *MsgpackGenSerializer) Serialize(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
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

	err := msgp.Encode(&buf, e)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
