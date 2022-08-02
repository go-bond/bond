package bond

import (
	"encoding/json"

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

type MsgPackSerializer struct {
}

func (m *MsgPackSerializer) Serialize(i interface{}) ([]byte, error) {
	return msgpack.Marshal(i)
}

func (m *MsgPackSerializer) Deserialize(b []byte, i interface{}) error {
	return msgpack.Unmarshal(b, i)
}
