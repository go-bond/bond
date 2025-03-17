package bond

import (
	"bytes"
)

type Serializer[T any] interface {
	Serialize(t T) ([]byte, error)
	Deserialize(b []byte, t T) error
}

type SerializerWithBuffer[T any] interface {
	SerializeFuncWithBuffer(buff *bytes.Buffer) func(T any) ([]byte, error)
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
