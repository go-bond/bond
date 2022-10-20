package bond

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
