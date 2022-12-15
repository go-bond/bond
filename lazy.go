package bond

type Lazy[T any] struct {
	GetFunc    func() (T, error)
	BufferFunc func() error
	EmitFunc   func() ([]T, error)
}

func (l Lazy[T]) Get() (T, error) {
	return l.GetFunc()
}

func (l Lazy[T]) Buffer() error {
	return l.BufferFunc()
}

func (l Lazy[T]) Emit() ([]T, error) {
	return l.EmitFunc()
}
