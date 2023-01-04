package bond

type Lazy[T any] struct {
	GetFunc func() (T, error)
}

func (l Lazy[T]) Get() (T, error) {
	return l.GetFunc()
}
