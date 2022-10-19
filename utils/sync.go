package utils

import "sync"

type SyncPool[T any] interface {
	Get() T
	Put(T)
}

type SyncPoolWrapper[T any] struct {
	sync.Pool
}

func (s *SyncPoolWrapper[T]) Get() T {
	return s.Pool.Get().(T)
}

func (s *SyncPoolWrapper[T]) Put(t T) {
	s.Pool.Put(t)
}
