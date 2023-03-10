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

type PreAllocatedPool[T any] struct {
	*SyncPoolWrapper[T]

	preAllocItems chan T
}

func NewPreAllocatedPool[T any](newFunc func() any, size int) *PreAllocatedPool[T] {
	preAllocItems := make(chan T, size)
	syncPool := &SyncPoolWrapper[T]{
		Pool: sync.Pool{
			New: newFunc,
		},
	}

	for i := 0; i < size; i++ {
		preAllocItems <- syncPool.Get()
	}

	return &PreAllocatedPool[T]{
		SyncPoolWrapper: syncPool,
		preAllocItems:   preAllocItems,
	}
}

func (s *PreAllocatedPool[T]) Get() T {
	select {
	case item := <-s.preAllocItems:
		return item
	default:
		return s.Pool.Get().(T)
	}
}

func (s *PreAllocatedPool[T]) Put(t T) {
	select {
	case s.preAllocItems <- t:
	default:
		s.Pool.Put(t)
	}
}
