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

	preAllocItems     []T
	preAllocItemsSize int

	mu sync.Mutex
}

func NewPreAllocatedPool[T any](newFunc func() any, size int) *PreAllocatedPool[T] {
	syncPool := &SyncPoolWrapper[T]{
		Pool: sync.Pool{
			New: newFunc,
		},
	}

	preAllocItems := make([]T, 0, size)
	for i := 0; i < size; i++ {
		preAllocItems = append(preAllocItems, syncPool.Get())
	}

	return &PreAllocatedPool[T]{
		SyncPoolWrapper:   syncPool,
		preAllocItems:     preAllocItems,
		preAllocItemsSize: size,
	}
}

func (s *PreAllocatedPool[T]) Get() T {
	s.mu.Lock()
	if itemsLen := len(s.preAllocItems); itemsLen > 0 {
		item := s.preAllocItems[itemsLen-1]
		s.preAllocItems = s.preAllocItems[:itemsLen-1]
		s.mu.Unlock()
		return item
	} else {
		s.mu.Unlock()
		return s.Pool.Get().(T)
	}
}

func (s *PreAllocatedPool[T]) Put(t T) {
	s.mu.Lock()
	if len(s.preAllocItems) < s.preAllocItemsSize {
		s.preAllocItems = append(s.preAllocItems, t)
		s.mu.Unlock()
	} else {
		s.mu.Unlock()
		s.Pool.Put(t)
	}
}
