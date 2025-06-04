package utils

import (
	"sync"
)

type SyncPool[T any] interface {
	Get() T
	Put(T)
}

func NewSyncPool[T any](newFunc func() any) SyncPool[T] {
	return &syncPool[T]{
		Pool: sync.Pool{
			New: newFunc,
		},
	}
}

type syncPool[T any] struct {
	sync.Pool
}

func (s *syncPool[T]) Get() T {
	return s.Pool.Get().(T)
}

func (s *syncPool[T]) Put(t T) {
	// prevent the dirty slice from being returned to the pool
	t = resetSlice(t)

	// put the item back to the pool
	s.Pool.Put(t)
}

type preAllocatedSyncPool[T any] struct {
	*syncPool[T]
	preAllocItems     []T
	preAllocItemsSize int
	mu                sync.Mutex
}

func NewPreAllocatedSyncPool[T any](newFunc func() any, size int) SyncPool[T] {
	syncPool := &syncPool[T]{
		Pool: sync.Pool{
			New: newFunc,
		},
	}

	// allocate pre-allocated items and put them into the pool
	preAllocItems := make([]T, 0, size)
	for i := 0; i < size; i++ {
		preAllocItems = append(preAllocItems, syncPool.Get())
	}

	return &preAllocatedSyncPool[T]{
		syncPool:          syncPool,
		preAllocItems:     preAllocItems,
		preAllocItemsSize: size,
	}
}

func (s *preAllocatedSyncPool[T]) Get() T {
	// Fast path: try to get from pre-allocated items with minimal locking
	s.mu.Lock()
	itemsLen := len(s.preAllocItems)
	if itemsLen > 0 {
		item := s.preAllocItems[itemsLen-1]
		s.preAllocItems = s.preAllocItems[:itemsLen-1]
		s.mu.Unlock()
		return item
	}
	s.mu.Unlock()

	// Slow path: get from the underlying pool
	return s.Pool.Get().(T)
}

func (s *preAllocatedSyncPool[T]) Put(t T) {
	// prevent the dirty slice from being returned to the pool
	t = resetSlice(t)

	// Fast path: try to put into pre-allocated items with minimal locking
	s.mu.Lock()
	if len(s.preAllocItems) < s.preAllocItemsSize {
		s.preAllocItems = append(s.preAllocItems, t)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Slow path: put into the underlying pool
	s.Pool.Put(t)
}

func resetSlice[T any](v T) T {
	switch s := any(v).(type) {
	case []byte:
		if len(s) > 0 {
			v = any(s[:0]).(T)
		}
	case [][]byte:
		for i := range s {
			s[i] = nil
		}

		if len(s) > 0 {
			v = any(s[:0]).(T)
		}
	case []string:
		if len(s) > 0 {
			v = any(s[:0]).(T)
		}
	case []int:
		if len(s) > 0 {
			v = any(s[:0]).(T)
		}
		// Add more specific types as needed
	}
	return v
}
