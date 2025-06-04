package utils

import (
	"fmt"
	"testing"
)

func TestSyncPool_CleansDirtyBuffers(t *testing.T) {
	tests := []struct {
		name     string
		newFunc  func() any
		dirtier  func(any) any
		verifier func(testing.TB, any)
	}{
		{
			name: "[]byte buffer",
			newFunc: func() any {
				return make([]byte, 0, 10)
			},
			dirtier: func(item any) any {
				buf := item.([]byte)
				buf = append(buf, 1, 2, 3, 4, 5)
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([]byte)
				if len(buf) != 0 {
					t.Errorf("Expected clean buffer with len=0, got len=%d", len(buf))
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
		{
			name: "[][]byte buffer",
			newFunc: func() any {
				return make([][]byte, 0, 5)
			},
			dirtier: func(item any) any {
				buf := item.([][]byte)
				buf = append(buf, []byte("test1"), []byte("test2"))
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([][]byte)
				for i, elem := range buf {
					if elem != nil {
						t.Errorf("Expected element %d to be nil, got %v", i, elem)
					}
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
		{
			name: "[]string buffer",
			newFunc: func() any {
				return make([]string, 0, 5)
			},
			dirtier: func(item any) any {
				buf := item.([]string)
				buf = append(buf, "dirty1", "dirty2", "dirty3")
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([]string)
				if len(buf) != 0 {
					t.Errorf("Expected clean buffer with len=0, got len=%d", len(buf))
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
		{
			name: "[]int buffer",
			newFunc: func() any {
				return make([]int, 0, 5)
			},
			dirtier: func(item any) any {
				buf := item.([]int)
				buf = append(buf, 10, 20, 30)
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([]int)
				if len(buf) != 0 {
					t.Errorf("Expected clean buffer with len=0, got len=%d", len(buf))
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewSyncPool[any](tt.newFunc)

			// Get a clean buffer from the pool
			buffer := pool.Get()

			// Verify it's initially clean
			tt.verifier(t, buffer)

			// Make the buffer dirty
			dirtyBuffer := tt.dirtier(buffer)

			// Put the dirty buffer back to the pool
			pool.Put(dirtyBuffer)

			// Get the buffer again and verify it's clean
			cleanBuffer := pool.Get()
			tt.verifier(t, cleanBuffer)
		})
	}
}

func TestPreAllocatedSyncPool_CleansDirtyBuffers(t *testing.T) {
	tests := []struct {
		name     string
		newFunc  func() any
		dirtier  func(any) any
		verifier func(testing.TB, any)
	}{
		{
			name: "[]byte buffer",
			newFunc: func() any {
				return make([]byte, 0, 10)
			},
			dirtier: func(item any) any {
				buf := item.([]byte)
				buf = append(buf, 1, 2, 3, 4, 5)
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([]byte)
				if len(buf) != 0 {
					t.Errorf("Expected clean buffer with len=0, got len=%d", len(buf))
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
		{
			name: "[][]byte buffer",
			newFunc: func() any {
				return make([][]byte, 0, 5)
			},
			dirtier: func(item any) any {
				buf := item.([][]byte)
				buf = append(buf, []byte("test1"), []byte("test2"))
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([][]byte)
				for i, elem := range buf {
					if elem != nil {
						t.Errorf("Expected element %d to be nil, got %v", i, elem)
					}
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
		{
			name: "[]string buffer",
			newFunc: func() any {
				return make([]string, 0, 5)
			},
			dirtier: func(item any) any {
				buf := item.([]string)
				buf = append(buf, "dirty1", "dirty2", "dirty3")
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([]string)
				if len(buf) != 0 {
					t.Errorf("Expected clean buffer with len=0, got len=%d", len(buf))
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
		{
			name: "[]int buffer",
			newFunc: func() any {
				return make([]int, 0, 5)
			},
			dirtier: func(item any) any {
				buf := item.([]int)
				buf = append(buf, 10, 20, 30)
				return buf
			},
			verifier: func(t testing.TB, item any) {
				buf := item.([]int)
				if len(buf) != 0 {
					t.Errorf("Expected clean buffer with len=0, got len=%d", len(buf))
				}
				if cap(buf) == 0 {
					t.Error("Expected buffer capacity to be preserved")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with different pool sizes
			poolSizes := []int{1, 3, 5}

			for _, poolSize := range poolSizes {
				t.Run(fmt.Sprintf("pool_size_%d", poolSize), func(t *testing.T) {
					pool := NewPreAllocatedSyncPool[any](tt.newFunc, poolSize)

					// Get a clean buffer from the pool
					buffer := pool.Get()

					// Verify it's initially clean
					tt.verifier(t, buffer)

					// Make the buffer dirty
					dirtyBuffer := tt.dirtier(buffer)

					// Put the dirty buffer back to the pool
					pool.Put(dirtyBuffer)

					// Get the buffer again and verify it's clean
					cleanBuffer := pool.Get()
					tt.verifier(t, cleanBuffer)
				})
			}
		})
	}
}

func TestPreAllocatedSyncPool_PreAllocAndOverflow(t *testing.T) {
	t.Run("pre-allocated items path", func(t *testing.T) {
		poolSize := 2
		pool := NewPreAllocatedSyncPool[[]byte](func() any {
			return make([]byte, 0, 10)
		}, poolSize)

		// Get all pre-allocated items and dirty them
		var buffers [][]byte
		for i := 0; i < poolSize; i++ {
			buf := pool.Get()
			buf = append(buf, byte(i), byte(i+1), byte(i+2))
			buffers = append(buffers, buf)
		}

		// Put them all back
		for _, buf := range buffers {
			pool.Put(buf)
		}

		// Get them again and verify they're clean
		for i := 0; i < poolSize; i++ {
			buf := pool.Get()
			if len(buf) != 0 {
				t.Errorf("Buffer %d not clean, length: %d", i, len(buf))
			}
		}
	})

	t.Run("overflow to underlying pool", func(t *testing.T) {
		poolSize := 2
		pool := NewPreAllocatedSyncPool[[]byte](func() any {
			return make([]byte, 0, 10)
		}, poolSize)

		// Get more items than pre-allocated
		var buffers [][]byte
		for i := 0; i < poolSize+2; i++ {
			buf := pool.Get()
			buf = append(buf, byte(i), byte(i+1), byte(i+2))
			buffers = append(buffers, buf)
		}

		// Put them all back
		for _, buf := range buffers {
			pool.Put(buf)
		}

		// Get them again and verify they're clean
		for i := 0; i < poolSize+2; i++ {
			buf := pool.Get()
			if len(buf) != 0 {
				t.Errorf("Buffer %d not clean, length: %d", i, len(buf))
			}
		}
	})
}

func TestResetSlice_SpecialCases(t *testing.T) {
	t.Run("[][]byte with nil slices", func(t *testing.T) {
		pool := NewSyncPool[[][]byte](func() any {
			return make([][]byte, 0, 5)
		})

		buf := pool.Get()
		buf = append(buf, []byte("test1"), nil, []byte("test2"))

		pool.Put(buf)

		cleanBuf := pool.Get()
		// For [][]byte, resetSlice sets elements to nil but doesn't reset length to 0
		for i, elem := range cleanBuf {
			if elem != nil {
				t.Errorf("Expected element %d to be nil, got %v", i, elem)
			}
		}
	})

	t.Run("empty slices", func(t *testing.T) {
		pool := NewSyncPool[[]byte](func() any {
			return make([]byte, 0)
		})

		buf := pool.Get()
		// Don't dirty the buffer, just put it back
		pool.Put(buf)

		cleanBuf := pool.Get()
		if len(cleanBuf) != 0 {
			t.Errorf("Expected clean buffer, got length: %d", len(cleanBuf))
		}
	})
}

func BenchmarkSyncPool_PutClean(b *testing.B) {
	pool := NewSyncPool[[]byte](func() any {
		return make([]byte, 0, 1024)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, 1, 2, 3, 4, 5)
		pool.Put(buf)
	}
}

func BenchmarkPreAllocatedSyncPool_PutClean(b *testing.B) {
	pool := NewPreAllocatedSyncPool[[]byte](func() any {
		return make([]byte, 0, 1024)
	}, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, 1, 2, 3, 4, 5)
		pool.Put(buf)
	}
}
