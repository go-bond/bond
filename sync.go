package bond

import (
	"sync"

	"github.com/cockroachdb/pebble"
)

type SyncBatch struct {
	batch *pebble.Batch

	mu sync.Mutex
}

func NewSyncBatch(batch *pebble.Batch) *SyncBatch {
	return &SyncBatch{
		batch: batch,
		mu:    sync.Mutex{},
	}
}

func (s *SyncBatch) WithSync(f func(batch *pebble.Batch) error) error {
	s.mu.Lock()
	err := f(s.batch)
	s.mu.Unlock()
	return err
}
