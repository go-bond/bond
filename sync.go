package bond

import (
	"sync"
)

type SyncBatch struct {
	batch Batch
	mu    sync.Mutex
}

func NewSyncBatch(batch Batch) *SyncBatch {
	return &SyncBatch{
		batch: batch,
		mu:    sync.Mutex{},
	}
}

func (s *SyncBatch) WithSync(f func(batch Batch) error) error {
	s.mu.Lock()
	err := f(s.batch)
	s.mu.Unlock()
	return err
}
