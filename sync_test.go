package bond

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// simpleBatchForSync is a minimal mock for Batch interface to avoid circular dependencies
type simpleBatchForSync struct{}

func (m *simpleBatchForSync) ID() uint64      { return 0 }
func (m *simpleBatchForSync) Type() BatchType { return BatchTypeReadWrite }
func (m *simpleBatchForSync) Len() int        { return 0 }
func (m *simpleBatchForSync) Count() uint32   { return 0 }
func (m *simpleBatchForSync) Empty() bool     { return true }
func (m *simpleBatchForSync) Reset()          {}
func (m *simpleBatchForSync) Get(key []byte, batch ...Batch) (data []byte, closer io.Closer, err error) {
	return nil, nil, nil
}
func (m *simpleBatchForSync) Set(key []byte, value []byte, opt WriteOptions, batch ...Batch) error {
	return nil
}
func (m *simpleBatchForSync) Delete(key []byte, opt WriteOptions, batch ...Batch) error { return nil }
func (m *simpleBatchForSync) DeleteRange(start []byte, end []byte, opt WriteOptions, batch ...Batch) error {
	return nil
}
func (m *simpleBatchForSync) Iter(opt *IterOptions, batch ...Batch) Iterator { return nil }
func (m *simpleBatchForSync) Apply(b Batch, opt WriteOptions) error          { return nil }
func (m *simpleBatchForSync) Commit(opt WriteOptions) error                  { return nil }
func (m *simpleBatchForSync) Close() error                                   { return nil }
func (m *simpleBatchForSync) OnCommit(f func(b Batch) error)                 {}
func (m *simpleBatchForSync) OnCommitted(f func(b Batch))                    {}
func (m *simpleBatchForSync) OnError(f func(b Batch, err error))             {}
func (m *simpleBatchForSync) OnClose(f func(b Batch))                        {}

func Test_SyncBatch(t *testing.T) {
	syncBatch := NewSyncBatch(&simpleBatchForSync{})

	syncChan := make(chan bool)
	go func() {
		_ = syncBatch.WithSync(func(batch Batch) error {
			syncChan <- true
			time.Sleep(5 * time.Second)
			return nil
		})
	}()
	<-syncChan

	assert.False(t, syncBatch.mu.TryLock())
}
