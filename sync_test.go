package bond

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_SyncBatch(t *testing.T) {
	syncBatch := NewSyncBatch(&MockBatch{})

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
