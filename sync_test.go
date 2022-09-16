package bond

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func Test_SyncBatch(t *testing.T) {
	syncBatch := NewSyncBatch(&pebble.Batch{})

	syncChan := make(chan bool)
	go func() {
		_ = syncBatch.WithSync(func(batch *pebble.Batch) error {
			syncChan <- true
			time.Sleep(5 * time.Second)
			return nil
		})
	}()
	<-syncChan

	assert.False(t, syncBatch.mu.TryLock())
}
