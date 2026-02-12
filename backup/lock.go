package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/thanos-io/objstore"
)

// DefaultLockTTL is the maximum age of a backup lock before it's considered stale.
const DefaultLockTTL = 1 * time.Hour

const lockFileName = ".lock"

// ErrBackupInProgress is returned when another backup holds an active lock.
var ErrBackupInProgress = fmt.Errorf("another backup is in progress")

type lockPayload struct {
	CreatedAt time.Time `json:"created_at"`
}

func lockPath(prefix string) string {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return path.Join(prefix, lockFileName)
}

// acquireLock attempts to acquire the backup lock. If an active (non-stale) lock
// exists, it returns ErrBackupInProgress. If the lock is stale or doesn't exist,
// a new lock is uploaded.
func acquireLock(ctx context.Context, bucket objstore.Bucket, prefix string, ttl time.Duration) error {
	lp := lockPath(prefix)

	exists, err := bucket.Exists(ctx, lp)
	if err != nil {
		return fmt.Errorf("check lock existence: %w", err)
	}

	if exists {
		rc, err := bucket.Get(ctx, lp)
		if err != nil {
			if bucket.IsObjNotFoundErr(err) {
				// Lock was removed between Exists and Get; proceed to create.
				return uploadLock(ctx, bucket, lp)
			}
			return fmt.Errorf("read lock: %w", err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return fmt.Errorf("read lock data: %w", err)
		}

		var payload lockPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			// Corrupt lock; treat as stale.
			return uploadLock(ctx, bucket, lp)
		}

		if time.Since(payload.CreatedAt) < ttl {
			return ErrBackupInProgress
		}
		// Stale lock; override it.
	}

	return uploadLock(ctx, bucket, lp)
}

func uploadLock(ctx context.Context, bucket objstore.Bucket, lockPath string) error {
	payload := lockPayload{CreatedAt: time.Now().UTC()}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal lock: %w", err)
	}
	if err := bucket.Upload(ctx, lockPath, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("upload lock: %w", err)
	}
	return nil
}

// releaseLock deletes the lock object. Ignores not-found errors.
func releaseLock(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	lp := lockPath(prefix)
	if err := bucket.Delete(ctx, lp); err != nil {
		if bucket.IsObjNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("delete lock: %w", err)
	}
	return nil
}

// startLockRefresh starts a background goroutine that re-uploads the lock
// (refreshing created_at) every ttl/2. Returns a stop function that terminates
// the goroutine.
func startLockRefresh(ctx context.Context, bucket objstore.Bucket, prefix string, ttl time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(ttl / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				lp := lockPath(prefix)
				_ = uploadLock(ctx, bucket, lp)
			}
		}
	}()
	return func() { close(done) }
}
