package backup

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"path"
	"time"

	"github.com/thanos-io/objstore"
)

// DefaultLockTTL is the maximum age of a backup lock before it's considered stale.
const DefaultLockTTL = 1 * time.Hour

const lockFileName = ".lock"

// defaultLockJitter is the maximum random delay between writing a lock and
// reading it back for verification. The jitter spreads concurrent acquirers
// apart so a read-back is very likely to observe another writer's overwrite.
// Tests override this to zero to avoid unnecessary delays.
var defaultLockJitter = 3 * time.Second

// ErrBackupInProgress is returned when another backup holds an active lock.
var ErrBackupInProgress = fmt.Errorf("another backup is in progress")

// ErrLockRefreshFailed is returned when the lock could not be refreshed
// within the TTL window. The backup is cancelled because the stale lock
// may be acquired by another process.
var ErrLockRefreshFailed = fmt.Errorf("lock refresh failed for longer than TTL")

type lockPayload struct {
	CreatedAt time.Time `json:"created_at"`
	Nonce     string    `json:"nonce,omitempty"`
}

func lockPath(prefix string) string {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return path.Join(prefix, lockFileName)
}

// generateNonce returns a random 128-bit hex string used to uniquely
// identify a lock write for read-after-write verification.
func generateNonce() string {
	b := make([]byte, 16)
	if _, err := crand.Read(b); err != nil {
		// Fallback: use timestamp nanos — still very likely unique.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// acquireLock attempts to acquire the backup lock. If an active (non-stale) lock
// exists, it returns ErrBackupInProgress. If the lock is stale or doesn't exist,
// a new lock is uploaded.
//
// Object storage lacks atomic compare-and-swap, so a TOCTOU race exists between
// checking/writing the lock. To mitigate this, acquireLock performs read-after-write
// verification: after uploading a lock it waits a random jitter (up to
// defaultLockJitter, 3 s by default) then re-reads the object to confirm its nonce
// still matches. If another process overwrote the lock in that window,
// ErrBackupInProgress is returned. This makes the collision window extremely small
// but does not eliminate it entirely; callers should treat the lock as best-effort.
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
				return uploadAndVerifyLock(ctx, bucket, lp)
			}
			return fmt.Errorf("read lock: %w", err)
		}
		defer rc.Close()

		data, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("read lock data: %w", err)
		}

		var payload lockPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			// Corrupt lock; treat as stale.
			return uploadAndVerifyLock(ctx, bucket, lp)
		}

		if time.Since(payload.CreatedAt) < ttl {
			return ErrBackupInProgress
		}
		// Stale lock; override it.
	}

	return uploadAndVerifyLock(ctx, bucket, lp)
}

// uploadAndVerifyLock writes a new lock with a random nonce, waits a random
// jitter (up to defaultLockJitter), then reads the lock back to confirm no
// other process overwrote it. If the nonce no longer matches,
// ErrBackupInProgress is returned.
func uploadAndVerifyLock(ctx context.Context, bucket objstore.Bucket, lp string) error {
	written, err := uploadLock(ctx, bucket, lp)
	if err != nil {
		return err
	}

	// Wait a random duration so concurrent acquirers are unlikely to
	// read back in lock-step.
	if jitterMax := defaultLockJitter; jitterMax > 0 {
		jitter := rand.N(jitterMax)
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Re-read the lock and verify our nonce is still present.
	rc, err := bucket.Get(ctx, lp)
	if err != nil {
		return fmt.Errorf("verify lock (read-back): %w", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("verify lock (read data): %w", err)
	}

	var current lockPayload
	if err := json.Unmarshal(data, &current); err != nil {
		return fmt.Errorf("verify lock (unmarshal): %w", err)
	}

	if current.Nonce != written.Nonce {
		return ErrBackupInProgress
	}

	return nil
}

func uploadLock(ctx context.Context, bucket objstore.Bucket, lockPath string) (lockPayload, error) {
	payload := lockPayload{
		CreatedAt: time.Now().UTC(),
		Nonce:     generateNonce(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return lockPayload{}, fmt.Errorf("marshal lock: %w", err)
	}
	if err := bucket.Upload(ctx, lockPath, bytes.NewReader(data)); err != nil {
		return lockPayload{}, fmt.Errorf("upload lock: %w", err)
	}
	return payload, nil
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
// (refreshing created_at) every ttl/3. If the lock cannot be refreshed for
// longer than the full TTL, cancelFunc is called to abort the backup — the
// stale lock could be acquired by another process, so continuing is unsafe.
// Returns a stop function that terminates the goroutine.
func startLockRefresh(ctx context.Context, bucket objstore.Bucket, prefix string, ttl time.Duration, cancelFunc context.CancelCauseFunc) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		lastRefresh := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				lp := lockPath(prefix)
				if _, err := uploadLock(ctx, bucket, lp); err != nil {
					if time.Since(lastRefresh) > ttl {
						cancelFunc(fmt.Errorf("%w: last error: %v", ErrLockRefreshFailed, err))
						return
					}
					continue
				}
				lastRefresh = time.Now()
			}
		}
	}()
	return func() { close(done) }
}
