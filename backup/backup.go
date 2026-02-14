package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/fujiwara/shapeio"
	"github.com/go-bond/bond"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

// BackupOptions configures a backup operation.
type BackupOptions struct {
	// Prefix is the object storage prefix for all backups (e.g. "backups/").
	Prefix string
	// Type is the backup type (complete or incremental).
	Type BackupType
	// At overrides the backup timestamp. If zero, time.Now().UTC() is used.
	At time.Time
	// Concurrency is the number of parallel uploads. Values <= 0 use DefaultConcurrency.
	Concurrency int
	// OnProgress is called after each file upload completes. Must be goroutine-safe.
	OnProgress ProgressFunc
	// LockTTL is the maximum age of a backup lock before it's considered stale.
	// If zero, DefaultLockTTL (1 hour) is used.
	LockTTL time.Duration
	// CheckpointDir is the directory where the Pebble checkpoint will be created.
	// The caller is responsible for choosing a location on the same filesystem as the DB.
	// Required.
	CheckpointDir string
	// RateLimit is the aggregate upload rate limit in bytes per second.
	// Zero uses DefaultRateLimit (100 MB/s). Negative disables rate limiting.
	RateLimit float64
	// MaxUploadRetries is the number of retries per file after the first failed upload.
	// Zero uses DefaultMaxUploadRetries. Only transient errors are retried.
	MaxUploadRetries int
	// InitialRetryBackoff is the delay before the first retry. Zero uses DefaultInitialRetryBackoff.
	InitialRetryBackoff time.Duration
}

// Backup takes a Pebble checkpoint and uploads it to object storage.
func Backup(ctx context.Context, db bond.DB, bucket objstore.Bucket, opts BackupOptions) (*BackupMeta, error) {
	if opts.Type == "" {
		opts.Type = BackupTypeComplete
	}

	ttl := opts.LockTTL
	if ttl <= 0 {
		ttl = DefaultLockTTL
	}

	if err := acquireLock(ctx, bucket, opts.Prefix, ttl); err != nil {
		return nil, err
	}
	stopRefresh := startLockRefresh(ctx, bucket, opts.Prefix, ttl)
	defer func() {
		stopRefresh()
		_ = releaseLock(ctx, bucket, opts.Prefix)
	}()

	if _, err := removeIncompleteDirs(ctx, bucket, opts.Prefix); err != nil {
		return nil, fmt.Errorf("remove incomplete backups: %w", err)
	}

	dt := opts.At
	if dt.IsZero() {
		dt = time.Now().UTC()
	}
	dt = dt.UTC()

	objPrefix := backupObjectPrefix(opts.Prefix, dt, opts.Type)

	if opts.CheckpointDir == "" {
		return nil, fmt.Errorf("CheckpointDir is required")
	}

	// Remove any stale checkpoint from a previous crash.
	if err := os.RemoveAll(opts.CheckpointDir); err != nil {
		return nil, fmt.Errorf("remove stale checkpoint: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(opts.CheckpointDir)
	}()

	if ckErr := db.Backend().Checkpoint(opts.CheckpointDir); ckErr != nil {
		return nil, fmt.Errorf("pebble checkpoint: %w", ckErr)
	}

	// Collect all files in the checkpoint.
	var allFiles []FileInfo
	err := filepath.WalkDir(opts.CheckpointDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(opts.CheckpointDir, p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		allFiles = append(allFiles, FileInfo{Name: rel, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk checkpoint: %w", err)
	}

	// Determine which files to upload.
	var filesToUpload []FileInfo
	if opts.Type == BackupTypeIncremental {
		filesToUpload, err = computeIncrementalFiles(ctx, bucket, opts.Prefix, allFiles)
		if err != nil {
			return nil, err
		}
	} else {
		filesToUpload = allFiles
	}

	// Compute total bytes for progress reporting.
	var totalBytes int64
	for _, fi := range filesToUpload {
		totalBytes += fi.Size
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	perStreamRate := resolvePerStreamRate(opts.RateLimit, concurrency)

	maxRetries := opts.MaxUploadRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxUploadRetries
	}
	initialBackoff := opts.InitialRetryBackoff
	if initialBackoff <= 0 {
		initialBackoff = DefaultInitialRetryBackoff
	}

	// Upload the files in parallel.
	var filesDone atomic.Int64
	var bytesDone atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, fi := range filesToUpload {
		fi := fi
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			localPath := filepath.Join(opts.CheckpointDir, fi.Name)
			objName := path.Join(objPrefix, fi.Name)
			if err := uploadFileWithRetry(gctx, bucket, objName, localPath, fi.Name, perStreamRate, maxRetries, initialBackoff); err != nil {
				return err
			}

			done := int(filesDone.Add(1))
			bytes := bytesDone.Add(fi.Size)
			if opts.OnProgress != nil {
				opts.OnProgress(ProgressEvent{
					File:       fi.Name,
					FileSize:   fi.Size,
					FilesDone:  done,
					FilesTotal: len(filesToUpload),
					BytesDone:  bytes,
					BytesTotal: totalBytes,
				})
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Read Pebble format version and bond data version.
	pebbleFmtVer := uint64(db.Backend().FormatMajorVersion())
	bondDataVer := uint32(bond.BOND_DB_DATA_VERSION)

	meta := newBackupMeta(opts.Type, dt, pebbleFmtVer, bondDataVer, filesToUpload, allFiles)
	if err := writeMeta(ctx, bucket, objPrefix, meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// isRetriableUploadError reports whether the error from bucket.Upload should be retried.
func isRetriableUploadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var t interface{ Temporary() bool }
	if errors.As(err, &t) && t.Temporary() {
		return true
	}
	return false
}

// uploadFileWithRetry uploads the file at localPath to objName, retrying on transient errors.
// It opens the file (and applies perStreamRate if > 0) for each attempt.
func uploadFileWithRetry(ctx context.Context, bucket objstore.Bucket, objName, localPath, fileName string, perStreamRate float64, maxRetries int, initialBackoff time.Duration) error {
	backoff := initialBackoff
	if backoff <= 0 {
		backoff = DefaultInitialRetryBackoff
	}
	for attempt := 0; ; attempt++ {
		f, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("open file %s: %w", fileName, err)
		}
		var r io.Reader = f
		if perStreamRate > 0 {
			sr := shapeio.NewReaderWithContext(f, ctx)
			sr.SetRateLimit(perStreamRate)
			r = sr
		}
		err = bucket.Upload(ctx, objName, r)
		f.Close()
		if err == nil {
			return nil
		}
		if !isRetriableUploadError(err) || attempt >= maxRetries {
			return fmt.Errorf("upload %s: %w", fileName, err)
		}
		// Jitter: 0.75 to 1.25 * backoff
		jittered := time.Duration(float64(backoff) * (0.75 + 0.5*rand.Float64()))
		timer := time.NewTimer(jittered)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("upload %s: %w", fileName, ctx.Err())
		case <-timer.C:
		}
		if backoff < MaxRetryBackoff {
			backoff *= 2
			if backoff > MaxRetryBackoff {
				backoff = MaxRetryBackoff
			}
		}
	}
}

func computeIncrementalFiles(ctx context.Context, bucket objstore.Bucket, prefix string, allFiles []FileInfo) ([]FileInfo, error) {
	backups, err := ListBackups(ctx, bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("list backups for incremental: %w", err)
	}
	if len(backups) == 0 {
		return nil, fmt.Errorf("no previous backup found; cannot create incremental backup")
	}

	// Read the previous backup's metadata.
	prev := backups[len(backups)-1]
	prevMeta, err := readMeta(ctx, bucket, prev.Prefix)
	if err != nil {
		return nil, fmt.Errorf("read previous backup meta: %w", err)
	}

	// Build lookup from previous checkpoint files.
	prevSet := make(map[string]int64, len(prevMeta.CheckpointFiles))
	for _, f := range prevMeta.CheckpointFiles {
		prevSet[f.Name] = f.Size
	}

	// Diff: new or changed files.
	var diff []FileInfo
	for _, f := range allFiles {
		prevSize, exists := prevSet[f.Name]
		if !exists || prevSize != f.Size {
			diff = append(diff, f)
		}
	}

	return diff, nil
}
