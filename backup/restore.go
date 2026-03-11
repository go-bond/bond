package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/fujiwara/shapeio"
	"github.com/go-bond/bond/utils"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

// backupWithMeta pairs a backup's object prefix with its parsed metadata.
type backupWithMeta struct {
	prefix string
	meta   *BackupMeta
}

// fileSource maps a required file to the backup that should supply it.
type fileSource struct {
	File         FileInfo
	BackupPrefix string
}

// resolveFileSources determines, for each required file, which backup in the
// chain should supply it. Backups are walked in reverse order so that the most
// recent version of each file wins. Returns an error if any required file
// cannot be found in any backup.
func resolveFileSources(requiredFiles []FileInfo, backups []backupWithMeta) ([]fileSource, error) {
	// Build a map of required file names to their expected sizes.
	need := make(map[string]int64, len(requiredFiles))
	for _, f := range requiredFiles {
		need[f.Name] = f.Size
	}

	resolved := make([]fileSource, 0, len(requiredFiles))
	resolvedSet := make(map[string]struct{}, len(requiredFiles))

	// Walk backups from newest to oldest.
	for i := len(backups) - 1; i >= 0; i-- {
		bm := backups[i]
		for _, fi := range bm.meta.Files {
			expectedSize, required := need[fi.Name]
			if !required {
				continue
			}
			if _, already := resolvedSet[fi.Name]; already {
				continue
			}
			if fi.Size != expectedSize {
				return nil, fmt.Errorf("corrupt backup chain: file %s has size %d in backup %s but expected %d",
					fi.Name, fi.Size, bm.prefix, expectedSize)
			}
			resolved = append(resolved, fileSource{
				File:         fi,
				BackupPrefix: bm.prefix,
			})
			resolvedSet[fi.Name] = struct{}{}
		}
	}

	if len(resolved) != len(requiredFiles) {
		var missing []string
		for _, f := range requiredFiles {
			if _, ok := resolvedSet[f.Name]; !ok {
				missing = append(missing, f.Name)
			}
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("corrupt backup chain: %d file(s) not found in any backup: %v", len(missing), missing)
	}

	return resolved, nil
}

// RestoreOptions configures a restore operation.
type RestoreOptions struct {
	// Prefix is the object storage prefix where backups are stored.
	Prefix string
	// RestoreDir is the local directory to restore into. Must be empty or non-existent.
	RestoreDir string
	// Before is the point-in-time cutoff. Backups after this time are ignored.
	// If zero, all backups are considered.
	Before time.Time
	// Concurrency is the number of parallel downloads per backup stage. Values <= 0 use DefaultConcurrency.
	Concurrency int
	// OnProgress is called after each file download completes. Must be goroutine-safe.
	OnProgress ProgressFunc
	// MaxDownloadBPS is the aggregate download rate limit in bytes per second.
	// Zero uses DefaultMaxDownloadBPS (100 MB/s). Negative disables rate limiting.
	MaxDownloadBPS int64
	// MaxDownloadRetries is the number of retries per file after the first failed download.
	// Zero uses DefaultMaxDownloadRetries. Only transient errors are retried.
	MaxDownloadRetries int
	// InitialRetryBackoff is the delay before the first retry. Zero uses DefaultInitialRetryBackoff.
	InitialRetryBackoff time.Duration
}

// Restore downloads a backup set from object storage and writes it to a local directory.
// The result is a valid Pebble/Bond database directory that can be opened with bond.Open.
func Restore(ctx context.Context, bucket objstore.Bucket, opts RestoreOptions) error {
	if opts.RestoreDir == "" {
		return fmt.Errorf("RestoreDir must be specified")
	}

	// Check for a .incomplete marker from a previously interrupted restore.
	// If found, clean the directory so we can start fresh.
	incomplete, err := HasIncompleteRestore(opts.RestoreDir)
	if err != nil {
		return fmt.Errorf("check incomplete restore: %w", err)
	}
	if incomplete {
		if err := cleanRestoreDir(opts.RestoreDir); err != nil {
			return fmt.Errorf("clean incomplete restore: %w", err)
		}
	}

	// Validate that RestoreDir is empty or doesn't exist.
	entries, err := os.ReadDir(opts.RestoreDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read restore dir: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("restore directory %q is not empty", opts.RestoreDir)
	}

	restoreSet, err := FindRestoreSet(ctx, bucket, opts.Prefix, opts.Before)
	if err != nil {
		return err
	}
	if len(restoreSet) == 0 {
		return fmt.Errorf("no backups found for prefix %q", opts.Prefix)
	}

	// Create the restore directory and place the .incomplete marker.
	if err := os.MkdirAll(opts.RestoreDir, 0755); err != nil {
		return fmt.Errorf("create restore dir: %w", err)
	}
	if err := writeRestoreIncompleteMarker(opts.RestoreDir); err != nil {
		return err
	}

	maxRetries := opts.MaxDownloadRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxDownloadRetries
	}
	initialBackoff := opts.InitialRetryBackoff
	if initialBackoff <= 0 {
		initialBackoff = DefaultInitialRetryBackoff
	}

	// Pre-read all metas.
	allBackups := make([]backupWithMeta, 0, len(restoreSet))
	for _, backup := range restoreSet {
		meta, err := readMeta(ctx, bucket, backup.Prefix, maxRetries, initialBackoff)
		if err != nil {
			return fmt.Errorf("read meta for %s: %w", backup.Prefix, err)
		}
		allBackups = append(allBackups, backupWithMeta{prefix: backup.Prefix, meta: meta})
	}

	// Use the last backup's CheckpointFiles as the definitive file set
	// and resolve each file to its most recent source backup.
	lastMeta := allBackups[len(allBackups)-1].meta
	resolved, err := resolveFileSources(lastMeta.CheckpointFiles, allBackups)
	if err != nil {
		return err
	}

	// Compute totals from the deduplicated resolved set.
	totalFiles := len(resolved)
	var totalBytes int64
	for _, rs := range resolved {
		totalBytes += rs.File.Size
	}

	// Pre-create all needed subdirectories to avoid concurrent MkdirAll calls.
	dirs := make(map[string]struct{})
	for _, rs := range resolved {
		localPath := filepath.Join(opts.RestoreDir, rs.File.Name)
		dirs[filepath.Dir(localPath)] = struct{}{}
	}
	for d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("create dir %s: %w", d, err)
		}
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	perStreamRate := resolvePerStreamRate(opts.MaxDownloadBPS, DefaultMaxDownloadBPS, concurrency)

	// Single parallel download phase over the resolved file set.
	var filesDone atomic.Int64
	var bytesDone atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, rs := range resolved {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			objName := path.Join(rs.BackupPrefix, rs.File.Name)
			localPath := filepath.Join(opts.RestoreDir, rs.File.Name)
			if err := downloadFileWithRetry(gctx, bucket, objName, localPath, perStreamRate, maxRetries, initialBackoff); err != nil {
				return err
			}

			done := int(filesDone.Add(1))
			bytes := bytesDone.Add(rs.File.Size)
			if opts.OnProgress != nil {
				opts.OnProgress(ProgressEvent{
					File:       rs.File.Name,
					FileSize:   rs.File.Size,
					FilesDone:  done,
					FilesTotal: totalFiles,
					BytesDone:  bytes,
					BytesTotal: totalBytes,
				})
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Recreate bond metadata directory with PEBBLE_FORMAT_VERSION.
	bondDir := filepath.Join(opts.RestoreDir, "bond")
	if err := os.MkdirAll(bondDir, 0755); err != nil {
		return fmt.Errorf("create bond dir: %w", err)
	}

	versionFile := filepath.Join(bondDir, "PEBBLE_FORMAT_VERSION")
	versionData := []byte(fmt.Sprintf("%d", lastMeta.PebbleFormatVersion))
	if err := utils.WriteFileWithSync(versionFile, versionData, 0644); err != nil {
		return fmt.Errorf("write pebble format version: %w", err)
	}

	// Write local backup meta so incremental backups can validate chain integrity.
	if err := writeLocalMeta(opts.RestoreDir, lastMeta); err != nil {
		return fmt.Errorf("write local meta: %w", err)
	}

	// Restore completed successfully — remove the .incomplete marker.
	if err := removeRestoreIncompleteMarker(opts.RestoreDir); err != nil {
		return err
	}

	return nil
}

// downloadFileWithRetry downloads objName from the bucket to localPath, retrying on transient errors.
// It fetches the object (and applies perStreamRate if > 0) for each attempt.
func downloadFileWithRetry(ctx context.Context, bucket objstore.Bucket, objName, localPath string, perStreamRate float64, maxRetries int, initialBackoff time.Duration) error {
	if initialBackoff <= 0 {
		initialBackoff = DefaultInitialRetryBackoff
	}

	policy := retrypolicy.NewBuilder[any]().
		HandleIf(func(_ any, err error) bool {
			return isRetryableError(err)
		}).
		WithMaxRetries(maxRetries).
		WithBackoff(initialBackoff, MaxRetryBackoff).
		WithJitterFactor(0.25).
		ReturnLastFailure().
		Build()

	err := failsafe.With[any](policy).
		WithContext(ctx).
		Run(func() error {
			rc, gErr := bucket.Get(ctx, objName)
			if gErr != nil {
				return gErr
			}
			defer rc.Close()

			var r io.Reader = rc
			if perStreamRate > 0 {
				sr := shapeio.NewReaderWithContext(rc, ctx)
				sr.SetRateLimit(perStreamRate)
				r = sr
			}

			f, fErr := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if fErr != nil {
				return fErr
			}

			if _, cErr := io.Copy(f, r); cErr != nil {
				_ = f.Close()
				_ = os.Remove(localPath)
				return cErr
			}
			return f.Close()
		})
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("download %s: %w", objName, ctx.Err())
		}
		return fmt.Errorf("download %s: %w", objName, err)
	}
	return nil
}
