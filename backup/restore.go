package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/go-bond/bond/utils"
	"github.com/thanos-io/objstore"
)

// RestoreOptions configures a restore operation.
type RestoreOptions struct {
	// Prefix is the object storage prefix where backups are stored.
	Prefix string
	// RestoreDir is the local directory to restore into. Must be empty or non-existent.
	RestoreDir string
	// Before is the point-in-time cutoff. Backups after this time are ignored.
	// If zero, all backups are considered.
	Before time.Time
}

// Restore downloads a backup set from object storage and writes it to a local directory.
// The result is a valid Pebble/Bond database directory that can be opened with bond.Open.
func Restore(ctx context.Context, bucket objstore.Bucket, opts RestoreOptions) error {
	if opts.RestoreDir == "" {
		return fmt.Errorf("RestoreDir must be specified")
	}

	// Validate that RestoreDir is empty or doesn't exist.
	entries, err := os.ReadDir(opts.RestoreDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read restore dir: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("restore directory %q is not empty", opts.RestoreDir)
	}

	before := opts.Before
	if before.IsZero() {
		before = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	}

	restoreSet, err := FindRestoreSet(ctx, bucket, opts.Prefix, before)
	if err != nil {
		return err
	}

	// Create the restore directory.
	if err := os.MkdirAll(opts.RestoreDir, 0755); err != nil {
		return fmt.Errorf("create restore dir: %w", err)
	}

	// Download and apply each backup in order.
	var lastMeta *BackupMeta
	for _, backup := range restoreSet {
		meta, err := readMeta(ctx, bucket, backup.Prefix)
		if err != nil {
			return fmt.Errorf("read meta for %s: %w", backup.Prefix, err)
		}

		for _, fi := range meta.Files {
			objName := path.Join(backup.Prefix, fi.Name)
			rc, err := bucket.Get(ctx, objName)
			if err != nil {
				return fmt.Errorf("get %s: %w", objName, err)
			}

			localPath := filepath.Join(opts.RestoreDir, fi.Name)
			if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
				rc.Close()
				return fmt.Errorf("create dir for %s: %w", fi.Name, err)
			}

			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return fmt.Errorf("read %s: %w", objName, err)
			}

			if err := os.WriteFile(localPath, data, 0644); err != nil {
				return fmt.Errorf("write %s: %w", localPath, err)
			}
		}

		lastMeta = meta
	}

	// Recreate bond metadata directory with PEBBLE_FORMAT_VERSION.
	if lastMeta != nil {
		bondDir := filepath.Join(opts.RestoreDir, "bond")
		if err := os.MkdirAll(bondDir, 0755); err != nil {
			return fmt.Errorf("create bond dir: %w", err)
		}

		versionFile := filepath.Join(bondDir, "PEBBLE_FORMAT_VERSION")
		versionData := []byte(fmt.Sprintf("%d", lastMeta.PebbleFormatVersion))
		if err := utils.WriteFileWithSync(versionFile, versionData, 0644); err != nil {
			return fmt.Errorf("write pebble format version: %w", err)
		}
	}

	return nil
}
