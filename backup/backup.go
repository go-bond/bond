package backup

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/go-bond/bond"
	"github.com/thanos-io/objstore"
)

// BackupOptions configures a backup operation.
type BackupOptions struct {
	// Prefix is the object storage prefix for all backups (e.g. "backups/").
	Prefix string
	// Type is the backup type (complete or incremental).
	Type BackupType
	// At overrides the backup timestamp. If zero, time.Now().UTC() is used.
	At time.Time
}

// Backup takes a Pebble checkpoint and uploads it to object storage.
func Backup(ctx context.Context, db bond.DB, bucket objstore.Bucket, opts BackupOptions) (*BackupMeta, error) {
	if opts.Type == "" {
		opts.Type = BackupTypeComplete
	}

	dt := opts.At
	if dt.IsZero() {
		dt = time.Now().UTC()
	}
	dt = dt.UTC()

	objPrefix := backupObjectPrefix(opts.Prefix, dt, opts.Type)

	// Create a temporary directory for the checkpoint.
	tempDir, err := os.MkdirTemp("", "bond-backup-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// The checkpoint destination must not exist yet; use a subdirectory.
	checkpointDir := filepath.Join(tempDir, "checkpoint")

	// Take a Pebble checkpoint with flushed WAL.
	if ckErr := db.Backend().Checkpoint(checkpointDir); ckErr != nil {
		return nil, fmt.Errorf("pebble checkpoint: %w", ckErr)
	}

	// Collect all files in the checkpoint.
	var allFiles []FileInfo
	err = filepath.WalkDir(checkpointDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(checkpointDir, p)
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

	// Upload the files.
	for _, fi := range filesToUpload {
		localPath := filepath.Join(checkpointDir, fi.Name)
		f, err := os.Open(localPath)
		if err != nil {
			return nil, fmt.Errorf("open file %s: %w", fi.Name, err)
		}
		objName := path.Join(objPrefix, fi.Name)
		if err := bucket.Upload(ctx, objName, f); err != nil {
			f.Close()
			return nil, fmt.Errorf("upload %s: %w", fi.Name, err)
		}
		f.Close()
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
