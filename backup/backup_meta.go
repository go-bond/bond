package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/go-bond/bond/utils"
	"github.com/google/uuid"
	"github.com/thanos-io/objstore"
)

// ErrChainBroken is returned when an incremental backup cannot continue
// because the local UUID does not match the latest backup in the bucket.
var ErrChainBroken = errors.New("backup chain broken")

// BackupMeta contains metadata about a single backup.
type BackupMeta struct {
	UUID                string     `json:"uuid"`
	Type                BackupType `json:"type"`
	Datetime            time.Time  `json:"datetime"`
	PebbleFormatVersion uint64     `json:"pebble_format_version"`
	BondDataVersion     uint32     `json:"bond_data_version"`
	Files               []FileInfo `json:"files"`
	CheckpointFiles     []FileInfo `json:"checkpoint_files"`
	CreatedAt           time.Time  `json:"created_at"`
}

// FileInfo describes a file in a backup.
type FileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

const metaFileName = "meta.json"

func newMetaRetryPolicy(maxRetries int, initialBackoff time.Duration) retrypolicy.RetryPolicy[any] {
	return retrypolicy.NewBuilder[any]().
		HandleIf(func(_ any, err error) bool {
			return isRetryableError(err)
		}).
		WithMaxRetries(maxRetries).
		WithBackoff(initialBackoff, MaxRetryBackoff).
		WithJitterFactor(0.25).
		ReturnLastFailure().
		Build()
}

func writeMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string, meta *BackupMeta, maxRetries int, initialBackoff time.Duration) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal backup meta: %w", err)
	}

	objName := path.Join(objectPrefix, metaFileName)
	policy := newMetaRetryPolicy(maxRetries, initialBackoff)

	err = failsafe.With[any](policy).
		WithContext(ctx).
		Run(func() error {
			return bucket.Upload(ctx, objName, bytes.NewReader(data))
		})
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("write backup meta %s: %w", objName, ctx.Err())
		}
		return fmt.Errorf("write backup meta %s: %w", objName, err)
	}
	return nil
}

func readMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string, maxRetries int, initialBackoff time.Duration) (*BackupMeta, error) {
	objName := path.Join(objectPrefix, metaFileName)
	policy := newMetaRetryPolicy(maxRetries, initialBackoff)

	var data []byte
	err := failsafe.With[any](policy).
		WithContext(ctx).
		Run(func() error {
			rc, gErr := bucket.Get(ctx, objName)
			if gErr != nil {
				return gErr
			}
			defer rc.Close()

			var rErr error
			data, rErr = io.ReadAll(rc)
			return rErr
		})
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("read backup meta %s: %w", objName, ctx.Err())
		}
		return nil, fmt.Errorf("read backup meta %s: %w", objName, err)
	}

	var meta BackupMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal backup meta: %w", err)
	}
	if err := validateMetaFileNames(&meta); err != nil {
		return nil, fmt.Errorf("invalid backup meta %s: %w", objName, err)
	}
	return &meta, nil
}

// ReadBackupMeta reads and returns the metadata for the backup at objectPrefix.
func ReadBackupMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string) (*BackupMeta, error) {
	return readMeta(ctx, bucket, objectPrefix, DefaultMaxDownloadRetries, DefaultInitialRetryBackoff)
}

// validateMetaFileNames rejects file names that could escape the target
// directory via path traversal (e.g. "../../etc/passwd") or absolute paths.
func validateMetaFileNames(meta *BackupMeta) error {
	for _, list := range [2][]FileInfo{meta.Files, meta.CheckpointFiles} {
		for _, fi := range list {
			if err := validateFileName(fi.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFileName(name string) error {
	if name == "" {
		return fmt.Errorf("file name is empty")
	}
	if filepath.IsAbs(name) {
		return fmt.Errorf("file name %q is an absolute path", name)
	}
	for _, part := range strings.Split(filepath.ToSlash(name), "/") {
		if part == ".." {
			return fmt.Errorf("file name %q contains path traversal", name)
		}
	}
	return nil
}

func generateUUID() string {
	return uuid.Must(uuid.NewV7()).String()
}

func newBackupMeta(backupUUID string, bt BackupType, dt time.Time, pebbleFmtVer uint64, bondDataVer uint32, files, checkpointFiles []FileInfo) *BackupMeta {
	return &BackupMeta{
		UUID:                backupUUID,
		Type:                bt,
		Datetime:            dt.UTC(),
		PebbleFormatVersion: pebbleFmtVer,
		BondDataVersion:     bondDataVer,
		Files:               files,
		CheckpointFiles:     checkpointFiles,
		CreatedAt:           time.Now().UTC(),
	}
}

const localMetaDir = "bond"
const localMetaFile = "meta.json"

func writeLocalMeta(dbDir string, meta *BackupMeta) error {
	dir := filepath.Join(dbDir, localMetaDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create local meta dir: %w", err)
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal local meta: %w", err)
	}
	return utils.WriteFileWithSync(filepath.Join(dir, localMetaFile), data, 0644)
}

func readLocalMeta(dbDir string) (*BackupMeta, error) {
	p := filepath.Join(dbDir, localMetaDir, localMetaFile)
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read local meta: %w", err)
	}
	var meta BackupMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal local meta: %w", err)
	}
	return &meta, nil
}
