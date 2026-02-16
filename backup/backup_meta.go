package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/thanos-io/objstore"
)

// BackupMeta contains metadata about a single backup.
type BackupMeta struct {
	Type                BackupType `json:"type"`
	Datetime            string     `json:"datetime"`
	PebbleFormatVersion uint64     `json:"pebble_format_version"`
	BondDataVersion     uint32     `json:"bond_data_version"`
	Files               []FileInfo `json:"files"`
	CheckpointFiles     []FileInfo `json:"checkpoint_files"`
	CreatedAt           string     `json:"created_at"`
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
			return isRetriableError(err)
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
	return &meta, nil
}

// ReadMeta reads and returns the metadata for the backup at objectPrefix.
func ReadMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string) (*BackupMeta, error) {
	return readMeta(ctx, bucket, objectPrefix, DefaultMaxDownloadRetries, DefaultInitialRetryBackoff)
}

func newBackupMeta(bt BackupType, dt time.Time, pebbleFmtVer uint64, bondDataVer uint32, files, checkpointFiles []FileInfo) *BackupMeta {
	return &BackupMeta{
		Type:                bt,
		Datetime:            dt.UTC().Format(datetimeFormat),
		PebbleFormatVersion: pebbleFmtVer,
		BondDataVersion:     bondDataVer,
		Files:               files,
		CheckpointFiles:     checkpointFiles,
		CreatedAt:           time.Now().UTC().Format(time.RFC3339),
	}
}
