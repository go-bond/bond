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

func writeMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string, meta *BackupMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal backup meta: %w", err)
	}
	return bucket.Upload(ctx, path.Join(objectPrefix, metaFileName), bytes.NewReader(data))
}

func readMeta(ctx context.Context, bucket objstore.Bucket, objectPrefix string) (*BackupMeta, error) {
	rc, err := bucket.Get(ctx, path.Join(objectPrefix, metaFileName))
	if err != nil {
		return nil, fmt.Errorf("get backup meta: %w", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read backup meta: %w", err)
	}

	var meta BackupMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal backup meta: %w", err)
	}
	return &meta, nil
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
