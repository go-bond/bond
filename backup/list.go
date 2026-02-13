package backup

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/thanos-io/objstore"
)

// BackupInfo describes a discovered backup.
type BackupInfo struct {
	Datetime time.Time
	Type     BackupType
	Prefix   string
}

// ErrNoBackupsFound is returned when no suitable backups exist.
var ErrNoBackupsFound = fmt.Errorf("no backups found")

// ListBackups discovers all backups under the given prefix and returns them
// sorted by datetime ascending.
func ListBackups(ctx context.Context, bucket objstore.Bucket, prefix string) ([]BackupInfo, error) {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var backups []BackupInfo
	err := bucket.Iter(ctx, prefix, func(name string) error {
		dt, bt, err := parseBackupDir(name)
		if err != nil {
			// Skip entries that don't match the backup dir pattern.
			return nil
		}

		incomplete, err := isIncomplete(ctx, bucket, name)
		if err != nil {
			return err
		}
		if incomplete {
			return nil
		}

		backups = append(backups, BackupInfo{
			Datetime: dt,
			Type:     bt,
			Prefix:   name,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iter bucket: %w", err)
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Datetime.Before(backups[j].Datetime)
	})

	return backups, nil
}

// FindRestoreSet finds the set of backups needed to restore to a point in time.
// It returns the latest complete backup at or before the cutoff, plus all
// subsequent incrementals up to the cutoff, sorted by datetime ascending.
func FindRestoreSet(ctx context.Context, bucket objstore.Bucket, prefix string, before time.Time) ([]BackupInfo, error) {
	allBackups, err := ListBackups(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}

	before = before.UTC()

	// Filter to backups at or before the cutoff.
	var filtered []BackupInfo
	for _, b := range allBackups {
		if !b.Datetime.After(before) {
			filtered = append(filtered, b)
		}
	}

	// Find the latest complete backup.
	latestCompleteIdx := -1
	for i := len(filtered) - 1; i >= 0; i-- {
		if filtered[i].Type == BackupTypeComplete {
			latestCompleteIdx = i
			break
		}
	}

	if latestCompleteIdx < 0 {
		return nil, ErrNoBackupsFound
	}

	// Return the complete backup plus all subsequent incrementals.
	return filtered[latestCompleteIdx:], nil
}
