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
	Seq      int
}

// ErrNoBackupsFound is returned when no suitable backups exist.
var ErrNoBackupsFound = fmt.Errorf("no backups found")

// ErrIncompleteRestoreChain is returned when the backup chain has gaps
// (one or more intermediate backups have been removed).
var ErrIncompleteRestoreChain = fmt.Errorf("backup chain is incomplete: one or more backups may have been removed")

// ListBackups discovers all backups under the given prefix and returns them
// sorted by datetime ascending.
func ListBackups(ctx context.Context, bucket objstore.Bucket, prefix string) ([]BackupInfo, error) {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var backups []BackupInfo
	err := bucket.Iter(ctx, prefix, func(name string) error {
		dt, bt, seq, err := parseBackupDir(name)
		if err != nil {
			// Skip entries that don't match the backup dir pattern.
			return nil
		}

		incomplete, err := isBackupIncomplete(ctx, bucket, name)
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
			Seq:      seq,
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
	if before.IsZero() {
		filtered = allBackups
	} else {
		for _, b := range allBackups {
			if !b.Datetime.After(before) {
				filtered = append(filtered, b)
			}
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
	restoreSet := filtered[latestCompleteIdx:]

	// Verify the chain has no gaps by checking sequence number contiguity.
	for i, b := range restoreSet {
		expected := restoreSet[0].Seq + i
		if b.Seq != expected {
			return nil, fmt.Errorf("%w: expected seq %d but got %d at %s",
				ErrIncompleteRestoreChain, expected, b.Seq, b.Prefix)
		}
	}

	return restoreSet, nil
}
