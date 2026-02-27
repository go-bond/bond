package backup

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

const datetimeFormat = "20060102150405"

// BackupType represents whether a backup is complete or incremental.
type BackupType string

const (
	BackupTypeComplete    BackupType = "complete"
	BackupTypeIncremental BackupType = "incremental"
)

func backupDirName(t time.Time, bt BackupType, seq int) string {
	return fmt.Sprintf("%s-%s-%05d", t.UTC().Format(datetimeFormat), bt, seq)
}

func backupObjectPrefix(prefix string, t time.Time, bt BackupType, seq int) string {
	return path.Join(prefix, backupDirName(t, bt, seq)) + "/"
}

func parseBackupDir(name string) (time.Time, BackupType, int, error) {
	name = strings.TrimSuffix(name, "/")
	name = path.Base(name)

	parts := strings.SplitN(name, "-", 3)
	if len(parts) != 3 {
		return time.Time{}, "", 0, fmt.Errorf("invalid backup dir name: %q", name)
	}

	t, err := time.Parse(datetimeFormat, parts[0])
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("invalid datetime in backup dir %q: %w", name, err)
	}

	bt := BackupType(parts[1])
	switch bt {
	case BackupTypeComplete, BackupTypeIncremental:
	default:
		return time.Time{}, "", 0, fmt.Errorf("invalid backup type in dir %q: %q", name, parts[1])
	}

	seq, err := strconv.Atoi(parts[2])
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("invalid seq in backup dir %q: %w", name, err)
	}

	return t.UTC(), bt, seq, nil
}
