package backup

import (
	"fmt"
	"path"
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

func backupDirName(t time.Time, bt BackupType) string {
	return fmt.Sprintf("%s-%s", t.UTC().Format(datetimeFormat), bt)
}

func backupObjectPrefix(prefix string, t time.Time, bt BackupType) string {
	return path.Join(prefix, backupDirName(t, bt)) + "/"
}

func parseBackupDir(name string) (time.Time, BackupType, error) {
	name = strings.TrimSuffix(name, "/")
	// Take only the last path component.
	name = path.Base(name)

	parts := strings.SplitN(name, "-", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("invalid backup dir name: %q", name)
	}

	t, err := time.Parse(datetimeFormat, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid datetime in backup dir %q: %w", name, err)
	}

	bt := BackupType(parts[1])
	switch bt {
	case BackupTypeComplete, BackupTypeIncremental:
	default:
		return time.Time{}, "", fmt.Errorf("invalid backup type in dir %q: %q", name, parts[1])
	}

	return t.UTC(), bt, nil
}
