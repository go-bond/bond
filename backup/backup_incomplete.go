package backup

import (
	"context"
	"fmt"
	"path"

	"github.com/thanos-io/objstore"
)

// isBackupIncomplete checks whether a backup directory is incomplete (missing meta.json).
func isBackupIncomplete(ctx context.Context, bucket objstore.Bucket, backupPrefix string) (bool, error) {
	metaPath := path.Join(backupPrefix, metaFileName)
	exists, err := bucket.Exists(ctx, metaPath)
	if err != nil {
		return false, fmt.Errorf("check meta.json existence: %w", err)
	}
	return !exists, nil
}

// deleteBackupDir deletes all objects under the given directory prefix.
func deleteBackupDir(ctx context.Context, bucket objstore.Bucket, dirPrefix string) error {
	var objects []string
	err := bucket.Iter(ctx, dirPrefix, func(name string) error {
		objects = append(objects, name)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return fmt.Errorf("iter backup dir %s: %w", dirPrefix, err)
	}

	for _, obj := range objects {
		if err := bucket.Delete(ctx, obj); err != nil {
			if bucket.IsObjNotFoundErr(err) {
				continue
			}
			return fmt.Errorf("delete %s: %w", obj, err)
		}
	}
	return nil
}

// removeIncompleteBackupDirs iterates all backup directories under prefix, identifies
// incomplete backups (directories without meta.json), and deletes them. Returns the count removed.
func removeIncompleteBackupDirs(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error) {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var incompletePrefixes []string
	err := bucket.Iter(ctx, prefix, func(name string) error {
		if _, _, err := parseBackupDir(name); err != nil {
			return nil
		}

		incomplete, err := isBackupIncomplete(ctx, bucket, name)
		if err != nil {
			return err
		}
		if incomplete {
			incompletePrefixes = append(incompletePrefixes, name)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("iter bucket for incomplete backups: %w", err)
	}

	for _, p := range incompletePrefixes {
		if err := deleteBackupDir(ctx, bucket, p); err != nil {
			return 0, err
		}
	}

	return len(incompletePrefixes), nil
}

// DeleteBackup deletes all objects under the given backup prefix.
func DeleteBackup(ctx context.Context, bucket objstore.Bucket, backupPrefix string) error {
	return deleteBackupDir(ctx, bucket, backupPrefix)
}

// RemoveIncompleteBackups removes incomplete backup directories (those without meta.json)
// under the given prefix. Returns the number of incomplete directories removed.
func RemoveIncompleteBackups(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error) {
	return removeIncompleteBackupDirs(ctx, bucket, prefix)
}
