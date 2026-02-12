package backup

import (
	"context"
	"fmt"
	"path"

	"github.com/thanos-io/objstore"
)

// isOrphaned checks whether a backup directory is orphaned (missing meta.json).
func isOrphaned(ctx context.Context, bucket objstore.Bucket, backupPrefix string) (bool, error) {
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

// removeOrphanedDirs iterates all backup directories under prefix, identifies
// orphans (directories without meta.json), and deletes them. Returns the count removed.
func removeOrphanedDirs(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error) {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var orphanPrefixes []string
	err := bucket.Iter(ctx, prefix, func(name string) error {
		if _, _, err := parseBackupDir(name); err != nil {
			return nil
		}

		orphaned, err := isOrphaned(ctx, bucket, name)
		if err != nil {
			return err
		}
		if orphaned {
			orphanPrefixes = append(orphanPrefixes, name)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("iter bucket for orphans: %w", err)
	}

	for _, p := range orphanPrefixes {
		if err := deleteBackupDir(ctx, bucket, p); err != nil {
			return 0, err
		}
	}

	return len(orphanPrefixes), nil
}

// RemoveOrphaned removes orphaned backup directories (those without meta.json)
// under the given prefix. Returns the number of orphaned directories removed.
func RemoveOrphaned(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error) {
	return removeOrphanedDirs(ctx, bucket, prefix)
}
