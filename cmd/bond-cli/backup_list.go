package main

import (
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/go-bond/bond/backup"
	"github.com/urfave/cli/v2"
)

type backupGroup struct {
	backups []backup.BackupInfo
	metas   []*backup.BackupMeta
}

func backupListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list all backups grouped by complete + incrementals",
		Flags: bucketFlags,
		Action: func(ctx *cli.Context) error {
			bucket, err := newBucket(ctx)
			if err != nil {
				return err
			}
			defer bucket.Close()

			prefix := ctx.String("prefix")
			backups, err := backup.ListBackups(ctx.Context, bucket, prefix)
			if err != nil {
				return fmt.Errorf("list backups: %w", err)
			}

			if len(backups) == 0 {
				fmt.Println("No backups found.")
				return nil
			}

			// Read metadata for each backup.
			metas := make([]*backup.BackupMeta, len(backups))
			for i, b := range backups {
				meta, err := backup.ReadBackupMeta(ctx.Context, bucket, b.Prefix)
				if err != nil {
					return fmt.Errorf("read meta for %s: %w", b.Prefix, err)
				}
				metas[i] = meta
			}

			// Group backups: each complete starts a new group.
			groups := groupBackups(backups, metas)

			var totalBackups int
			var totalFiles int
			var totalSize int64

			for i, g := range groups {
				fmt.Printf("BACKUP GROUP %d (%s)\n", i+1,
					g.backups[0].Datetime.UTC().Format("2006-01-02 15:04:05 UTC"))
				fmt.Printf("  %-15s %-27s %-8s %-12s %s\n",
					"TYPE", "DATETIME", "FILES", "SIZE", "PREFIX")

				var groupFiles int
				var groupSize int64

				for j, b := range g.backups {
					meta := g.metas[j]
					files := len(meta.Files)
					var size int64
					for _, f := range meta.Files {
						size += f.Size
					}

					fmt.Printf("  %-15s %-27s %-8d %-12s %s\n",
						b.Type,
						b.Datetime.UTC().Format("2006-01-02 15:04:05 UTC"),
						files,
						humanize.IBytes(uint64(size)),
						b.Prefix)

					groupFiles += files
					groupSize += size
				}

				fmt.Printf("  Group total: %d files, %s\n\n",
					groupFiles, humanize.IBytes(uint64(groupSize)))

				totalBackups += len(g.backups)
				totalFiles += groupFiles
				totalSize += groupSize
			}

			fmt.Printf("Total: %d backups, %d files, %s\n",
				totalBackups, totalFiles, humanize.IBytes(uint64(totalSize)))

			return nil
		},
	}
}

func groupBackups(backups []backup.BackupInfo, metas []*backup.BackupMeta) []backupGroup {
	var groups []backupGroup
	var current *backupGroup

	for i, b := range backups {
		if b.Type == backup.BackupTypeComplete {
			if current != nil {
				groups = append(groups, *current)
			}
			current = &backupGroup{}
		}
		if current == nil {
			// Orphaned incremental before any complete; start a group anyway.
			current = &backupGroup{}
		}
		current.backups = append(current.backups, b)
		current.metas = append(current.metas, metas[i])
	}
	if current != nil {
		groups = append(groups, *current)
	}
	return groups
}

