package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-bond/bond/backup"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
)

func backupDeleteCommand() *cli.Command {
	flags := append([]cli.Flag{
		&cli.BoolFlag{
			Name:  "all",
			Usage: "delete all backups under the prefix",
		},
		&cli.DurationFlag{
			Name:  "older-than",
			Usage: "delete backups older than this duration (e.g. 720h for 30 days)",
		},
		&cli.StringFlag{
			Name:  "datetime",
			Usage: "delete a specific backup by datetime-type (e.g. 20250212120000-complete)",
		},
		&cli.BoolFlag{
			Name:  "force",
			Usage: "skip confirmation prompt",
		},
		&cli.BoolFlag{
			Name:  "keep-last",
			Usage: "when using --older-than, keep the most recent complete backup chain to ensure at least one restorable backup remains (default: true)",
			Value: true,
		},
	}, bucketFlags...)

	return &cli.Command{
		Name:  "delete",
		Usage: "delete backups from object storage",
		Flags: flags,
		Action: func(ctx *cli.Context) error {
			deleteAll := ctx.Bool("all")
			olderThan := ctx.Duration("older-than")
			datetime := ctx.String("datetime")
			force := ctx.Bool("force")

			modes := 0
			if deleteAll {
				modes++
			}
			if olderThan > 0 {
				modes++
			}
			if datetime != "" {
				modes++
			}
			if modes == 0 {
				return fmt.Errorf("specify one of --all, --older-than, or --datetime")
			}
			if modes > 1 {
				return fmt.Errorf("--all, --older-than, and --datetime are mutually exclusive")
			}

			bucket, err := newBucket(ctx)
			if err != nil {
				return err
			}
			defer bucket.Close()

			prefix := ctx.String("prefix")
			allBackups, err := backup.ListBackups(ctx.Context, bucket, prefix)
			if err != nil {
				return fmt.Errorf("list backups: %w", err)
			}

			if len(allBackups) == 0 {
				fmt.Println("No backups found.")
				return nil
			}

			keepLast := ctx.Bool("keep-last")

			// Determine which backups to delete.
			var toDelete []backup.BackupInfo
			var keptByKeepLast int
			switch {
			case deleteAll:
				toDelete = allBackups
			case olderThan > 0:
				cutoff := time.Now().UTC().Add(-olderThan)
				toDelete, keptByKeepLast = selectOlderThan(allBackups, cutoff, keepLast)
			case datetime != "":
				toDelete = selectByDatetime(allBackups, datetime)
				if len(toDelete) == 0 {
					return fmt.Errorf("no backup found matching %q", datetime)
				}
			}

			if len(toDelete) == 0 {
				fmt.Println("No backups match the criteria.")
				return nil
			}

			// Read metadata for summary.
			var totalFiles int
			var totalSize int64
			for _, b := range toDelete {
				meta, err := backup.ReadBackupMeta(ctx.Context, bucket, b.Prefix)
				if err != nil {
					return fmt.Errorf("read meta for %s: %w", b.Prefix, err)
				}
				totalFiles += len(meta.Files)
				for _, f := range meta.Files {
					totalSize += f.Size
				}
			}

			// Print what will be deleted.
			fmt.Printf("The following %d backup(s) will be deleted (%d files, %s):\n\n",
				len(toDelete), totalFiles, humanize.IBytes(uint64(totalSize)))
			for _, b := range toDelete {
				fmt.Printf("  %-15s %s  %s\n",
					b.Type,
					b.Datetime.UTC().Format("2006-01-02 15:04:05 UTC"),
					b.Prefix)
			}
			fmt.Println()

			// Warn if --keep-last preserved backups.
			if keptByKeepLast > 0 {
				fmt.Printf("NOTE: --keep-last preserved %d backup(s) (the most recent complete chain) to ensure at least one restorable backup remains.\n"+
					"      Use --keep-last=false to delete all matching backups.\n\n", keptByKeepLast)
			}

			// Check for orphaned incrementals when deleting a complete backup.
			if !deleteAll {
				warnOrphanedIncrementals(toDelete, allBackups)
			}

		if !force {
			// When stdin is not a terminal (e.g. CI, piped input,
			// /dev/null), require --force instead of silently aborting.
			// Without this check, EOF from /dev/null or an empty pipe
			// would be interpreted as "N" with no indication of why the
			// deletion was skipped.
			if !isatty.IsTerminal(os.Stdin.Fd()) && !isatty.IsCygwinTerminal(os.Stdin.Fd()) {
				return fmt.Errorf("stdin is not a terminal; use --force to skip the confirmation prompt in non-interactive environments")
			}

			fmt.Print("Are you sure you want to proceed? [y/N]: ")
			reader := bufio.NewReader(os.Stdin)
			answer, _ := reader.ReadString('\n')
			answer = strings.TrimSpace(strings.ToLower(answer))
			if answer != "y" && answer != "yes" {
				fmt.Println("Aborted.")
				return nil
			}
		}

			// Perform deletion.
			for _, b := range toDelete {
				fmt.Printf("Deleting %s ...\n", b.Prefix)
				if err := backup.DeleteBackup(ctx.Context, bucket, b.Prefix); err != nil {
					return fmt.Errorf("delete %s: %w", b.Prefix, err)
				}
			}

			fmt.Printf("\nSuccessfully deleted %d backup(s).\n", len(toDelete))
			return nil
		},
	}
}

// selectOlderThan returns backups older than cutoff. When a complete backup is
// selected, its dependent incrementals (up to the next complete) are also included
// to avoid leaving orphaned incrementals.
//
// If keepLast is true and the selection would include every complete backup,
// the most recent complete backup and its dependent incrementals are excluded
// to ensure at least one restorable backup chain remains. The second return
// value reports how many backups were preserved by keepLast.
func selectOlderThan(all []backup.BackupInfo, cutoff time.Time, keepLast bool) ([]backup.BackupInfo, int) {
	selected := make(map[int]bool)

	for i, b := range all {
		if b.Datetime.Before(cutoff) {
			selected[i] = true
		}
	}

	// If a complete backup is selected, ensure its incrementals are also selected.
	for i, b := range all {
		if b.Type != backup.BackupTypeComplete || !selected[i] {
			continue
		}
		for j := i + 1; j < len(all); j++ {
			if all[j].Type == backup.BackupTypeComplete {
				break
			}
			selected[j] = true
		}
	}

	// Guard: if keepLast is set, check whether ALL complete backups are selected.
	// If so, exclude the last complete backup chain to prevent deleting everything.
	var kept int
	if keepLast {
		allCompleteSelected := true
		for i, b := range all {
			if b.Type == backup.BackupTypeComplete && !selected[i] {
				allCompleteSelected = false
				break
			}
		}

		if allCompleteSelected {
			// Find the last complete backup and exclude its chain.
			lastCompleteIdx := -1
			for i := len(all) - 1; i >= 0; i-- {
				if all[i].Type == backup.BackupTypeComplete {
					lastCompleteIdx = i
					break
				}
			}
			if lastCompleteIdx >= 0 {
				if selected[lastCompleteIdx] {
					delete(selected, lastCompleteIdx)
					kept++
				}
				for j := lastCompleteIdx + 1; j < len(all); j++ {
					if all[j].Type == backup.BackupTypeComplete {
						break
					}
					if selected[j] {
						delete(selected, j)
						kept++
					}
				}
			}
		}
	}

	var result []backup.BackupInfo
	for i, b := range all {
		if selected[i] {
			result = append(result, b)
		}
	}
	return result, kept
}

// selectByDatetime finds backups matching the given datetime-type string
// (e.g. "20250212120000-complete").
func selectByDatetime(all []backup.BackupInfo, datetime string) []backup.BackupInfo {
	var result []backup.BackupInfo
	for _, b := range all {
		dirName := b.Datetime.UTC().Format("20060102150405") + "-" + string(b.Type)
		if dirName == datetime {
			result = append(result, b)
		}
	}
	return result
}

// warnOrphanedIncrementals prints a warning if deleting a complete backup
// would orphan incrementals that are not in the delete set.
func warnOrphanedIncrementals(toDelete, all []backup.BackupInfo) {
	deleteSet := make(map[string]bool)
	for _, b := range toDelete {
		deleteSet[b.Prefix] = true
	}

	for i, b := range all {
		if b.Type != backup.BackupTypeComplete || !deleteSet[b.Prefix] {
			continue
		}
		var orphaned []backup.BackupInfo
		for j := i + 1; j < len(all); j++ {
			if all[j].Type == backup.BackupTypeComplete {
				break
			}
			if !deleteSet[all[j].Prefix] {
				orphaned = append(orphaned, all[j])
			}
		}
		if len(orphaned) > 0 {
			fmt.Printf("WARNING: Deleting complete backup %s will orphan %d incremental(s):\n",
				b.Prefix, len(orphaned))
			for _, o := range orphaned {
				fmt.Printf("  %s  %s\n",
					o.Datetime.UTC().Format("2006-01-02 15:04:05 UTC"),
					o.Prefix)
			}
			fmt.Println()
		}
	}
}
