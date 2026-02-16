package main

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-bond/bond/backup"
	"github.com/urfave/cli/v2"
)

func backupRestoreCommand() *cli.Command {
	flags := append([]cli.Flag{
		&cli.StringFlag{
			Name:     "restore-dir",
			Usage:    "local directory to restore into (must be empty or non-existent)",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "before",
			Usage: "point-in-time cutoff in RFC3339 format (e.g. 2025-02-12T15:00:00Z); if omitted, restores latest",
		},
		&cli.IntFlag{
			Name:  "concurrency",
			Usage: "number of parallel downloads",
			Value: backup.DefaultConcurrency,
		},
		&cli.Float64Flag{
			Name:  "rate-limit",
			Usage: "download rate limit in MB/s (0 for default, negative to disable)",
			Value: 0,
		},
	}, bucketFlags...)

	return &cli.Command{
		Name:  "restore",
		Usage: "restore a backup from object storage to a local directory",
		Flags: flags,
		Action: func(ctx *cli.Context) error {
			bucket, err := newBucket(ctx)
			if err != nil {
				return err
			}
			defer bucket.Close()

			var before time.Time
			if s := ctx.String("before"); s != "" {
				before, err = time.Parse(time.RFC3339, s)
				if err != nil {
					return fmt.Errorf("invalid --before value %q: expected RFC3339 format (e.g. 2025-02-12T15:00:00Z): %w", s, err)
				}
			}

			rateLimitMBs := ctx.Float64("rate-limit")
			var rateLimit float64
			if rateLimitMBs > 0 {
				rateLimit = rateLimitMBs * 1024 * 1024
			} else if rateLimitMBs < 0 {
				rateLimit = -1
			}

			opts := backup.RestoreOptions{
				Prefix:      ctx.String("prefix"),
				RestoreDir:  ctx.String("restore-dir"),
				Before:      before,
				Concurrency: ctx.Int("concurrency"),
				RateLimit:   rateLimit,
			OnProgress: func(ev backup.ProgressEvent) {
				fmt.Printf("\x1b[2K\r[%d/%d files] %s / %s  %s",
					ev.FilesDone, ev.FilesTotal,
					humanize.IBytes(uint64(ev.BytesDone)),
					humanize.IBytes(uint64(ev.BytesTotal)),
					ev.File)
			},
			}

			fmt.Printf("Restoring backup to %s ...\n", ctx.String("restore-dir"))
			if err := backup.Restore(ctx.Context, bucket, opts); err != nil {
				return fmt.Errorf("restore: %w", err)
			}

			fmt.Println("\nRestore completed successfully.")
			return nil
		},
	}
}
