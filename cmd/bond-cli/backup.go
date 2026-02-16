package main

import "github.com/urfave/cli/v2"

var BackupCommand *cli.Command

func init() {
	BackupCommand = &cli.Command{
		Name:  "backup",
		Usage: "manage backups in S3-compatible object storage",
		Subcommands: []*cli.Command{
			backupListCommand(),
			backupDeleteCommand(),
			backupRestoreCommand(),
		},
	}
}
