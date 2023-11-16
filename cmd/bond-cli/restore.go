package main

import (
	"github.com/go-bond/bond"
	"github.com/urfave/cli/v2"
)

var RestoreCommand *cli.Command

func init() {
	RestoreCommand = &cli.Command{
		Name:  "restore",
		Usage: "restore bond db from the specifed directory",
		Flags: []cli.Flag{
			_FlagExportBondDir,
			_FlagExportIndex,
			_FlagExportPath,
		},
		Action: func(ctx *cli.Context) error {
			db, err := bond.Open(ctx.String("bond-dir"), nil)
			if err != nil {
				return err
			}
			return db.Restore(ctx.Context, ctx.String("dir"), ctx.Bool("index"))
		},
	}
}
