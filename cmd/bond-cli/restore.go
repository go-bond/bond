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
			_FlagExportTables,
		},
		Action: func(ctx *cli.Context) error {
			db, err := bond.Open(ctx.String("bond-dir"), nil)
			if err != nil {
				return err
			}
			var tables []bond.TableID
			for _, id := range ctx.IntSlice("tables") {
				tables = append(tables, bond.TableID(id))
			}
			return db.Restore(ctx.Context, ctx.String("dir"), tables, ctx.Bool("with-index"))
		},
	}
}
