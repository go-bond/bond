package main

import (
	"github.com/go-bond/bond"
	"github.com/urfave/cli/v2"
)

var _FlagExportPath = &cli.StringFlag{
	Name:     "dir",
	Usage:    "sets export dir",
	Required: true,
}

var _FlagExportIndex = &cli.BoolFlag{
	Name:     "with-index",
	Usage:    "exports index",
	Required: false,
}

var _FlagExportBondDir = &cli.StringFlag{
	Name:     "bond-dir",
	Usage:    "sets bond dir",
	Required: true,
}

var _FlagExportTables = &cli.IntSliceFlag{
	Name:     "tables",
	Usage:    "sets table ids",
	Required: true,
}

var DumpCommand *cli.Command

func init() {
	DumpCommand = &cli.Command{
		Name:  "dump",
		Usage: "dumps bond db to the specified directory",
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
			return db.Dump(ctx.Context, ctx.String("dir"), tables, ctx.Bool("with-index"))
		},
	}
}
