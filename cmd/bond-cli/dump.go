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
	Name:     "index",
	Usage:    "exports index",
	Required: false,
}

var _FlagExportBondDir = &cli.StringFlag{
	Name:     "bond-dir",
	Usage:    "sets bond dir",
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
		},
		Action: func(ctx *cli.Context) error {
			db, err := bond.Open(ctx.String("bond-dir"), nil)
			if err != nil {
				return err
			}
			return db.Dump(ctx.Context, ctx.String("dir"), ctx.Bool("index"))
		},
	}
}
