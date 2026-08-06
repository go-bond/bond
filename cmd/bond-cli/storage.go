package main

import (
	"encoding/json"
	"fmt"

	"github.com/go-bond/bond"
	"github.com/urfave/cli/v2"
)

var StorageCommand = &cli.Command{
	Name:  "storage",
	Usage: "inspect physical Bond/Pebble storage compatibility",
	Subcommands: []*cli.Command{
		{
			Name:  "inspect",
			Usage: "report active, registered, and encountered SST key schemas",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "dir",
					Usage:    "offline Bond database directory",
					Required: true,
				},
			},
			Action: func(ctx *cli.Context) error {
				diagnostics, err := bond.InspectStorageDirectory(ctx.String("dir"))
				if err != nil {
					return fmt.Errorf("inspect storage: %w", err)
				}
				encoder := json.NewEncoder(ctx.App.Writer)
				encoder.SetIndent("", "  ")
				return encoder.Encode(diagnostics)
			},
		},
	},
}
