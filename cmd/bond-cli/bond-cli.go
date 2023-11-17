package main

import (
	"fmt"
	"os"

	"github.com/go-bond/bond/inspect"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "bond-cli",
		Usage: "tools to manage bond db",
		Commands: []*cli.Command{
			inspect.NewInspectCLI(nil),
			DumpCommand,
			RestoreCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[Error] %s\n", err.Error())
	}
}
