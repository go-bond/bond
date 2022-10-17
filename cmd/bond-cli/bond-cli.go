package main

import (
	"fmt"
	"os"

	"github.com/go-bond/bond"
)

func main() {
	app := bond.NewInspectCLI(nil)

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[Error] %s\n", err.Error())
	}
}
