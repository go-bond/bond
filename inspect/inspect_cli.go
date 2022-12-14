package inspect

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-bond/bond"
	"github.com/urfave/cli/v2"
)

var _FlagBondURL = &cli.StringFlag{
	Name:     "url",
	Usage:    "sets bond url",
	Required: true,
}

var _FlagHeaders = &cli.StringSliceFlag{
	Name:     "headers",
	Usage:    "sets http headers",
	Value:    cli.NewStringSlice(),
	Required: false,
}

var _FlagTable = &cli.StringFlag{
	Name:     "table",
	Usage:    "sets table",
	Required: true,
}

var _FlagIndex = &cli.StringFlag{
	Name:     "index",
	Usage:    "sets query index",
	Value:    bond.PrimaryIndexName,
	Required: false,
}

var _FlagIndexSelector = &cli.StringFlag{
	Name:     "index-selector",
	Usage:    "sets query index selector",
	Required: false,
}

var _FlagFilter = &cli.StringFlag{
	Name:     "filter",
	Usage:    "sets query filter",
	Required: false,
}

var _FlagLimit = &cli.Uint64Flag{
	Name:     "limit",
	Usage:    "sets query row limit",
	Value:    30,
	Required: false,
}

var _FlagAfter = &cli.StringFlag{
	Name:     "after",
	Usage:    "sets query after",
	Required: false,
}

var _FlagDeadline = &cli.DurationFlag{
	Name:     "deadline",
	Usage:    "sets query deadline",
	Value:    15 * time.Second,
	Required: false,
}

func NewInspectCLI(init func(path string) (Inspect, error)) *cli.App {
	var (
		inspect Inspect
		err     error
	)

	return &cli.App{
		Name: "bond-cli",
		Usage: "The cli for bond database.\n\n" +
			"bond-cli --url .bond tables\n" +
			"bond-cli --url http://localhost:7777/bond tables\n" +
			"bond-cli --url http://localhost:7777/bond indexes --table token_balances\n" +
			"bond-cli --url http://localhost:7777/bond entry-fields --table token_balances",
		Flags: []cli.Flag{
			_FlagBondURL,
			_FlagHeaders,
		},
		Before: func(ctx *cli.Context) error {
			url := ctx.String(_FlagBondURL.Name)
			if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
				headersStr := ctx.StringSlice(_FlagHeaders.Name)

				headers := make(map[string]string)
				for _, s := range headersStr {
					header := strings.SplitN(s, "=", 2)
					if len(header) != 2 {
						return fmt.Errorf("invalid header: %s", s)
					}

					headers[header[0]] = header[1]
				}

				inspect = NewInspectRemote(url, headers)
			} else {
				if init == nil {
					return fmt.Errorf("this CLI only supports http & https urls")
				}

				inspect, err = init(url)
				if err != nil {
					return fmt.Errorf("failed to initialize Inspect - %w", err)
				}
			}
			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "tables",
				Usage: "lists table names",
				Action: func(ctx *cli.Context) error {
					tables, err := inspect.Tables()
					if err != nil {
						return err
					}

					resultJson, err := json.Marshal(tables)
					if err != nil {
						return err
					}

					fmt.Print(string(resultJson))
					return nil
				},
			},
			{
				Name:  "indexes",
				Usage: "lists index names for given table",
				Flags: []cli.Flag{
					_FlagTable,
				},
				Action: func(ctx *cli.Context) error {
					indexes, err := inspect.Indexes(ctx.String(_FlagTable.Name))
					if err != nil {
						return err
					}

					resultJson, err := json.Marshal(indexes)
					if err != nil {
						return err
					}

					fmt.Print(string(resultJson))
					return nil
				},
			},
			{
				Name:  "entry-fields",
				Usage: "lists entry fields for given table",
				Flags: []cli.Flag{
					_FlagTable,
				},
				Action: func(ctx *cli.Context) error {
					fields, err := inspect.EntryFields(ctx.String(_FlagTable.Name))
					if err != nil {
						return err
					}

					resultJson, err := json.Marshal(fields)
					if err != nil {
						return err
					}

					fmt.Print(string(resultJson))
					return nil
				},
			},
			{
				Name:  "query",
				Usage: "executes query",
				Flags: []cli.Flag{
					_FlagTable,
					_FlagIndex,
					_FlagIndexSelector,
					_FlagFilter,
					_FlagLimit,
					_FlagAfter,
					_FlagDeadline,
				},
				Action: func(ctx *cli.Context) error {
					queryCtx, cancel := context.WithDeadline(
						context.Background(), time.Now().Add(ctx.Duration(_FlagDeadline.Name)))
					defer cancel()

					var indexSelector map[string]interface{}
					if indexSelectorStr := ctx.String(_FlagIndexSelector.Name); indexSelectorStr != "" {
						err = json.Unmarshal([]byte(indexSelectorStr), &indexSelector)
						if err != nil {
							return err
						}
					}

					var filter map[string]interface{}
					if filterStr := ctx.String(_FlagFilter.Name); filterStr != "" {
						err = json.Unmarshal([]byte(filterStr), &filter)
						if err != nil {
							return err
						}
					}

					var after map[string]interface{}
					if afterStr := ctx.String(_FlagAfter.Name); afterStr != "" {
						err = json.Unmarshal([]byte(afterStr), &after)
						if err != nil {
							return err
						}
					}

					result, err := inspect.Query(
						queryCtx,
						ctx.String(_FlagTable.Name),
						ctx.String(_FlagIndex.Name),
						indexSelector,
						filter,
						ctx.Uint64(_FlagLimit.Name),
						after,
					)
					if err != nil {
						return err
					}

					resultJson, err := json.Marshal(result)
					if err != nil {
						return err
					}

					fmt.Print(string(resultJson))
					return nil
				},
			},
		},
		HideHelp:        true,
		HideHelpCommand: true,
	}
}
