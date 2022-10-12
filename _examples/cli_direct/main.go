package main

import (
	"fmt"
	"os"

	"github.com/go-bond/bond"
)

type ExampleStruct struct {
	Id          uint64 `json:"id"`
	Type        string `json:"type"`
	IsActive    bool   `json:"isActive"`
	Description string `json:"description"`
	Amount      uint64 `json:"amount"`
}

func main() {
	app := bond.NewInspectCLI(func(path string) (bond.Inspect, error) {
		db, err := bond.Open(path, &bond.Options{})
		if err != nil {
			panic(err)
		}

		const (
			ExampleStructTableID bond.TableID = 1
		)

		ExampleStructTable := bond.NewTable[*ExampleStruct](bond.TableOptions[*ExampleStruct]{
			DB:        db,
			TableID:   ExampleStructTableID,
			TableName: "example_stuct_table",
			TablePrimaryKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
				return b.AddUint64Field(es.Id).Bytes()
			},
		})

		var (
			ExampleStructTypeIndex = bond.NewIndex[*ExampleStruct](bond.IndexOptions[*ExampleStruct]{
				IndexID:   bond.PrimaryIndexID + 1,
				IndexName: "type_idx",
				IndexKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
					return b.AddBytesField([]byte(es.Type)).Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*ExampleStruct],
			})
			ExampleStructOrderAmountDESCIndex = bond.NewIndex[*ExampleStruct](bond.IndexOptions[*ExampleStruct]{
				IndexID:   bond.PrimaryIndexID + 2,
				IndexName: "main_ord_amount_desc_idx",
				IndexKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
					return b.Bytes()
				},
				IndexOrderFunc: func(o bond.IndexOrder, es *ExampleStruct) bond.IndexOrder {
					return o.OrderUint64(es.Amount, bond.IndexOrderTypeDESC)
				},
			})
			ExampleStructIsActivePartialIndex = bond.NewIndex[*ExampleStruct](bond.IndexOptions[*ExampleStruct]{
				IndexID:   bond.PrimaryIndexID + 3,
				IndexName: "main_isactive_true_idx",
				IndexKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
					return b.Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*ExampleStruct],
				IndexFilterFunc: func(es *ExampleStruct) bool {
					return es.IsActive
				},
			})
		)

		err = ExampleStructTable.AddIndex([]*bond.Index[*ExampleStruct]{
			ExampleStructTypeIndex,
			ExampleStructOrderAmountDESCIndex,
			ExampleStructIsActivePartialIndex,
		})
		if err != nil {
			panic(err)
		}

		return bond.NewInspect([]bond.TableInfo{ExampleStructTable})
	})

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("[Error] %s\n", err.Error())
	}
}
