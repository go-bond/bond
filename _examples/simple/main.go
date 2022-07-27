package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
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
	fmt.Println("=>")

	db, err := bond.Open("example", &bond.Options{})
	if err != nil {
		panic(err)
	}

	defer func() { _ = os.RemoveAll("example") }()

	const (
		ExampleStructTableId bond.TableID = 1
	)

	ExampleStructTable := bond.NewTable[*ExampleStruct](
		db,
		ExampleStructTableId,
		func(b bond.KeyBuilder, es *ExampleStruct) []byte {
			return b.AddUint64Field(es.Id).Bytes()
		})

	var (
		ExampleStructTypeIndex = bond.NewIndex[*ExampleStruct](
			bond.PrimaryIndexID+1,
			func(b bond.KeyBuilder, es *ExampleStruct) []byte {
				return b.AddBytesField([]byte(es.Type)).Bytes()
			},
			bond.IndexOrderDefault[*ExampleStruct],
		)
		ExampleStructOrderAmountDESCIndex = bond.NewIndex[*ExampleStruct](
			bond.PrimaryIndexID+2,
			func(b bond.KeyBuilder, es *ExampleStruct) []byte {
				return b.Bytes()
			},
			func(o bond.IndexOrder, es *ExampleStruct) bond.IndexOrder {
				return o.OrderUint64(es.Amount, bond.IndexOrderTypeDESC)
			},
		)
		ExampleStructIsActivePartialIndex = bond.NewIndex[*ExampleStruct](
			bond.PrimaryIndexID+3,
			func(b bond.KeyBuilder, es *ExampleStruct) []byte {
				return b.Bytes()
			},
			bond.IndexOrderDefault[*ExampleStruct],
			func(es *ExampleStruct) bool {
				return es.IsActive
			},
		)
	)

	err = ExampleStructTable.AddIndex([]*bond.Index[*ExampleStruct]{
		ExampleStructTypeIndex,
		ExampleStructOrderAmountDESCIndex,
		ExampleStructIsActivePartialIndex,
	})
	if err != nil {
		panic(err)
	}

	exapleStructs := []*ExampleStruct{
		{
			Id:          1,
			Type:        "test",
			IsActive:    true,
			Description: "test description",
			Amount:      1,
		},
		{
			Id:          2,
			Type:        "test",
			IsActive:    false,
			Description: "test description 2",
			Amount:      7,
		},
		{
			Id:          3,
			Type:        "test",
			IsActive:    true,
			Description: "test description 3",
			Amount:      5,
		},
		{
			Id:          4,
			Type:        "test",
			IsActive:    true,
			Description: "test description 4",
			Amount:      3,
		},
		{
			Id:          5,
			Type:        "test2",
			IsActive:    true,
			Description: "test description 5",
			Amount:      2,
		},
	}

	fmt.Println("==> Insert")
	err = ExampleStructTable.Insert(exapleStructs)
	if err != nil {
		panic(err)
	}

	var exampleStructsFromQuery []*ExampleStruct
	err = ExampleStructTable.Query().Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query All")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")

	err = ExampleStructTable.Query().
		With(ExampleStructIsActivePartialIndex, &ExampleStruct{IsActive: true}).
		Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query only active using Index")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")

	err = ExampleStructTable.Query().
		With(ExampleStructTypeIndex, &ExampleStruct{Type: "test"}).
		Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query only of type 'test' using Index")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")

	err = ExampleStructTable.Query().
		With(ExampleStructOrderAmountDESCIndex, &ExampleStruct{}).
		Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query ordered by balance DESC using ordered index")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")

	err = ExampleStructTable.Query().
		Offset(1).
		Limit(2).
		Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query offset=1 and limit=2")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")

	err = ExampleStructTable.Query().
		Filter(func(es *ExampleStruct) bool {
			return es.Amount > 5
		}).
		Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query with filter es.Amount > 5")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")

	err = ExampleStructTable.Query().
		Order(func(es *ExampleStruct, es2 *ExampleStruct) bool {
			return es.Amount < es2.Amount
		}).
		Execute(&exampleStructsFromQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Query with ordering without using ordered indexes")
	spew.Dump(exampleStructsFromQuery)
	fmt.Println("")
}
