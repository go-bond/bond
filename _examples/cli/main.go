package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/inspect"
)

type ExampleStruct struct {
	Id          uint64 `json:"id"`
	Type        string `json:"type"`
	IsActive    bool   `json:"isActive"`
	Description string `json:"description"`
	Amount      uint64 `json:"amount"`
}

func main() {
	fmt.Println("=> CLI Example")

	db, err := bond.Open("example", &bond.Options{})
	if err != nil {
		panic(err)
	}

	defer func() { _ = os.RemoveAll("example") }()

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

	fmt.Println("==> Insert Records")
	err = ExampleStructTable.Insert(context.Background(), exapleStructs)
	if err != nil {
		panic(err)
	}

	fmt.Println("==> Start Inspect Handler")

	insp, err := inspect.NewInspect([]bond.TableInfo{ExampleStructTable})
	if err != nil {
		panic(err)
	}

	listen := make(chan struct{})

	go func() {
		listen <- struct{}{}

		err = http.ListenAndServe(":5555", inspect.NewInspectHandler(insp))
		if err != nil {
			panic(err)
		}
	}()

	<-listen
	fmt.Println("==> Example:")
	fmt.Println("    ./bond-cli --url http://localhost:5555/ tables")
	fmt.Println("    ./bond-cli --url http://localhost:5555/ indexes --table example_stuct_table")
	fmt.Println("    ./bond-cli --url http://localhost:5555/ entry-fields --table example_stuct_table")
	fmt.Println("    ./bond-cli --url http://localhost:5555/ query --table example_stuct_table")

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-c

	fmt.Println("==> Closing...")
}
