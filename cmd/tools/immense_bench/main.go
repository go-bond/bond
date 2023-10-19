package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-bond/bond"
	"github.com/urfave/cli/v2"
)

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

type UniqueRand struct {
	generated    map[string]bool //keeps track of
	rng          *rand.Rand      //underlying random number generator
	generatedInt map[uint32]bool
}

// Generating unique rand
func NewUniqueRand() *UniqueRand {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	return &UniqueRand{
		generated:    map[string]bool{},
		rng:          r1,
		generatedInt: map[uint32]bool{},
	}
}

func (u *UniqueRand) String() string {
	for {
		i := RandomString(30)
		if !u.generated[i] {
			u.generated[i] = true
			return i
		}
	}
}
func (u *UniqueRand) Int() uint32 {
	for {
		i := u.rng.Uint32()
		if !u.generatedInt[i] {
			u.generatedInt[i] = true
			return i
		}
	}
}

type TokenBalance struct {
	ID              string `json:"id"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

var insertedEntries uint64

// Insert records to the bond db
// Number of entires = batchSize * totalBatch
func insertRecords(tableID bond.TableID, db bond.DB, batchSize, totalBatch int, wg *sync.WaitGroup) {
	idGenerator := NewUniqueRand()
	TokenBalanceTableID := bond.TableID(tableID)

	table := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableName: fmt.Sprintf("token_balance_%d", tableID),
		TableID:   TokenBalanceTableID,
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.ID).Bytes()
		},
	})

	accountIdx := bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 1,
		IndexName: "account_address_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
	})

	amountIdx := bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 2,
		IndexName: "account_amount_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, t *TokenBalance) []byte {
			return builder.Bytes()
		},
		IndexOrderFunc: func(o bond.IndexOrder, t *TokenBalance) bond.IndexOrder {
			return o.OrderUint64(t.Balance, bond.IndexOrderTypeDESC)
		},
	})

	tokenIdx := bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 3,
		IndexName: "token_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, t *TokenBalance) []byte {
			return builder.Bytes()
		},
		IndexOrderFunc: func(o bond.IndexOrder, t *TokenBalance) bond.IndexOrder {
			return o.OrderUint32(t.TokenID, bond.IndexOrderTypeDESC)
		},
	})

	contractIdx := bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 4,
		IndexName: "contract_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.ContractAddress).Bytes()
		},
		IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
	})

	err := table.AddIndex([]*bond.Index[*TokenBalance]{
		accountIdx,
		amountIdx,
		tokenIdx,
		contractIdx,
	})

	if err != nil {
		panic(err)
	}

	entries := make([]*TokenBalance, 0, batchSize)
	for i := 0; i < totalBatch; i++ {
		for j := 0; j < batchSize; j++ {
			id := idGenerator.String()
			n := idGenerator.Int()
			entries = append(entries, &TokenBalance{
				ID:              id,
				AccountID:       uint32(n % 10),
				ContractAddress: "0xtestContract" + fmt.Sprintf("%d", n),
				AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", n%5),
				Balance:         uint64((n % 100) * 10),
			})
		}
		err := table.Insert(context.TODO(), entries)
		if err != nil {
			panic(err)
		}
		atomic.AddUint64(&insertedEntries, uint64(batchSize))
		entries = entries[:0]
	}
	wg.Done()
}

func DirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	return size, err
}

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "batch_size",
				Value: 100,
				Usage: "size of the batch",
			},
			&cli.IntFlag{
				Name:  "total_batch",
				Value: 8000,
				Usage: "number of batch",
			},
			&cli.IntFlag{
				Name:  "total_table",
				Value: 8,
				Usage: "number of table",
			},
		},
		Action: func(cCtx *cli.Context) error {
			runBondInsert(cCtx.Int("total_table"),
				cCtx.Int("total_batch"),
				cCtx.Int("batch_size"))
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func runBondInsert(totalTable, totalBatch, batchSize int) {
	db, err := bond.Open("example", &bond.Options{})
	if err != nil {
		panic(err)
	}

	defer func() {
		sz, _ := DirSize("example")
		fmt.Printf("size of database %s \n", humanize.Bytes(sz))
		_ = os.RemoveAll("example")
	}()

	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < totalTable; i++ {
		wg.Add(1)
		func(id bond.TableID) {
			go insertRecords(id, db, batchSize, totalBatch, wg)
		}(bond.TableID(i))

	}

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		for range ticker.C {
			fmt.Printf("inserted records %d \n", atomic.LoadUint64(&insertedEntries))
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Total time taken to insert %s \n", elapsed)
}
