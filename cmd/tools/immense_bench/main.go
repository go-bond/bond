package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-bond/bond"
	"github.com/urfave/cli/v2"
)

type UniqueRand struct {
	generated map[uint64]bool //keeps track of
	rng       *rand.Rand      //underlying random number generator
	ids       []uint64
}

// Generating unique rand
func NewUniqueRand(space int) *UniqueRand {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return &UniqueRand{
		generated: make(map[uint64]bool, space),
		rng:       r1,
		ids:       make([]uint64, 0, space),
	}
}

func (u *UniqueRand) Int() uint64 {
	for {
		i := u.rng.Uint64()
		if !u.generated[i] {
			u.generated[i] = true
			u.ids = append(u.ids, i)
			return i
		}
	}
}

type TokenBalance struct {
	ID              uint64 `json:"id"`
	AccountID       uint32 `json:"accountId"`
	ContractAddress string `json:"contractAddress"`
	AccountAddress  string `json:"accountAddress"`
	TokenID         uint32 `json:"tokenId"`
	Balance         uint64 `json:"balance"`
}

func getTable(tableID int, db bond.DB) bond.Table[*TokenBalance] {
	TokenBalanceTableID := bond.TableID(tableID)

	table := bond.NewTable(bond.TableOptions[*TokenBalance]{
		DB:        db,
		TableName: fmt.Sprintf("token_balance_%d", tableID),
		TableID:   TokenBalanceTableID,
		TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		},
	})

	accountIdx := bond.NewIndex(bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 1,
		IndexName: "account_address_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddStringField(tb.AccountAddress).Bytes()
		},
		IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
	})

	amountIdx := bond.NewIndex(bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 2,
		IndexName: "account_amount_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, t *TokenBalance) []byte {
			return builder.Bytes()
		},
		IndexOrderFunc: func(o bond.IndexOrder, t *TokenBalance) bond.IndexOrder {
			return o.OrderUint64(t.Balance, bond.IndexOrderTypeDESC)
		},
	})

	tokenIdx := bond.NewIndex(bond.IndexOptions[*TokenBalance]{
		IndexID:   bond.PrimaryIndexID + 3,
		IndexName: "token_idx",
		IndexKeyFunc: func(builder bond.KeyBuilder, t *TokenBalance) []byte {
			return builder.Bytes()
		},
		IndexOrderFunc: func(o bond.IndexOrder, t *TokenBalance) bond.IndexOrder {
			return o.OrderUint32(t.TokenID, bond.IndexOrderTypeDESC)
		},
	})

	contractIdx := bond.NewIndex(bond.IndexOptions[*TokenBalance]{
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
	return table
}

var insertedEntries uint64

var readEntries uint64

// Insert records to the bond db
// Number of entires = batchSize * totalBatch
func insertRecords(db bond.DB, tableID, batchSize, totalBatch int, idGenerator *UniqueRand, wg *sync.WaitGroup) {
	table := getTable(tableID, db)
	entries := make([]*TokenBalance, 0, batchSize)
	for i := 0; i < totalBatch; i++ {
		for j := 0; j < batchSize; j++ {
			id := idGenerator.Int()
			entries = append(entries, &TokenBalance{
				ID:              id,
				AccountID:       uint32(id % 10),
				ContractAddress: RandStringRunes(20),
				AccountAddress:  RandStringRunes(20),
				Balance:         uint64((id % 100) * 10),
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

func readRecords(db bond.DB, tableID int, idGenerator *UniqueRand, wg *sync.WaitGroup) {
	table := getTable(tableID, db)
	for _, id := range idGenerator.ids {
		token := &TokenBalance{
			ID: id,
		}
		_, err := table.Get(token)
		if err != nil {
			panic(err.Error())
		}
		atomic.AddUint64(&readEntries, 1)
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
				Value: 20,
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
			&cli.BoolFlag{
				Name:  "read",
				Value: false,
				Usage: "run bondRead",
			},
			&cli.BoolFlag{
				Name:  "pprof",
				Value: false,
				Usage: "run pprof",
			},
			&cli.IntFlag{
				Name:  "n",
				Value: 1,
				Usage: "number of iteration",
			},
		},
		Action: func(cCtx *cli.Context) error {

			if cCtx.Bool("pprof") {
				var (
					cpuProfile *os.File
					memProfile *os.File
					err        error
				)

				cpuProfile, err = os.Create("cpuprofile")
				if err != nil {
					panic(err)
				}
				memProfile, err = os.Create("memprofile")
				if err != nil {
					panic(err)
				}
				fmt.Println("Profiling started")
				pprof.StartCPUProfile(cpuProfile)

				defer pprof.StopCPUProfile()
				defer pprof.WriteHeapProfile(memProfile)
			}

			timeTaken := map[string][]time.Duration{"insert": []time.Duration{}, "read": []time.Duration{}}

			for i := 0; i < cCtx.Int("n"); i++ {
				db, err := bond.Open("example", &bond.Options{})
				if err != nil {
					panic(err)
				}
				close := func() {
					db.Close()
					sz, _ := DirSize("example")
					fmt.Printf("size of database %s \n", humanize.Bytes(sz))
					_ = os.RemoveAll("example")
				}

				fmt.Println("Insert Started")
				generators, insertTime := runBondInsert(db, cCtx.Int("total_table"),
					cCtx.Int("total_batch"),
					cCtx.Int("batch_size"))

				fmt.Printf("Total time taken to insert %s \n", insertTime)
				timeTaken["insert"] = append(timeTaken["insert"], insertTime)

				if !cCtx.Bool("read") {
					close()
					continue
				}
				fmt.Println("Read started")
				readTime := runBondRead(db, generators)

				fmt.Printf("Total time taken to insert %s \n", insertTime)
				fmt.Printf("Total time taken to read %s \n", readTime)
				timeTaken["read"] = append(timeTaken["read"], readTime)
				close()
			}

			fmt.Printf("Results: \n\n")
			for _, insertTime := range timeTaken["insert"] {
				fmt.Printf("Total time taken to insert %s \n", insertTime)
			}
			for _, readTime := range timeTaken["read"] {
				fmt.Printf("Total time taken to read %s \n", readTime)
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func runBondInsert(db bond.DB, totalTable, totalBatch, batchSize int) ([]*UniqueRand, time.Duration) {
	wg := &sync.WaitGroup{}
	start := time.Now()
	generators := []*UniqueRand{}

	for i := 0; i < totalTable; i++ {
		wg.Add(1)
		gen := NewUniqueRand(batchSize * totalBatch)
		generators = append(generators, gen)
		go insertRecords(db, i, batchSize, totalBatch, gen, wg)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			fmt.Printf("inserted records %d \n", atomic.LoadUint64(&insertedEntries))
		}
	}()

	wg.Wait()
	ticker.Stop()

	elapsed := time.Since(start)
	return generators, elapsed
}

func runBondRead(db bond.DB, idGenerators []*UniqueRand) time.Duration {
	start := time.Now()
	wg := &sync.WaitGroup{}

	for i := 0; i < len(idGenerators); i++ {
		wg.Add(1)
		go func(idx int) {
			readRecords(db, idx, idGenerators[idx], wg)
		}(i)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			fmt.Printf("read records %d \n", atomic.LoadUint64(&readEntries))
		}
	}()

	wg.Wait()
	ticker.Stop()

	elapsed := time.Since(start)
	return elapsed
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
