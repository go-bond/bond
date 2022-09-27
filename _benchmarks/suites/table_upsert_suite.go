package suites

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	bench.RegisterBenchmarkSuite(
		bench.NewBenchmarkSuite("BenchmarkTableUpsertSuite", "skip-table-upsert",
			BenchmarkTableUpsertSuite),
	)
}

func BenchmarkTableUpsertSuite(bs *bench.BenchmarkSuite) []bench.BenchmarkResult {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	var serializers = []struct {
		Name       string
		Serializer bond.Serializer[any]
	}{
		//{"JSONSerializer", &bond.JsonSerializer{}},
		{"MsgpackSerializer", &bond.MsgpackSerializer{}},
	}

	var results []bench.BenchmarkResult
	for _, serializer := range serializers {
		db := setupDatabase(serializer.Serializer)

		const (
			TokenBalanceTableID = bond.TableID(1)
		)

		tokenBalanceTable := bond.NewTable[*TokenBalance](db, TokenBalanceTableID, func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
			return builder.AddUint64Field(tb.ID).Bytes()
		})

		const (
			_                                 = bond.PrimaryIndexID
			TokenBalanceAccountAddressIndexID = iota
		)

		var (
			TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](
				TokenBalanceAccountAddressIndexID,
				func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.AddStringField(tb.AccountAddress).Bytes()
				},
				bond.IndexOrderDefault[*TokenBalance],
			)
		)

		err := tokenBalanceTable.AddIndex([]*bond.Index[*TokenBalance]{
			TokenBalanceAccountAddressIndex,
		})
		if err != nil {
			panic(err)
		}

		var tokenBalances []*TokenBalance
		for i := 0; i < 10000000; i++ {
			tokenBalances = append(tokenBalances, &TokenBalance{
				ID:              uint64(i + 1),
				AccountID:       uint32(i % 10),
				ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
				AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5),
				Balance:         uint64((i % 100) * 10),
			})
		}

		var upsertBatches = []struct {
			batchSize int
		}{
			{batchSize: 1},
			{batchSize: 10},
			{batchSize: 100},
			{batchSize: 1000},
			{batchSize: 10000},
			{batchSize: 100000},
			{batchSize: 1000000},
		}

		for _, v := range upsertBatches {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name:          fmt.Sprintf("%s/%s/Upsert_%d", bs.Name, serializer.Name, v.batchSize),
					Inputs:        v,
					BenchmarkFunc: UpsertInBatchSize(tokenBalanceTable, tokenBalances, v.batchSize),
				}),
			)
		}

		for _, v := range upsertBatches {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name:          fmt.Sprintf("%s/%s/UpsertWithBatch_%d", bs.Name, serializer.Name, v.batchSize),
					Inputs:        v,
					BenchmarkFunc: UpsertInBatchSizeWithBatch(db, tokenBalanceTable, tokenBalances, v.batchSize),
				}),
			)
		}

		tearDownDatabase(db)
	}

	return results
}

func UpsertInBatchSize(tbt *bond.Table[*TokenBalance], tbs []*TokenBalance, insertBatchSize int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := tbt.Upsert(context.Background(), tbs[:insertBatchSize], bond.TableUpsertOnConflictReplace[*TokenBalance])
			if err != nil {
				panic(err)
			}
		}
	}
}

func UpsertInBatchSizeWithBatch(db *bond.DB, tbt *bond.Table[*TokenBalance], tbs []*TokenBalance, insertBatchSize int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		batch := db.NewIndexedBatch()
		for i := 0; i < b.N; i++ {
			err := tbt.Upsert(context.Background(), tbs[:insertBatchSize], bond.TableUpsertOnConflictReplace[*TokenBalance], batch)
			if err != nil {
				panic(err)
			}
		}
		_ = batch.Commit(pebble.Sync)
	}
}
