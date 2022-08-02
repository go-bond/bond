package suites

import (
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	bench.RegisterBenchmarkSuite(
		bench.NewBenchmarkSuite("BenchmarkTableUpdateSuite", "skip-table-update",
			BenchmarkTableUpdateSuite),
	)
}

func BenchmarkTableUpdateSuite(bs *bench.BenchmarkSuite) []bench.BenchmarkResult {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	var serializers = []struct {
		Name       string
		Serializer bond.Serializer
	}{
		//{"JSONSerializer", &bond.JsonSerializer{}},
		{"MsgPackSerializer", &bond.MsgPackSerializer{}},
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
		for i := 0; i < 1000000; i++ {
			tokenBalances = append(tokenBalances, &TokenBalance{
				ID:              uint64(i + 1),
				AccountID:       uint32(i % 10),
				ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
				AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5),
				Balance:         uint64((i % 100) * 10),
			})
		}

		err = tokenBalanceTable.Insert(tokenBalances)
		if err != nil {
			panic(err)
		}

		var updateBatches = []struct {
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

		for _, v := range updateBatches {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name:          fmt.Sprintf("%s/%s/Update_%d", bs.Name, serializer.Name, v.batchSize),
					Inputs:        v,
					BenchmarkFunc: UpdateInBatchSize(tokenBalanceTable, tokenBalances, v.batchSize),
				}),
			)
		}

		tearDownDatabase(db)
	}

	return results
}

func UpdateInBatchSize(tbt *bond.Table[*TokenBalance], tbs []*TokenBalance, updateBatchSize int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := tbt.Update(tbs[:updateBatchSize])
			if err != nil {
				panic(err)
			}
		}
	}
}
