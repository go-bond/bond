package suites

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/serializers"
)

func init() {
	bench.RegisterBenchmarkSuite(
		bench.NewBenchmarkSuite("BenchmarkTableExistSuite", "skip-table-exist",
			BenchmarkTableExistSuite),
	)
}

func BenchmarkTableExistSuite(bs *bench.BenchmarkSuite) []bench.BenchmarkResult {
	var serializers = []struct {
		Name       string
		Serializer bond.Serializer[any]
	}{
		//{"JSONSerializer", &serializers.JsonSerializer{}},
		{"CBORSerializer", &serializers.CBORSerializer{}},
	}

	var results []bench.BenchmarkResult
	for _, serializer := range serializers {
		db := setupDatabase(serializer.Serializer)

		const (
			TokenBalanceTableID = bond.TableID(1)
		)

		tokenBalanceTable := bond.NewTable[*TokenBalance](bond.TableOptions[*TokenBalance]{
			DB:        db,
			TableName: "token_balance",
			TableID:   TokenBalanceTableID,
			TablePrimaryKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
				return builder.AddUint64Field(tb.ID).Bytes()
			},
		})

		const (
			_                                 = bond.PrimaryIndexID
			TokenBalanceAccountAddressIndexID = iota
		)

		var (
			TokenBalanceAccountAddressIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
				IndexID:   TokenBalanceAccountAddressIndexID,
				IndexName: "account_address_idx",
				IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.AddStringField(tb.AccountAddress).Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
			})
		)

		err := tokenBalanceTable.AddIndex([]*bond.Index[*TokenBalance]{
			TokenBalanceAccountAddressIndex,
		})
		if err != nil {
			panic(err)
		}

		var tokenBalances []*TokenBalance
		for i := 0; i < 2; i++ {
			tokenBalances = append(tokenBalances, &TokenBalance{
				ID:              uint64(i + 1),
				AccountID:       uint32(i % 10),
				ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
				AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5),
				Balance:         uint64((i % 100) * 10),
			})
		}

		err = tokenBalanceTable.Insert(context.Background(), tokenBalances[:1])
		if err != nil {
			panic(err)
		}

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Exist_True", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						tokenBalanceTable.Exist(tokenBalances[0])
					}
				},
			}),
		)

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Exist_False", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						tokenBalanceTable.Exist(tokenBalances[1])
					}
				},
			}),
		)

		tearDownDatabase(db)
	}

	return results
}
