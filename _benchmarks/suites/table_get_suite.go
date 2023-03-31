package suites

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/serializers"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	bench.RegisterBenchmarkSuite(
		bench.NewBenchmarkSuite("BenchmarkTableGetSuite", "skip-table-get",
			BenchmarkTableGetSuite),
	)
}

func BenchmarkTableGetSuite(bs *bench.BenchmarkSuite) []bench.BenchmarkResult {
	msgpack.GetEncoder().SetCustomStructTag("json")
	msgpack.GetDecoder().SetCustomStructTag("json")

	var serializers = []struct {
		Name       string
		Serializer bond.Serializer[any]
	}{
		//{"JSONSerializer", &serializers.JsonSerializer{}},
		//{"MsgpackSerializer", &serializers.MsgpackSerializer{}},
		//{"MsgpackGenSerializer", &serializers.MsgpackGenSerializer{}},
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
		for i := 0; i < 1000000; i++ {
			tokenBalances = append(tokenBalances, &TokenBalance{
				ID:              uint64(i + 1),
				AccountID:       uint32(i % 10),
				ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
				AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%5),
				Balance:         uint64((i % 100) * 10),
			})
		}

		err = tokenBalanceTable.Insert(context.Background(), tokenBalances)
		if err != nil {
			panic(err)
		}

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Get_Row_Exist", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = tokenBalanceTable.Get(context.Background(), bond.NewSelectorPoint(tokenBalances[0]))
					}
				},
			}),
		)

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Get_Row_Does_Not_Exist", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = tokenBalanceTable.Get(context.Background(), bond.NewSelectorPoint(tokenBalances[1]))
					}
				},
			}),
		)

		var tokenBalancesToGet []*TokenBalance
		for i := 0; i < 1000000; i++ {
			if i%10 == 0 {
				tokenBalancesToGet = append(tokenBalancesToGet, tokenBalances[i])
			}
		}

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Get_Rows_Sequencial_100000", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = tokenBalanceTable.Get(context.Background(), bond.NewSelectorRange(tokenBalances[0], tokenBalances[100000]))
					}
				},
			}),
		)

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Get_Rows_RandomAccess_100000", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						for _, tokenBalance := range tokenBalancesToGet {
							_, _ = tokenBalanceTable.Get(context.Background(), bond.NewSelectorPoint(tokenBalance))
						}
					}
				},
			}),
		)

		results = append(results,
			bs.Benchmark(bench.Benchmark{
				Name:   fmt.Sprintf("%s/%s/Get_Rows_RandomAccess_Selector_Points_100000", bs.Name, serializer.Name),
				Inputs: nil,
				BenchmarkFunc: func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _ = tokenBalanceTable.Get(context.Background(), bond.NewSelectorPoints(tokenBalancesToGet...))
					}
				},
			}),
		)

		tearDownDatabase(db)
	}

	return results
}
