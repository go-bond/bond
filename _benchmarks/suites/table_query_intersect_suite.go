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
		bench.NewBenchmarkSuite("BenchmarkTableQueryIntersectSuite", "skip-table-query-intersect",
			BenchmarkTableQueryIntersectSuite),
	)
}

func BenchmarkTableQueryIntersectSuite(bs *bench.BenchmarkSuite) []bench.BenchmarkResult {
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
			_                                                 = bond.PrimaryIndexID
			TokenBalanceAccountAddressIndexID                 = iota
			TokenBalanceContractAddressIndexID                = iota
			TokenBalanceAccountAddressOrderBalanceDESCIndexID = iota
			TokenBalanceAccountAndContractAddressIndexID      = iota
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
			TokenBalanceContractAddressIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
				IndexID:   TokenBalanceContractAddressIndexID,
				IndexName: "contract_address_idx",
				IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.AddStringField(tb.ContractAddress).Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
			})
			TokenBalanceAccountAddressOrderBalanceDESCIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
				IndexID:   TokenBalanceAccountAddressOrderBalanceDESCIndexID,
				IndexName: "account_address_ord_balance_desc_idx",
				IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.AddStringField(tb.AccountAddress).Bytes()
				},
				IndexOrderFunc: func(o bond.IndexOrder, tb *TokenBalance) bond.IndexOrder {
					return o.OrderUint64(tb.Balance, bond.IndexOrderTypeDESC)
				},
			})
			TokenBalanceAccountAndContractAddressIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
				IndexID:   TokenBalanceAccountAndContractAddressIndexID,
				IndexName: "account_address_contract_address_idx",
				IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.
						AddStringField(tb.AccountAddress).
						AddStringField(tb.ContractAddress).
						Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
			})
		)

		err := tokenBalanceTable.AddIndex([]*bond.Index[*TokenBalance]{
			TokenBalanceAccountAddressIndex,
			TokenBalanceContractAddressIndex,
			TokenBalanceAccountAddressOrderBalanceDESCIndex,
			TokenBalanceAccountAndContractAddressIndex,
		})
		if err != nil {
			panic(err)
		}

		var tokenBalances []*TokenBalance
		for i := 0; i < 10000000; i++ {
			tokenBalances = append(tokenBalances, &TokenBalance{
				ID:              uint64(i + 1),
				AccountID:       0,
				ContractAddress: fmt.Sprintf("0xtestContract%d", i%10),
				AccountAddress:  fmt.Sprintf("0xtestAccount%d", i%1000),
				TokenID:         uint32(i),
				Balance:         uint64((i % 100) * 10),
			})
		}

		err = tokenBalanceTable.Insert(context.Background(), tokenBalances[0:10000000])
		if err != nil {
			panic(err)
		}

		tokenBalances = nil

		var queryInputs = []struct {
			index     *bond.Index[*TokenBalance]
			index2    *bond.Index[*TokenBalance]
			indexName string
			selector  *TokenBalance
			selector2 *TokenBalance
			filter    func(tb *TokenBalance) bool
			order     func(tb *TokenBalance, tb2 *TokenBalance) bool
			offset    int
			limit     int
		}{
			// AccountAddress Index
			{index: TokenBalanceAccountAndContractAddressIndex, indexName: "AccountAndContractAddress", selector: &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0"}, offset: 0, limit: 0},
			{index: TokenBalanceAccountAndContractAddressIndex, indexName: "AccountAndContractAddress", selector: &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0"}, offset: 0, limit: 500},
			{index: TokenBalanceAccountAndContractAddressIndex, indexName: "AccountAndContractAddress", selector: &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0"}, offset: 0, limit: 1000},
			{index: TokenBalanceAccountAndContractAddressIndex, indexName: "AccountAndContractAddress", selector: &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0"}, offset: 0, limit: 5000},
			{index: TokenBalanceAccountAndContractAddressIndex, indexName: "AccountAndContractAddress", selector: &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0"}, offset: 0, limit: 10000},
		}

		for _, v := range queryInputs {
			var selectorID = uint64(0)
			if v.selector != nil {
				selectorID = v.selector.ID
			}

			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name: fmt.Sprintf("%s/%s/Query_Index_No_Intersect_%s_Sel_%d_Offset_%d_Limit_%d",
						bs.Name, serializer.Name, v.indexName, selectorID, v.offset, v.limit),
					Inputs:        v,
					BenchmarkFunc: QueryIntersectWithOpts(tokenBalanceTable, v.index, v.selector, v.index2, v.selector2, v.offset, v.limit),
				}),
			)
		}

		selector := &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0"}

		var queryInputs2 = []struct {
			index     *bond.Index[*TokenBalance]
			index2    *bond.Index[*TokenBalance]
			indexName string
			selector  *TokenBalance
			selector2 *TokenBalance
			filter    func(tb *TokenBalance) bool
			order     func(tb *TokenBalance, tb2 *TokenBalance) bool
			offset    int
			limit     int
		}{
			// AccountAddress Index
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressIndex, indexName: "AccountAddress_And_ContractAddress", selector: selector, selector2: selector, offset: 0, limit: 0},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressIndex, indexName: "AccountAddress_And_ContractAddress", selector: selector, selector2: selector, offset: 0, limit: 500},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressIndex, indexName: "AccountAddress_And_ContractAddress", selector: selector, selector2: selector, offset: 0, limit: 1000},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressIndex, indexName: "AccountAddress_And_ContractAddress", selector: selector, selector2: selector, offset: 0, limit: 5000},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressIndex, indexName: "AccountAddress_And_ContractAddress", selector: selector, selector2: selector, offset: 0, limit: 10000},
		}

		for _, v := range queryInputs2 {
			var selectorID = uint64(0)
			if v.selector != nil {
				selectorID = v.selector.ID
			}

			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name: fmt.Sprintf("%s/%s/Query_Index_Intersect_%s_Sel_%d_Offset_%d_Limit_%d",
						bs.Name, serializer.Name, v.indexName, selectorID, v.offset, v.limit),
					Inputs:        v,
					BenchmarkFunc: QueryIntersectWithOpts(tokenBalanceTable, v.index, v.selector, v.index2, v.selector2, v.offset, v.limit),
				}),
			)
		}

		tearDownDatabase(db)
	}

	return results
}

func QueryIntersectWithOpts(tbt bond.Table[*TokenBalance], idx *bond.Index[*TokenBalance], sel *TokenBalance, idx2 *bond.Index[*TokenBalance], sel2 *TokenBalance, offset int, limit int) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance

			q := tbt.Query()
			if idx != nil && sel != nil {
				q = q.With(idx, sel)
			}

			if idx2 != nil && sel2 != nil {
				q = q.Intersects(tbt.Query().With(idx2, sel2))
			}

			if offset != 0 {
				q = q.Offset(uint64(offset))
			}

			if limit != 0 {
				q = q.Limit(uint64(limit))
			}

			err := q.Execute(context.Background(), &tokenBalances)
			if err != nil {
				panic(err)
			}

			if len(tokenBalances) == 0 {
				panic("no results")
			}
		}
	}
}
