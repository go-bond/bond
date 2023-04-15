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
			_                                                       = bond.PrimaryIndexID
			TokenBalanceAccountAddressIndexID                       = iota
			TokenBalanceContractAddressIndexID                      = iota
			TokenBalanceContractAddressAccountAddressTokenIDIndexID = iota
			TokenBalanceContractAddressTokenIDIndexID               = iota
			TokenBalanceAccountAddressOrderBalanceDESCIndexID       = iota
			TokenBalanceAccountAndContractAddressIndexID            = iota
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
					return builder.AddStringField(tb.ContractAddress).
						AddUint32Field(tb.TokenID).
						Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
			})
			TokenBalanceContractAddressTokenIDIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
				IndexID:   TokenBalanceContractAddressTokenIDIndexID,
				IndexName: "contract_address_token_id_idx",
				IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.AddStringField(tb.ContractAddress).
						AddUint32Field(tb.TokenID).
						Bytes()
				},
				IndexOrderFunc: bond.IndexOrderDefault[*TokenBalance],
			})
			TokenBalanceContractAddressAccountAddressTokenIDIndex = bond.NewIndex[*TokenBalance](bond.IndexOptions[*TokenBalance]{
				IndexID:   TokenBalanceContractAddressAccountAddressTokenIDIndexID,
				IndexName: "contract_address_account_address_token_id_idx",
				IndexKeyFunc: func(builder bond.KeyBuilder, tb *TokenBalance) []byte {
					return builder.AddStringField(tb.ContractAddress).
						AddUint32Field(tb.TokenID).
						Bytes()
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
			TokenBalanceContractAddressTokenIDIndex,
			TokenBalanceContractAddressAccountAddressTokenIDIndex,
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
				ContractAddress: fmt.Sprintf("0xtestContract%d", i%1),
				AccountAddress:  fmt.Sprintf("0xtestAccount%d", i%10000),
				TokenID:         uint32(i % 10),
				Balance:         uint64((i % 100) * 10),
			})
		}

		err = tokenBalanceTable.Insert(context.Background(), tokenBalances[0:10000000])
		if err != nil {
			panic(err)
		}

		tokenBalances = nil

		filter := func(tb *TokenBalance) bool {
			return tb.AccountAddress == "0xtestAccount0" && tb.TokenID == 0
		}

		var queryInputs = []struct {
			index     *bond.Index[*TokenBalance]
			selector  bond.Selector[*TokenBalance]
			indexName string
			filter    func(tb *TokenBalance) bool
			order     func(tb *TokenBalance, tb2 *TokenBalance) bool
			offset    int
			limit     int
		}{
			// AccountAddress Index
			{index: TokenBalanceContractAddressIndex, indexName: "TokenBalanceContractAddressIndex", filter: filter, selector: bond.NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract0"}), offset: 0, limit: 0},
			{index: TokenBalanceContractAddressIndex, indexName: "TokenBalanceContractAddressIndex", filter: filter, selector: bond.NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract0"}), offset: 0, limit: 500},
			{index: TokenBalanceContractAddressIndex, indexName: "TokenBalanceContractAddressIndex", filter: filter, selector: bond.NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract0"}), offset: 0, limit: 1000},
			{index: TokenBalanceContractAddressIndex, indexName: "TokenBalanceContractAddressIndex", filter: filter, selector: bond.NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract0"}), offset: 0, limit: 5000},
			{index: TokenBalanceContractAddressIndex, indexName: "TokenBalanceContractAddressIndex", filter: filter, selector: bond.NewSelectorPoint(&TokenBalance{ContractAddress: "0xtestContract0"}), offset: 0, limit: 10000},
		}

		for _, v := range queryInputs {
			var selectorID = uint64(0)
			if v.selector != nil {
				selectorID = v.selector.(bond.SelectorPoint[*TokenBalance]).Point().ID
			}

			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name: fmt.Sprintf("%s/%s/Query_Index_Filter_%s_Sel_%d_Offset_%d_Limit_%d",
						bs.Name, serializer.Name, v.indexName, selectorID, v.offset, v.limit),
					Inputs:        v,
					BenchmarkFunc: QueryFilterWithOpts(tokenBalanceTable, v.index, v.selector, v.filter, v.offset, v.limit),
				}),
			)
		}

		selector := &TokenBalance{AccountAddress: "0xtestAccount0", ContractAddress: "0xtestContract0", TokenID: 0}

		var queryInputs2 = []struct {
			index     *bond.Index[*TokenBalance]
			index2    *bond.Index[*TokenBalance]
			indexName string
			selector  bond.Selector[*TokenBalance]
			selector2 bond.Selector[*TokenBalance]
			filter    func(tb *TokenBalance) bool
			order     func(tb *TokenBalance, tb2 *TokenBalance) bool
			offset    int
			limit     int
		}{
			// AccountAddress Index
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressTokenIDIndex, indexName: "AccountAddress_And_ContractAddress_TokenID", selector: bond.NewSelectorPoint(selector), selector2: bond.NewSelectorPoint(selector), offset: 0, limit: 0},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressTokenIDIndex, indexName: "AccountAddress_And_ContractAddress_TokenID", selector: bond.NewSelectorPoint(selector), selector2: bond.NewSelectorPoint(selector), offset: 0, limit: 500},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressTokenIDIndex, indexName: "AccountAddress_And_ContractAddress_TokenID", selector: bond.NewSelectorPoint(selector), selector2: bond.NewSelectorPoint(selector), offset: 0, limit: 1000},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressTokenIDIndex, indexName: "AccountAddress_And_ContractAddress_TokenID", selector: bond.NewSelectorPoint(selector), selector2: bond.NewSelectorPoint(selector), offset: 0, limit: 5000},
			{index: TokenBalanceAccountAddressIndex, index2: TokenBalanceContractAddressTokenIDIndex, indexName: "AccountAddress_And_ContractAddress_TokenID", selector: bond.NewSelectorPoint(selector), selector2: bond.NewSelectorPoint(selector), offset: 0, limit: 10000},
		}

		for _, v := range queryInputs2 {
			var selectorID = uint64(0)
			if v.selector != nil {
				selectorID = v.selector.(bond.SelectorPoint[*TokenBalance]).Point().ID
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

		var queryInputs3 = []struct {
			index     *bond.Index[*TokenBalance]
			index2    *bond.Index[*TokenBalance]
			indexName string
			selector  bond.Selector[*TokenBalance]
			selector2 bond.Selector[*TokenBalance]
			filter    func(tb *TokenBalance) bool
			order     func(tb *TokenBalance, tb2 *TokenBalance) bool
			offset    int
			limit     int
		}{
			// AccountAddress Index
			{index: TokenBalanceContractAddressAccountAddressTokenIDIndex, indexName: "TokenBalanceContractAddressAccountAddressTokenID", selector: bond.NewSelectorPoint(selector), offset: 0, limit: 0},
			{index: TokenBalanceContractAddressAccountAddressTokenIDIndex, indexName: "TokenBalanceContractAddressAccountAddressTokenID", selector: bond.NewSelectorPoint(selector), offset: 0, limit: 500},
			{index: TokenBalanceContractAddressAccountAddressTokenIDIndex, indexName: "TokenBalanceContractAddressAccountAddressTokenID", selector: bond.NewSelectorPoint(selector), offset: 0, limit: 1000},
			{index: TokenBalanceContractAddressAccountAddressTokenIDIndex, indexName: "TokenBalanceContractAddressAccountAddressTokenID", selector: bond.NewSelectorPoint(selector), offset: 0, limit: 5000},
			{index: TokenBalanceContractAddressAccountAddressTokenIDIndex, indexName: "TokenBalanceContractAddressAccountAddressTokenID", selector: bond.NewSelectorPoint(selector), offset: 0, limit: 10000},
		}

		for _, v := range queryInputs3 {
			var selectorID = uint64(0)
			if v.selector != nil {
				selectorID = v.selector.(bond.SelectorPoint[*TokenBalance]).Point().ID
			}

			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name: fmt.Sprintf("%s/%s/Query_Index_Direct_%s_Sel_%d_Offset_%d_Limit_%d",
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

func QueryIntersectWithOpts(tbt bond.Table[*TokenBalance], idx *bond.Index[*TokenBalance], sel bond.Selector[*TokenBalance], idx2 *bond.Index[*TokenBalance], sel2 bond.Selector[*TokenBalance], offset int, limit int) func(b *testing.B) {
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

func QueryFilterWithOpts(tbt bond.Table[*TokenBalance], idx *bond.Index[*TokenBalance], sel bond.Selector[*TokenBalance], filter func(tb *TokenBalance) bool, offset int, limit int) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance

			q := tbt.Query().Filter(bond.EvaluableFunc(filter))
			if idx != nil && sel != nil {
				q = q.With(idx, sel)
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
				fmt.Printf("failed with patams %d %d\n", offset, limit)
				panic("no results")
			}
		}
	}
}
