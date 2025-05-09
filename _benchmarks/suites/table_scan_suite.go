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
		bench.NewBenchmarkSuite("BenchmarkTableScanSuite", "skip-table-scan",
			BenchmarkTableScanSuite),
	)
}

func BenchmarkTableScanSuite(bs *bench.BenchmarkSuite) []bench.BenchmarkResult {
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
		for i := 0; i < 4000000; i++ {
			tokenBalances = append(tokenBalances, &TokenBalance{
				ID:              uint64(i + 1),
				AccountID:       uint32(i % 10),
				ContractAddress: "0xtestContract" + fmt.Sprintf("%d", i),
				AccountAddress:  "0xtestAccount" + fmt.Sprintf("%d", i%2),
				Balance:         uint64((i % 100) * 10),
			})
		}

		var tokenBalancesAccount0 []*TokenBalance
		for _, tokenBalance := range tokenBalances {
			if tokenBalance.AccountAddress == "0xtestAccount0" {
				tokenBalancesAccount0 = append(tokenBalancesAccount0, tokenBalance)
			}
		}

		var scanSizes = []struct {
			scanSize int
		}{
			{scanSize: 1},
			{scanSize: 10},
			{scanSize: 1000},
			{scanSize: 10000},
			{scanSize: 100000},
			{scanSize: 1000000},
		}

		for _, v := range scanSizes {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name:          fmt.Sprintf("%s/%s/Scan_%d", bs.Name, serializer.Name, v.scanSize),
					Inputs:        v,
					BenchmarkFunc: ScanElements(tokenBalanceTable, tokenBalances, v.scanSize),
				}),
			)
		}

		for _, v := range scanSizes {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name:   fmt.Sprintf("%s/%s/ScanIndex_%d", bs.Name, serializer.Name, v.scanSize),
					Inputs: v,
					BenchmarkFunc: ScanIndexElements(tokenBalanceTable, TokenBalanceAccountAddressIndex,
						bond.NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount0"}), tokenBalancesAccount0, v.scanSize),
				}),
			)
		}

		var skipReadSizes = []struct {
			skipNumber int
			readNumber int
		}{
			{skipNumber: 0, readNumber: 0},
			{skipNumber: 1000, readNumber: 0},
			{skipNumber: 10000, readNumber: 0},
			{skipNumber: 100000, readNumber: 0},
			{skipNumber: 1000000, readNumber: 0},
			{skipNumber: 0, readNumber: 1000},
			{skipNumber: 1000, readNumber: 1000},
			{skipNumber: 10000, readNumber: 1000},
			{skipNumber: 100000, readNumber: 1000},
			{skipNumber: 1000000, readNumber: 1000},
		}

		err = tokenBalanceTable.Insert(context.Background(), tokenBalances)
		if err != nil {
			panic(err)
		}

		for _, v := range skipReadSizes {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name: fmt.Sprintf("%s/%s/Scan_Skip_%d_Read_%d", bs.Name, serializer.Name,
						v.skipNumber, v.readNumber),
					Inputs: v,
					BenchmarkFunc: ScanSkipThrough(tokenBalanceTable, v.skipNumber,
						v.readNumber),
				}),
			)
		}

		err = tokenBalanceTable.Delete(context.Background(), tokenBalances)
		if err != nil {
			panic(err)
		}

		err = tokenBalanceTable.Insert(context.Background(), tokenBalancesAccount0)
		if err != nil {
			panic(err)
		}

		for _, v := range skipReadSizes {
			results = append(results,
				bs.Benchmark(bench.Benchmark{
					Name: fmt.Sprintf("%s/%s/ScanIndex_Skip_%d_Read_%d", bs.Name, serializer.Name,
						v.skipNumber, v.readNumber),
					Inputs: v,
					BenchmarkFunc: ScanIndexSkipThrough(tokenBalanceTable, TokenBalanceAccountAddressIndex,
						bond.NewSelectorPoint(&TokenBalance{AccountAddress: "0xtestAccount0"}), v.skipNumber, v.readNumber),
				}),
			)
		}

		err = tokenBalanceTable.Delete(context.Background(), tokenBalancesAccount0)
		if err != nil {
			panic(err)
		}

		tearDownDatabase(db)
	}

	return results
}

func ScanElements(tbt bond.Table[*TokenBalance], tbs []*TokenBalance, numberToScan int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.StopTimer()
		err := tbt.Insert(context.Background(), tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance
			err = tbt.Scan(context.Background(), &tokenBalances, false)
			if err != nil {
				panic(err)
			}
		}

		b.StopTimer()
		err = tbt.Delete(context.Background(), tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}

func ScanIndexElements(tbt bond.Table[*TokenBalance], idx *bond.Index[*TokenBalance], sel bond.Selector[*TokenBalance], tbs []*TokenBalance, numberToScan int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.StopTimer()
		err := tbt.Insert(context.Background(), tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var tokenBalances []*TokenBalance
			err = tbt.ScanIndex(context.Background(), idx, sel, &tokenBalances, false)
		}

		b.StopTimer()
		err = tbt.Delete(context.Background(), tbs[:numberToScan])
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}

func ScanSkipThrough(tbt bond.Table[*TokenBalance], numberToSkip int, numberToRead int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var counter = 0
			var tokenBalances = make([]*TokenBalance, 0, numberToRead)
			err := tbt.ScanForEach(context.Background(), func(_ bond.KeyBytes, l bond.Lazy[*TokenBalance]) (bool, error) {
				counter++
				if counter <= numberToSkip {
					return true, nil
				}

				if counter >= numberToSkip+numberToRead {
					return false, nil
				}

				tb, err := l.Get()
				if err != nil {
					return false, err
				}

				tokenBalances = append(tokenBalances, tb)
				return true, nil
			}, false)
			if err != nil {
				panic(err)
			}
		}
	}
}

func ScanIndexSkipThrough(tbt bond.Table[*TokenBalance], idx *bond.Index[*TokenBalance], sel bond.Selector[*TokenBalance], numberToSkip int, numberToRead int) func(*testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var counter = 0
			var tokenBalances = make([]*TokenBalance, 0, numberToRead)
			err := tbt.ScanIndexForEach(context.Background(), idx, sel, func(_ bond.KeyBytes, l bond.Lazy[*TokenBalance]) (bool, error) {
				counter++
				if counter <= numberToSkip {
					return true, nil
				}

				if counter >= numberToSkip+numberToRead {
					return false, nil
				}

				tb, err := l.Get()
				if err != nil {
					return false, err
				}

				tokenBalances = append(tokenBalances, tb)
				return true, nil
			}, false)
			if err != nil {
				panic(err)
			}
		}
	}
}
