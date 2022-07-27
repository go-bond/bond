package main

import (
	"fmt"

	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/_benchmarks/suites"
)

func main() {
	var allResults []bench.BenchmarkResult

	fmt.Println("=> Bond Benchmarks")

	fmt.Println("==> Run BenchmarkTableInsertSuite")
	allResults = append(allResults, suites.BenchmarkTableInsertSuite()...)

	fmt.Println("==> Run BenchmarkTableScanSuite")
	allResults = append(allResults, suites.BenchmarkTableScanSuite()...)

	for _, result := range allResults {
		fmt.Printf("%-80s %-30s %-30s\n", result.Name, result.Result.String(), result.Result.MemString())
	}
}
