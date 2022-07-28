package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/_benchmarks/reporters"
	"github.com/go-bond/bond/_benchmarks/suites"
)

func main() {
	skipTableInsert := flag.Bool("skip-table-insert", false, "")
	skipTableScan := flag.Bool("skip-table-scan", false, "")
	skipTableQuery := flag.Bool("skip-table-query", false, "")
	report := flag.String("report", "stdout", "")
	flag.Parse()

	var allResults []bench.BenchmarkResult

	fmt.Println("=> Bond Benchmarks")

	if !*skipTableInsert {
		fmt.Println("==> Run BenchmarkTableInsertSuite")
	}

	if !*skipTableScan {
		fmt.Println("==> Run BenchmarkTableScanSuite")
		allResults = append(allResults, suites.BenchmarkTableScanSuite()...)
	}

	if !*skipTableQuery {
		fmt.Println("==> Run BenchmarkTableQuerySuite")
		allResults = append(allResults, suites.BenchmarkTableQuerySuite()...)
	}

	var err error
	switch *report {
	case "stdout":
		err = reporters.NewIOReporter(os.Stdout).Report(allResults)
	default:
		err = reporters.NewIOReporter(os.Stdout).Report(allResults)
	}

	if err != nil {
		fmt.Printf("=> Failed to generate report\n")
	}
}
