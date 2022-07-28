package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/_benchmarks/reporters"
	_ "github.com/go-bond/bond/_benchmarks/suites"
)

func main() {
	report := flag.String("report", "stdout", "--report=stdout")
	flag.Parse()

	var allResults []bench.BenchmarkResult

	fmt.Println("=> Bond Benchmarks")

	for _, suite := range bench.BenchmarkSuites() {
		if !*suite.SkipFlag {
			fmt.Printf("==> Run %s\n", suite.Name)
			allResults = append(allResults, suite.BenchmarkFunc(suite)...)
		}
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
