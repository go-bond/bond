package main

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/_benchmarks/reporters"
	_ "github.com/go-bond/bond/_benchmarks/suites"
)

func RunBenchmarks(b *testing.B) []bench.BenchmarkResult {
	var allResults []bench.BenchmarkResult

	for _, suite := range bench.BenchmarkSuites() {
		if b != nil {
			suite.Runner = b
		}

		if !*suite.SkipFlag {
			fmt.Printf("==> Run %s\n", suite.Name)
			allResults = append(allResults, suite.BenchmarkFunc(suite)...)
		}
	}

	return allResults
}

func main() {
	report := flag.String("report", "stdout", "--report=stdout")
	flag.Parse()

	fmt.Println("=> Bond Benchmarks")
	allResults := RunBenchmarks(nil)

	var err error
	switch *report {
	case "stdout":
		err = reporters.NewIOReporter(os.Stdout).Report(allResults)
	case "csv":
		var file *os.File
		file, err = os.Create(fmt.Sprintf("benchmark_%s.csv", time.Now().Format("2006_01_02_15_04")))
		if err != nil {
			break
		}

		err = reporters.NewCSVReporter(file).Report(allResults)
		if err != nil {
			break
		}

		err = file.Close()
	default:
		err = reporters.NewIOReporter(os.Stdout).Report(allResults)
	}

	if err != nil {
		fmt.Printf("=> Failed to generate report: %s\n", err.Error())
	}
}
