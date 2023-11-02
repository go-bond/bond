package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-bond/bond/_benchmarks/bench"
	"github.com/go-bond/bond/_benchmarks/reporters"
	_ "github.com/go-bond/bond/_benchmarks/suites"
	"golang.org/x/exp/slices"
)

const AllTestSuites = ""

func RunBenchmarks(b *testing.B, testSuiteNames []string) []bench.BenchmarkResult {
	var allResults []bench.BenchmarkResult

	for _, suite := range bench.BenchmarkSuites() {
		if b != nil {
			suite.Runner = b
		}

		if len(testSuiteNames) != 0 && !slices.Contains(testSuiteNames, suite.Name) {
			continue
		}

		if !*suite.SkipFlag {
			fmt.Printf("==> Run %s\n", suite.Name)
			allResults = append(allResults, suite.BenchmarkFunc(suite)...)
		}
	}

	return allResults
}

type SliceFlag []string

func (i *SliceFlag) String() string {
	return strings.Join(*i, ",")
}

func (i *SliceFlag) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	report := flag.String("report", "stdout", "--report=stdout")
	var testSuites SliceFlag
	flag.Var(&testSuites, "test-suite", "--test-suite=name")
	flag.Parse()

	fmt.Println("=> Bond Benchmarks")
	allResults := RunBenchmarks(nil, testSuites)

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
