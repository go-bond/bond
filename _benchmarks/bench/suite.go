package bench

import (
	"flag"
	"testing"
)

var registeredSuites []*BenchmarkSuite

func BenchmarkSuites() []*BenchmarkSuite {
	return registeredSuites
}

func RegisterBenchmarkSuite(bs *BenchmarkSuite) {
	registeredSuites = append(registeredSuites, bs)
}

type Benchmark struct {
	Name               string
	Inputs             any
	NumberOfOperations int
	BenchmarkFunc      func(b *testing.B)
}

type BenchmarkResult struct {
	Benchmark
	Result testing.BenchmarkResult
}

type BenchmarkSuite struct {
	Name          string
	SkipFlag      *bool
	BenchmarkFunc func(bs *BenchmarkSuite) []BenchmarkResult
	Runner        *testing.B
}

func NewBenchmarkSuite(name string, skipFlag string, benchSuiteFunc func(bs *BenchmarkSuite) []BenchmarkResult) *BenchmarkSuite {
	return &BenchmarkSuite{
		Name:          name,
		SkipFlag:      flag.Bool(skipFlag, false, ""),
		BenchmarkFunc: benchSuiteFunc,
	}
}

func (bs *BenchmarkSuite) Benchmark(benchmark Benchmark) BenchmarkResult {
	if bs.Runner != nil {
		bs.Runner.Run(benchmark.Name, benchmark.BenchmarkFunc)
		return BenchmarkResult{}
	} else {
		return BenchmarkResult{
			Benchmark: benchmark,
			Result:    testing.Benchmark(benchmark.BenchmarkFunc),
		}
	}
}
