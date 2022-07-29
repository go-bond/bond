package bench

import (
	"flag"
	"fmt"
	"testing"
	"time"
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
	testing.BenchmarkResult
}

func (br *BenchmarkResult) OpsPerSec() float64 {
	return br.Extra["ops/s"]
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
		fmt.Printf("===> %s\n", benchmark.Name)

		result := BenchmarkResult{
			Benchmark:       benchmark,
			BenchmarkResult: testing.Benchmark(benchmark.BenchmarkFunc),
		}

		if result.NumberOfOperations != 0 {
			result.Extra["ops/s"] = float64(time.Second) / float64(result.NsPerOp()/int64(result.NumberOfOperations))
		}

		return result
	}
}
