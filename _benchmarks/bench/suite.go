package bench

import (
	"flag"
	"testing"
)

var registeredSuites []*BenchmarkSuite

type BenchmarkResult struct {
	Name               string
	Inputs             any
	NumberOfOperations int
	Result             testing.BenchmarkResult
}

type BenchmarkSuite struct {
	Name          string
	SkipFlag      *bool
	BenchmarkFunc func(bs *BenchmarkSuite) []BenchmarkResult
}

func NewBenchmarkSuite(name string, skipFlag string, benchSuiteFunc func(bs *BenchmarkSuite) []BenchmarkResult) *BenchmarkSuite {
	return &BenchmarkSuite{
		Name:          name,
		SkipFlag:      flag.Bool(skipFlag, false, ""),
		BenchmarkFunc: benchSuiteFunc,
	}
}

func BenchmarkSuites() []*BenchmarkSuite {
	return registeredSuites
}

func RegisterBenchmarkSuite(bs *BenchmarkSuite) {
	registeredSuites = append(registeredSuites, bs)
}
