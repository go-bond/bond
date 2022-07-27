package bench

import "testing"

type BenchmarkResult struct {
	Name       string
	Normalizer int
	Result     testing.BenchmarkResult
}
