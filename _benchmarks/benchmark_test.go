package main

import "testing"

func BenchmarkSuites(b *testing.B) {
	RunBenchmarks(b, AllTestSuites)
}
