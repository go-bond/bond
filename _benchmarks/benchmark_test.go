package main

import (
	"path/filepath"
	"testing"

	"github.com/go-bond/bond/_benchmarks/compactkeys"
)

func BenchmarkSuites(b *testing.B) {
	RunBenchmarks(b, AllTestSuites)
}

func BenchmarkBondLifecycleBaselines(b *testing.B) {
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		b.Fatal(err)
	}
	compactkeys.RunLifecycleBenchmarks(b, repoRoot)
}
