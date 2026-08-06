package compactkeys

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-bond/bond"
)

func BaselineCandidates(performance bond.PerformanceProfile) []EngineSpec {
	return []EngineSpec{
		{Name: "compression-legacy-uniform-bloom", Performance: performance, Compression: bond.CompressionLegacy, TableFilter: bond.TableFilterUniformBloom},
		{Name: "compression-balanced-uniform-bloom", Performance: performance, Compression: bond.CompressionBalanced, TableFilter: bond.TableFilterUniformBloom},
		{Name: "compression-good-uniform-bloom", Performance: performance, Compression: bond.CompressionGood, TableFilter: bond.TableFilterUniformBloom},
		{Name: "filter-progressive-bloom", Performance: performance, Compression: bond.DefaultCompressionProfile, TableFilter: bond.TableFilterProgressiveBloom},
		{Name: "filter-progressive-binary-fuse", Performance: performance, Compression: bond.DefaultCompressionProfile, TableFilter: bond.TableFilterProgressiveBinaryFuse},
	}
}

func RunLifecycleBenchmarks(b *testing.B, repoRoot string) {
	b.ReportAllocs()
	dataset, err := Generate(RepresentativeDatasetSpec(20260806, 1_000))
	if err != nil {
		b.Fatal(err)
	}
	for _, candidate := range BaselineCandidates(bond.LowPerformance) {
		b.Run(candidate.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				manifest, err := RunLifecycle(context.Background(), RunRequest{
					RepoRoot: repoRoot,
					RootDir:  filepath.Join(b.TempDir(), "run"),
					Engine:   candidate,
					Run:      RunSpec{Repetitions: b.N, Run: i + 1, BatchSize: 512},
					Dataset:  dataset,
				})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(manifest.Results.AfterCompaction.SST.PhysicalBytes), "sst-bytes/op")
				b.ReportMetric(float64(manifest.Results.AfterCompaction.SST.PropertyFilterBytes), "filter-bytes/op")
				b.ReportMetric(manifest.Results.Filter.FalsePositiveRate, "filter-fp-rate")
				b.ReportMetric(float64(manifest.Results.ProcessCompactionCPUNS), "compact-cpu-ns/op")
			}
		})
	}
}
