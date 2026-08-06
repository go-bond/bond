package compactkeys

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/internal/fullkeyexperiment"
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

// FullKeyCandidates compares the frozen Phase 2 incumbent against the three
// immutable Phase 3 complete-key schema bundle sizes. No policy dimension
// other than the active physical writer differs between these candidates.
func FullKeyCandidates(performance bond.PerformanceProfile) []EngineSpec {
	return []EngineSpec{
		{
			Name: "legacy-schema-b16", Performance: performance,
			Compression: bond.DefaultCompressionProfile, TableFilter: bond.DefaultTableFilterProfile,
			BundleSize: 16,
		},
		{
			Name: "full-key-schema-b16", Performance: performance,
			Compression: bond.DefaultCompressionProfile, TableFilter: bond.DefaultTableFilterProfile,
			WriterSchema: fullkeyexperiment.NameB16, BundleSize: 16,
		},
		{
			Name: "full-key-schema-b32", Performance: performance,
			Compression: bond.DefaultCompressionProfile, TableFilter: bond.DefaultTableFilterProfile,
			WriterSchema: fullkeyexperiment.NameB32, BundleSize: 32,
		},
		{
			Name: "full-key-schema-b64", Performance: performance,
			Compression: bond.DefaultCompressionProfile, TableFilter: bond.DefaultTableFilterProfile,
			WriterSchema: fullkeyexperiment.NameB64, BundleSize: 64,
		},
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

func RunFullKeyLifecycleBenchmarks(b *testing.B, repoRoot string) {
	b.ReportAllocs()
	dataset, err := Generate(RepresentativeDatasetSpec(20260806, 1_000))
	if err != nil {
		b.Fatal(err)
	}
	for _, candidate := range FullKeyCandidates(bond.LowPerformance) {
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
				b.ReportMetric(float64(manifest.Results.AfterCompaction.SST.PropertyDataBytes), "data-bytes/op")
				b.ReportMetric(float64(manifest.Results.AfterCompaction.SST.PropertyIndexUncompressedBytes), "index-bytes/op")
				b.ReportMetric(float64(manifest.Results.AfterCompaction.SST.PropertyFilterBytes), "filter-bytes/op")
				b.ReportMetric(float64(manifest.Results.ProcessCompactionCPUNS), "compact-cpu-ns/op")
			}
		})
	}
}
