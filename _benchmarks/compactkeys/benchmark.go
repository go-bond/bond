package compactkeys

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/internal/fullkeyexperiment"
	"github.com/go-bond/bond/internal/typedschemaexperiment"
)

type TypedFamilySuite struct {
	Name    string
	Dataset DatasetSpec
	Engines []EngineSpec
}

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

// TypedFamilySuites compare the frozen legacy writer with one typed schema on
// a matching isolated one-family dataset. Stock Pebble cannot route these
// writers per table, so no suite mixes primary-key families or represents a
// production option.
func TypedFamilySuites(
	performance bond.PerformanceProfile, seed int64, rows int,
) []TypedFamilySuite {
	type suiteSpec struct {
		name   string
		shape  KeyShape
		writer string
	}
	specs := []suiteSpec{
		{name: "sequential-u64", shape: KeyShapeSequentialUint64, writer: typedschemaexperiment.NameUint64},
		{name: "random-u64", shape: KeyShapeRandomUint64, writer: typedschemaexperiment.NameUint64},
		{name: "sequential-u32", shape: KeyShapeSequentialUint32, writer: typedschemaexperiment.NameUint32},
		{name: "random-u32", shape: KeyShapeRandomUint32, writer: typedschemaexperiment.NameUint32},
		{name: "bytes-32", shape: KeyShapeBytes32, writer: typedschemaexperiment.NameBytes},
	}
	result := make([]TypedFamilySuite, 0, len(specs))
	for index, spec := range specs {
		dataset := DatasetSpec{
			Seed:                seed + int64(index),
			Rows:                rows,
			SecondaryIndexes:    3,
			KeyShape:            spec.shape,
			OrderShape:          OrderShapeMixed,
			IndexCardinality:    IndexCardinalityMixed,
			PrefixShape:         PrefixShapeMixed,
			PartialIndexPercent: 70,
			ValueBytes:          160,
		}
		result = append(result, TypedFamilySuite{
			Name:    spec.name,
			Dataset: dataset,
			Engines: []EngineSpec{
				{
					Name: "legacy-" + spec.name, Performance: performance,
					Compression: bond.DefaultCompressionProfile, TableFilter: bond.DefaultTableFilterProfile,
					WriterSchema: bond.DefaultKeySchemaName(), BundleSize: typedschemaexperiment.BundleSize,
				},
				{
					Name: "typed-" + spec.name, Performance: performance,
					Compression: bond.DefaultCompressionProfile, TableFilter: bond.DefaultTableFilterProfile,
					WriterSchema: spec.writer, BundleSize: typedschemaexperiment.BundleSize,
				},
			},
		})
	}
	return result
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

func RunTypedFamilyLifecycleBenchmarks(b *testing.B, repoRoot string) {
	b.ReportAllocs()
	for _, suite := range TypedFamilySuites(bond.LowPerformance, 20260806, 1_000) {
		dataset, err := Generate(suite.Dataset)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(suite.Name, func(b *testing.B) {
			for _, candidate := range suite.Engines {
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
						b.ReportMetric(float64(manifest.Results.ProcessCompactionCPUNS), "compact-cpu-ns/op")
						b.ReportMetric(float64(manifest.Results.HitLatency.P95NS), "hit-p95-ns/op")
						b.ReportMetric(float64(manifest.Results.PointMissLatency.P95NS), "miss-p95-ns/op")
					}
				})
			}
		})
	}
}
