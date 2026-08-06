package compactkeys

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

func TestDatasetGeneratorDeterministic(t *testing.T) {
	spec := RepresentativeDatasetSpec(0x5eed, 128)
	first, err := Generate(spec)
	require.NoError(t, err)
	second, err := Generate(spec)
	require.NoError(t, err)
	require.Equal(t, first.Digest, second.Digest)
	require.True(t, reflect.DeepEqual(first, second))

	changed, err := Generate(RepresentativeDatasetSpec(0x5eee, 128))
	require.NoError(t, err)
	require.NotEqual(t, first.Digest, changed.Digest)
}

func TestDatasetGeneratorCoversShapes(t *testing.T) {
	wantKeyShapes := map[KeyShape]bool{
		KeyShapeSequentialUint64: false,
		KeyShapeRandomUint64:     false,
		KeyShapeBytes20:          false,
		KeyShapeBytes32:          false,
		KeyShapeBytes64:          false,
		KeyShapeUUID:             false,
		KeyShapeAddress:          false,
		KeyShapeComposite2:       false,
		KeyShapeComposite3:       false,
	}
	wantOrders := map[OrderShape]bool{OrderShapeNone: false, OrderShapeFixed: false, OrderShapeVariable: false}
	wantCardinality := map[IndexCardinality]bool{IndexCardinalityLow: false, IndexCardinalityHigh: false}
	wantPrefixes := map[PrefixShape]bool{PrefixShapeCommon: false, PrefixShapeRandom: false}
	wantIndexCounts := map[int]bool{1: false, 3: false, 8: false}

	for _, spec := range CoverageDatasetSpecs() {
		wantKeyShapes[spec.KeyShape] = true
		wantOrders[spec.OrderShape] = true
		wantCardinality[spec.IndexCardinality] = true
		wantPrefixes[spec.PrefixShape] = true
		wantIndexCounts[spec.SecondaryIndexes] = true
		require.Less(t, spec.PartialIndexPercent, 100)
		dataset, err := Generate(spec)
		require.NoError(t, err)
		require.NotEmpty(t, dataset.InitialEntries)
		require.NotEmpty(t, dataset.Digest)
	}
	for shape, covered := range wantKeyShapes {
		require.Truef(t, covered, "key shape %s is not covered", shape)
	}
	for shape, covered := range wantOrders {
		require.Truef(t, covered, "order shape %s is not covered", shape)
	}
	for shape, covered := range wantCardinality {
		require.Truef(t, covered, "cardinality %s is not covered", shape)
	}
	for shape, covered := range wantPrefixes {
		require.Truef(t, covered, "prefix shape %s is not covered", shape)
	}
	for count, covered := range wantIndexCounts {
		require.Truef(t, covered, "%d secondary indexes are not covered", count)
	}
}

func TestVariableOrderUsesMultipleEncodedLengths(t *testing.T) {
	lengths := make(map[int]struct{})
	for row := range 48 {
		lengths[len(makeIndexOrder(OrderShapeVariable, row, 1))] = struct{}{}
	}
	require.Greater(t, len(lengths), 1)
}

func TestOracleDetectsRecordAndOrderDrift(t *testing.T) {
	dataset, err := Generate(RepresentativeDatasetSpec(42, 40))
	require.NoError(t, err)
	opts := bond.BuildPebbleOptions(bond.LowPerformance)
	db, err := pebble.Open(t.TempDir(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = writeEntries(db, dataset.InitialEntries, 32)
	require.NoError(t, err)
	require.NoError(t, NewOracle(dataset.InitialEntries).Verify(db))

	valueDrift := NewOracle(dataset.InitialEntries)
	valueDrift.Entries[len(valueDrift.Entries)/2].Value = []byte("corrupt")
	require.ErrorContains(t, valueDrift.Verify(db), "value mismatch")

	orderDrift := NewOracle(dataset.InitialEntries)
	orderDrift.Entries[0], orderDrift.Entries[1] = orderDrift.Entries[1], orderDrift.Entries[0]
	require.ErrorContains(t, orderDrift.Verify(db), "key mismatch")

	missDrift := NewOracle(dataset.InitialEntries, [][]byte{dataset.InitialEntries[0].Key})
	require.ErrorContains(t, missDrift.Verify(db), "point miss")
}

func TestStorageMetricsSnapshotUsesPebbleGlobalWriteAmp(t *testing.T) {
	metrics := &pebble.Metrics{}
	metrics.WAL.BytesWritten = 100
	metrics.Levels[0].TablesFlushed.Bytes = 80
	metrics.Levels[1].TableBytesIn = 999
	metrics.Levels[6].TablesCompacted.Bytes = 120

	total := metrics.Total()
	want := total.WriteAmp()
	require.Equal(t, 3.0, want)
	require.Equal(t, want, storageMetricsSnapshot(metrics).WriteAmplification)
}

func TestManifestRoundTrip(t *testing.T) {
	manifest := RunManifest{
		ManifestVersion: ManifestVersion,
		CapturedAt:      time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
		Revisions: Revisions{
			BondCommit:        "bond-sha",
			BondDirty:         true,
			SourceFingerprint: "source-digest",
			PebbleVersion:     "pebble-version",
			PebbleCommit:      "pebble-sha",
			PebbleOriginURL:   "origin",
		},
		Runtime: RuntimeInfo{GoVersion: "go-test", GOOS: "linux", GOARCH: "amd64", Compiler: "gc"},
		Machine: MachineInfo{Hostname: "host", CPUModel: "cpu", LogicalCPUs: 8, MemoryBytes: 1024},
		Engine: EngineSpec{
			Name:         "baseline",
			Performance:  bond.MediumPerformance,
			Compression:  bond.CompressionBalanced,
			TableFilter:  bond.TableFilterProgressiveBloom,
			WriterSchema: "default",
			BundleSize:   16,
		},
		Run:             RunSpec{Warmups: 1, Repetitions: 3, Run: 2, BatchSize: 512},
		Dataset:         RepresentativeDatasetSpec(42, 100),
		DatasetDigest:   "dataset-digest",
		LogicalLayout:   LogicalLayoutVersion,
		FormatMajor:     30,
		ActiveKeySchema: "default",
		StoragePolicy: StoragePolicyProvenance{
			SpanPolicy: SpanPolicyManifest{
				ValueStorage: ValueStoragePolicyManifest{DisableBlobSeparation: true},
			},
			ValueSeparation:    ValueSeparationManifest{Enabled: true, MinimumSize: 64},
			ValueBlocksEnabled: true,
		},
		Schema: SchemaProvenance{
			ComparerName:      "comparer",
			ActiveWriter:      "default",
			RegisteredReaders: []string{"default", "reader-v1"},
			ActiveBundleSize:  16,
		},
		EffectiveOptions: "[Options]",
		Results: LifecycleResults{
			Rows: 100,
			AfterCompaction: StorageSnapshot{SST: SSTResults{
				CompressionProfiles: map[string]int{"Balanced": 1},
				FilterFamilies:      map[string]int{"bloom": 1},
				KeySchemas:          map[string]int{"default": 1},
			}},
			CompactionDelta: CompactionDelta{
				TableBytesRead:    100,
				TableBytesWritten: 80,
			},
			Filter: FilterResults{AbsentProbes: 10, FalsePositives: 1, FalsePositiveRate: 0.1},
		},
	}
	data, err := manifest.JSON()
	require.NoError(t, err)
	var decoded RunManifest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, manifest, decoded)
}

func TestLifecycleOracleAndManifest(t *testing.T) {
	dataset, err := Generate(RepresentativeDatasetSpec(20260806, 120))
	require.NoError(t, err)
	manifest, err := RunLifecycle(context.Background(), RunRequest{
		RepoRoot: repositoryRoot(t),
		RootDir:  filepath.Join(t.TempDir(), "lifecycle"),
		Engine: EngineSpec{
			Name:        "test-balanced-progressive-bloom",
			Performance: bond.LowPerformance,
			Compression: bond.CompressionBalanced,
			TableFilter: bond.TableFilterProgressiveBloom,
		},
		Run:     RunSpec{Warmups: 0, Repetitions: 1, Run: 1, BatchSize: 64},
		Dataset: dataset,
	})
	require.NoError(t, err)
	require.Equal(t, ManifestVersion, manifest.ManifestVersion)
	require.Equal(t, dataset.Digest, manifest.DatasetDigest)
	require.Equal(t, LogicalLayoutVersion, manifest.LogicalLayout)
	require.Equal(t, uint64(pebble.FormatNewest), manifest.FormatMajor)
	require.Equal(t, "8fb150d9135d6f94e183a874475e0bd1afb18f63", manifest.Revisions.PebbleCommit)
	require.NotEmpty(t, manifest.Revisions.SourceFingerprint)
	require.NotEmpty(t, manifest.ActiveKeySchema)
	require.Contains(t, manifest.EffectiveOptions, "format_major_version=30")
	require.Positive(t, manifest.Results.DatabaseBytes)
	require.Positive(t, manifest.Results.AfterFlush.SST.Files)
	require.Positive(t, manifest.Results.AfterCompaction.SST.Files)
	require.Positive(t, manifest.Results.AfterCompaction.SST.PropertyFilterBytes)
	require.Positive(t, manifest.Results.CompactionDelta.PebbleDurationNS)
	require.Equal(t, len(dataset.MissKeys), manifest.Results.PointMissLatency.Samples)
	require.Equal(t, len(dataset.MissKeys), manifest.Results.Filter.AbsentProbes)
	require.Positive(t, manifest.Results.Filter.ObservedChecks)
	require.Equal(t, manifest.Results.Filter.ObservedChecks, manifest.Results.Filter.UsefulNegatives+manifest.Results.Filter.FalsePositives)
	require.False(t, manifest.StoragePolicy.SpanPolicy.PreferFastCompression)
	require.True(t, manifest.StoragePolicy.SpanPolicy.ValueStorage.DisableBlobSeparation)
	require.True(t, manifest.StoragePolicy.SpanPolicy.ValueStorage.DisableSeparationBySuffix)
	require.Equal(t, 16, manifest.Engine.BundleSize)
	require.Equal(t, 16, manifest.Schema.ActiveBundleSize)
	require.Equal(t, manifest.ActiveKeySchema, manifest.Schema.ActiveWriter)
	require.True(t, sort.StringsAreSorted(manifest.Schema.RegisteredReaders))
	require.Contains(t, manifest.Schema.RegisteredReaders, manifest.ActiveKeySchema)
}

func BenchmarkBondLifecycleBaselines(b *testing.B) {
	RunLifecycleBenchmarks(b, repositoryRoot(b))
}

func repositoryRoot(tb testing.TB) string {
	tb.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
}
