package compactkeys

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
	"github.com/go-bond/bond/internal/fullkeyexperiment"
)

type RunRequest struct {
	RepoRoot       string
	RootDir        string
	Engine         EngineSpec
	Run            RunSpec
	Dataset        Dataset
	OptionsBuilder func(EngineSpec) (*pebble.Options, error)
	KeepDatabase   bool
}

type quietPebbleLogger struct{}

func (quietPebbleLogger) Infof(string, ...interface{})  {}
func (quietPebbleLogger) Errorf(string, ...interface{}) {}
func (quietPebbleLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func RunLifecycle(ctx context.Context, request RunRequest) (manifest RunManifest, returnedErr error) {
	if request.RepoRoot == "" {
		return RunManifest{}, fmt.Errorf("repository root is required")
	}
	if request.RootDir == "" {
		return RunManifest{}, fmt.Errorf("run root is required")
	}
	if request.Run.BatchSize <= 0 {
		request.Run.BatchSize = 512
	}
	if request.Engine.Name == "" {
		return RunManifest{}, fmt.Errorf("engine name is required")
	}
	if request.Dataset.Digest == "" || request.Dataset.Digest != datasetDigest(request.Dataset) {
		return RunManifest{}, fmt.Errorf("dataset digest is missing or stale")
	}
	if err := os.MkdirAll(request.RootDir, 0o755); err != nil {
		return RunManifest{}, err
	}
	if !request.KeepDatabase {
		defer func() {
			if err := os.RemoveAll(filepath.Join(request.RootDir, "db")); returnedErr == nil && err != nil {
				returnedErr = err
			}
			if err := os.RemoveAll(filepath.Join(request.RootDir, "checkpoint")); returnedErr == nil && err != nil {
				returnedErr = err
			}
		}()
	}

	dbDir := filepath.Join(request.RootDir, "db")
	checkpointDir := filepath.Join(request.RootDir, "checkpoint")
	for _, path := range []string{dbDir, checkpointDir} {
		if _, err := os.Stat(path); err == nil {
			return RunManifest{}, fmt.Errorf("run path already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return RunManifest{}, err
		}
	}

	newOptions := func() (*pebble.Options, error) {
		var opts *pebble.Options
		var err error
		if request.OptionsBuilder != nil {
			opts, err = request.OptionsBuilder(request.Engine)
		} else {
			opts, err = bond.BuildPebbleOptionsWithConfig(bond.PebbleOptionsConfig{
				Performance: request.Engine.Performance,
				Compression: request.Engine.Compression,
				TableFilter: request.Engine.TableFilter,
			})
			if err == nil {
				if request.Engine.BundleSize > 0 {
					err = fullkeyexperiment.Configure(opts, request.Engine.WriterSchema)
				} else {
					opts.EnsureDefaults()
				}
			}
		}
		if err != nil {
			return nil, err
		}
		opts.Logger = quietPebbleLogger{}
		return opts, nil
	}
	opts, err := newOptions()
	if err != nil {
		return RunManifest{}, err
	}
	if request.Engine.WriterSchema != "" && request.Engine.WriterSchema != opts.KeySchema {
		return RunManifest{}, fmt.Errorf("options selected writer %q, requested %q", opts.KeySchema, request.Engine.WriterSchema)
	}

	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		return RunManifest{}, err
	}
	dbOpen := true
	defer func() {
		if dbOpen {
			if err := db.Close(); returnedErr == nil && err != nil {
				returnedErr = err
			}
		}
	}()

	results := LifecycleResults{
		Rows:                request.Dataset.PrimaryRows,
		InitialEntries:      len(request.Dataset.InitialEntries),
		FinalEntries:        len(request.Dataset.FinalEntries),
		InitialIndexEntries: request.Dataset.InitialIndexEntries,
		FinalIndexEntries:   request.Dataset.FinalIndexEntries,
	}

	loadLatencies, err := writeEntries(db, request.Dataset.InitialEntries, request.Run.BatchSize)
	if err != nil {
		return RunManifest{}, fmt.Errorf("load dataset: %w", err)
	}
	results.LoadLatency = summarizeLatencies(loadLatencies)
	initialOracle := NewOracle(request.Dataset.InitialEntries, request.Dataset.MissKeys)
	if err := initialOracle.Verify(db); err != nil {
		return RunManifest{}, fmt.Errorf("verify initial dataset: %w", err)
	}

	snapshot := db.NewSnapshot()
	mutationLatencies, err := applyMutations(db, request.Dataset.Mutations, request.Run.BatchSize)
	if err != nil {
		snapshot.Close()
		return RunManifest{}, fmt.Errorf("apply mutations: %w", err)
	}
	results.MutationLatency = summarizeLatencies(mutationLatencies)
	finalOracle := NewOracle(request.Dataset.FinalEntries, request.Dataset.MissKeys)
	if err := finalOracle.Verify(db); err != nil {
		snapshot.Close()
		return RunManifest{}, fmt.Errorf("verify mutations: %w", err)
	}
	if err := initialOracle.Verify(snapshot); err != nil {
		snapshot.Close()
		return RunManifest{}, fmt.Errorf("verify snapshot: %w", err)
	}
	if err := snapshot.Close(); err != nil {
		return RunManifest{}, err
	}

	started := time.Now()
	if err := db.Flush(); err != nil {
		return RunManifest{}, fmt.Errorf("flush: %w", err)
	}
	results.FlushNS = time.Since(started).Nanoseconds()
	metricsAfterFlush := db.Metrics()
	results.AfterFlush, err = collectStorageSnapshot(db, metricsAfterFlush)
	if err != nil {
		return RunManifest{}, fmt.Errorf("capture post-flush storage: %w", err)
	}

	compactStart, compactEnd := compactBounds(request.Dataset.FinalEntries)
	cpuStart, err := processCPUTime()
	if err != nil {
		return RunManifest{}, err
	}
	started = time.Now()
	if err := db.Compact(ctx, compactStart, compactEnd, false); err != nil {
		return RunManifest{}, fmt.Errorf("compact: %w", err)
	}
	results.CompactionNS = time.Since(started).Nanoseconds()
	cpuEnd, err := processCPUTime()
	if err != nil {
		return RunManifest{}, err
	}
	results.ProcessCompactionCPUNS = (cpuEnd - cpuStart).Nanoseconds()
	metricsAfterCompaction := db.Metrics()
	results.AfterCompaction, err = collectStorageSnapshot(db, metricsAfterCompaction)
	if err != nil {
		return RunManifest{}, fmt.Errorf("capture post-compaction storage: %w", err)
	}
	results.CompactionDelta = calculateCompactionDelta(metricsAfterFlush, metricsAfterCompaction)
	if err := finalOracle.Verify(db); err != nil {
		return RunManifest{}, fmt.Errorf("verify compacted dataset: %w", err)
	}

	hitLatencies, err := measureHits(db, request.Dataset.FinalEntries, 2048)
	if err != nil {
		return RunManifest{}, err
	}
	results.HitLatency = summarizeLatencies(hitLatencies)
	pointMissLatencies, err := measurePointMisses(db, request.Dataset.MissKeys)
	if err != nil {
		return RunManifest{}, err
	}
	results.PointMissLatency = summarizeLatencies(pointMissLatencies)
	filterBefore := db.Metrics().Filter
	filterProbeLatencies, err := measureFilterProbes(db, request.Dataset.MissKeys)
	if err != nil {
		return RunManifest{}, err
	}
	results.FilterProbeLatency = summarizeLatencies(filterProbeLatencies)
	filterAfter := db.Metrics().Filter
	results.Filter = filterResults(len(request.Dataset.MissKeys), filterBefore, filterAfter)
	probeMetrics := db.Metrics()
	cacheHits, cacheMisses := probeMetrics.BlockCache.HitsAndMisses.Aggregate()
	results.PostProbeCache = CacheResults{Hits: cacheHits, Misses: cacheMisses, Bytes: probeMetrics.BlockCache.Size}

	started = time.Now()
	if err := db.Checkpoint(checkpointDir); err != nil {
		return RunManifest{}, fmt.Errorf("checkpoint: %w", err)
	}
	checkpointCompatibility := benchmarkStorageCompatibility(opts, results.AfterCompaction.SST)
	if err := bond.WriteStorageCompatibility(checkpointDir, checkpointCompatibility); err != nil {
		return RunManifest{}, fmt.Errorf("write checkpoint compatibility: %w", err)
	}
	results.CheckpointNS = time.Since(started).Nanoseconds()
	checkpointOptions, err := newOptions()
	if err != nil {
		return RunManifest{}, err
	}
	started = time.Now()
	checkpointDB, err := pebble.Open(checkpointDir, checkpointOptions)
	results.CheckpointOpenNS = time.Since(started).Nanoseconds()
	if err != nil {
		return RunManifest{}, fmt.Errorf("open checkpoint: %w", err)
	}
	if err := finalOracle.Verify(checkpointDB); err != nil {
		checkpointDB.Close()
		return RunManifest{}, fmt.Errorf("verify checkpoint: %w", err)
	}
	if err := checkpointDB.Close(); err != nil {
		return RunManifest{}, err
	}

	if err := db.Close(); err != nil {
		return RunManifest{}, err
	}
	dbOpen = false
	reopenOptions, err := newOptions()
	if err != nil {
		return RunManifest{}, err
	}
	started = time.Now()
	reopened, err := pebble.Open(dbDir, reopenOptions)
	results.ReopenNS = time.Since(started).Nanoseconds()
	if err != nil {
		return RunManifest{}, fmt.Errorf("reopen database: %w", err)
	}
	if err := finalOracle.Verify(reopened); err != nil {
		reopened.Close()
		return RunManifest{}, fmt.Errorf("verify reopened database: %w", err)
	}
	if err := reopened.Close(); err != nil {
		return RunManifest{}, err
	}

	results.DatabaseBytes, err = directoryBytes(dbDir)
	if err != nil {
		return RunManifest{}, err
	}
	if results.Rows > 0 {
		results.BytesPerRow = float64(results.DatabaseBytes) / float64(results.Rows)
	}
	if results.FinalIndexEntries > 0 {
		results.BytesPerIndexEntry = float64(results.DatabaseBytes) / float64(results.FinalIndexEntries)
	}

	revisions, err := collectRevisions(request.RepoRoot)
	if err != nil {
		return RunManifest{}, err
	}
	machine, err := collectMachine(request.RootDir)
	if err != nil {
		return RunManifest{}, err
	}
	storagePolicy, schema, err := collectPolicyProvenance(opts, request.Engine.BundleSize)
	if err != nil {
		return RunManifest{}, err
	}
	engine := request.Engine
	engine.WriterSchema = opts.KeySchema
	engine.BundleSize = schema.ActiveBundleSize
	return RunManifest{
		ManifestVersion: ManifestVersion,
		CapturedAt:      time.Now().UTC(),
		Revisions:       revisions,
		Runtime: RuntimeInfo{
			GoVersion: runtime.Version(),
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			Compiler:  runtime.Compiler,
		},
		Machine:          machine,
		Engine:           engine,
		Run:              request.Run,
		Dataset:          request.Dataset.Spec,
		DatasetDigest:    request.Dataset.Digest,
		LogicalLayout:    LogicalLayoutVersion,
		FormatMajor:      uint64(opts.FormatMajorVersion),
		ActiveKeySchema:  opts.KeySchema,
		StoragePolicy:    storagePolicy,
		Schema:           schema,
		Compatibility:    checkpointCompatibility,
		EffectiveOptions: opts.String(),
		Results:          results,
	}, nil
}

func benchmarkStorageCompatibility(opts *pebble.Options, sst SSTResults) bond.StorageCompatibility {
	requiredSet := make(map[string]struct{})
	for name, count := range sst.KeySchemas {
		if count > 0 {
			requiredSet[name] = struct{}{}
		}
	}
	if len(requiredSet) == 0 && opts.KeySchema != "" {
		requiredSet[opts.KeySchema] = struct{}{}
	}
	required := make([]string, 0, len(requiredSet))
	for name := range requiredSet {
		required = append(required, name)
	}
	sort.Strings(required)
	return bond.StorageCompatibility{
		ReaderEpoch:       bond.StorageReaderEpoch,
		FormatMajor:       uint64(opts.FormatMajorVersion),
		RequiredKeySchema: required,
	}
}

func writeEntries(db *pebble.DB, entries []Entry, batchSize int) ([]time.Duration, error) {
	latencies := make([]time.Duration, 0, (len(entries)+batchSize-1)/batchSize)
	for start := 0; start < len(entries); start += batchSize {
		end := min(start+batchSize, len(entries))
		batch := db.NewBatch()
		for _, entry := range entries[start:end] {
			if err := batch.Set(entry.Key, entry.Value, nil); err != nil {
				batch.Close()
				return nil, err
			}
		}
		started := time.Now()
		err := batch.Commit(pebble.NoSync)
		latencies = append(latencies, time.Since(started))
		closeErr := batch.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return latencies, nil
}

func applyMutations(db *pebble.DB, mutations []Mutation, batchSize int) ([]time.Duration, error) {
	latencies := make([]time.Duration, 0, (len(mutations)+batchSize-1)/batchSize)
	for start := 0; start < len(mutations); start += batchSize {
		end := min(start+batchSize, len(mutations))
		batch := db.NewBatch()
		for _, mutation := range mutations[start:end] {
			var err error
			if mutation.Delete {
				err = batch.Delete(mutation.Key, nil)
			} else {
				err = batch.Set(mutation.Key, mutation.Value, nil)
			}
			if err != nil {
				batch.Close()
				return nil, err
			}
		}
		started := time.Now()
		err := batch.Commit(pebble.NoSync)
		latencies = append(latencies, time.Since(started))
		closeErr := batch.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return latencies, nil
}

func measureHits(db *pebble.DB, entries []Entry, limit int) ([]time.Duration, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	count := min(len(entries), limit)
	stride := max(1, len(entries)/count)
	latencies := make([]time.Duration, 0, count)
	for i := 0; i < len(entries) && len(latencies) < count; i += stride {
		started := time.Now()
		value, closer, err := db.Get(entries[i].Key)
		latencies = append(latencies, time.Since(started))
		if err != nil {
			return nil, fmt.Errorf("hit probe %d: %w", i, err)
		}
		if !bytes.Equal(value, entries[i].Value) {
			closer.Close()
			return nil, fmt.Errorf("hit probe %d value mismatch", i)
		}
		if err := closer.Close(); err != nil {
			return nil, err
		}
	}
	return latencies, nil
}

func measurePointMisses(db *pebble.DB, keys [][]byte) ([]time.Duration, error) {
	latencies := make([]time.Duration, 0, len(keys))
	for i, key := range keys {
		started := time.Now()
		_, closer, err := db.Get(key)
		latencies = append(latencies, time.Since(started))
		if err == nil {
			if closer != nil {
				_ = closer.Close()
			}
			return nil, fmt.Errorf("point miss probe %d unexpectedly found a value", i)
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			return nil, fmt.Errorf("point miss probe %d: %w", i, err)
		}
	}
	return latencies, nil
}

func measureFilterProbes(db *pebble.DB, keys [][]byte) ([]time.Duration, error) {
	latencies := make([]time.Duration, 0, len(keys))
	iter, err := db.NewIter(&pebble.IterOptions{UseL6Filters: true})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for i, key := range keys {
		started := time.Now()
		valid := iter.SeekPrefixGE(key)
		latencies = append(latencies, time.Since(started))
		if valid {
			return nil, fmt.Errorf("miss probe %d unexpectedly found %x", i, iter.Key())
		}
		if err := iter.Error(); err != nil {
			return nil, fmt.Errorf("miss probe %d: %w", i, err)
		}
	}
	return latencies, nil
}

func filterResults(probes int, before, after pebble.FilterMetrics) FilterResults {
	hits := after.Hits - before.Hits
	misses := after.Misses - before.Misses
	checks := hits + misses
	rate := 0.0
	if checks > 0 {
		rate = float64(misses) / float64(checks)
	}
	return FilterResults{
		AbsentProbes:      probes,
		L6FiltersEnabled:  true,
		UsefulNegatives:   hits,
		FalsePositives:    misses,
		ObservedChecks:    checks,
		FalsePositiveRate: rate,
	}
}

func collectSSTResults(db *pebble.DB) (SSTResults, error) {
	levels, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		return SSTResults{}, err
	}
	result := SSTResults{
		CompressionProfiles: make(map[string]int),
		FilterFamilies:      make(map[string]int),
		KeySchemas:          make(map[string]int),
		KeySchemaBytes:      make(map[string]uint64),
	}
	for _, level := range levels {
		for _, table := range level {
			result.Files++
			result.PhysicalBytes += table.Size
			if table.Properties == nil {
				return SSTResults{}, fmt.Errorf("properties missing for SST %s", table.FileNum)
			}
			properties := table.Properties
			result.PropertyRawKeyBytes += properties.RawKeySize
			result.PropertyRawValueBytes += properties.RawValueSize
			result.PropertyDataBytes += properties.DataSize
			result.PropertyIndexUncompressedBytes += properties.IndexSize
			result.PropertyFilterBytes += properties.FilterSize
			result.PropertyValueBlockBytes += properties.ValueBlocksSize
			result.CompressionProfiles[properties.CompressionName]++
			result.FilterFamilies[properties.FilterFamily]++
			result.KeySchemas[properties.KeySchemaName]++
			result.KeySchemaBytes[properties.KeySchemaName] += table.Size
		}
	}
	return result, nil
}

func collectStorageSnapshot(db *pebble.DB, metrics *pebble.Metrics) (StorageSnapshot, error) {
	sst, err := collectSSTResults(db)
	if err != nil {
		return StorageSnapshot{}, err
	}
	return StorageSnapshot{Metrics: storageMetricsSnapshot(metrics), SST: sst}, nil
}

func storageMetricsSnapshot(metrics *pebble.Metrics) StorageMetricsSnapshot {
	total := metrics.Total()
	snapshot := StorageMetricsSnapshot{
		PebbleCompactionDurationNS: metrics.Compact.Duration.Nanoseconds(),
		WALBytesIn:                 metrics.WAL.BytesIn,
		WALBytesWritten:            metrics.WAL.BytesWritten,
		MemtableBytes:              metrics.MemTable.Size,
		WriteAmplification:         total.WriteAmp(),
	}
	cacheHits, cacheMisses := metrics.BlockCache.HitsAndMisses.Aggregate()
	snapshot.Cache = CacheResults{Hits: cacheHits, Misses: cacheMisses, Bytes: metrics.BlockCache.Size}
	if len(metrics.Levels) > 0 {
		snapshot.L0Sublevels = metrics.Levels[0].Sublevels
	}
	for level, levelMetrics := range metrics.Levels {
		snapshot.Levels = append(snapshot.Levels, LevelResult{
			Level:     level,
			Files:     levelMetrics.Tables.Count,
			Bytes:     levelMetrics.Tables.Bytes,
			Sublevels: levelMetrics.Sublevels,
		})
		snapshot.CompactionBytesRead += levelMetrics.TableBytesRead + levelMetrics.BlobBytesRead
		snapshot.CompactionBytesWritten += levelMetrics.TablesCompacted.Bytes + levelMetrics.BlobBytesCompacted
	}
	return snapshot
}

func calculateCompactionDelta(before, after *pebble.Metrics) CompactionDelta {
	delta := CompactionDelta{
		PebbleDurationNS: after.Compact.Duration.Nanoseconds() - before.Compact.Duration.Nanoseconds(),
	}
	for level := range after.Levels {
		delta.TableBytesRead += counterDelta(before.Levels[level].TableBytesRead, after.Levels[level].TableBytesRead)
		delta.TableBytesWritten += counterDelta(before.Levels[level].TablesCompacted.Bytes, after.Levels[level].TablesCompacted.Bytes)
		delta.BlobBytesRead += counterDelta(before.Levels[level].BlobBytesRead, after.Levels[level].BlobBytesRead)
		delta.BlobBytesWritten += counterDelta(before.Levels[level].BlobBytesCompacted, after.Levels[level].BlobBytesCompacted)
	}
	return delta
}

func counterDelta(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func collectPolicyProvenance(opts *pebble.Options, configuredBundleSize int) (StoragePolicyProvenance, SchemaProvenance, error) {
	if opts.SpanPolicyFunc == nil {
		return StoragePolicyProvenance{}, SchemaProvenance{}, fmt.Errorf("span policy function is not configured")
	}
	spanPolicy, err := opts.SpanPolicyFunc(pebble.UserKeyBounds{})
	if err != nil {
		return StoragePolicyProvenance{}, SchemaProvenance{}, fmt.Errorf("resolve effective span policy: %w", err)
	}
	if opts.ValueSeparationPolicy == nil {
		return StoragePolicyProvenance{}, SchemaProvenance{}, fmt.Errorf("value separation policy is not configured")
	}
	valueSeparation := opts.ValueSeparationPolicy()
	valueBlocksEnabled := opts.EnableValueBlocks != nil && opts.EnableValueBlocks()
	storage := StoragePolicyProvenance{
		SpanPolicy: SpanPolicyManifest{
			KeyRangeStart:         hex.EncodeToString(spanPolicy.KeyRange.Start),
			KeyRangeEnd:           hex.EncodeToString(spanPolicy.KeyRange.End),
			PreferFastCompression: spanPolicy.PreferFastCompression,
			ValueStorage: ValueStoragePolicyManifest{
				DisableSeparationBySuffix:         spanPolicy.ValueStoragePolicy.DisableSeparationBySuffix,
				DisableBlobSeparation:             spanPolicy.ValueStoragePolicy.DisableBlobSeparation,
				OverrideBlobSeparationMinimumSize: spanPolicy.ValueStoragePolicy.OverrideBlobSeparationMinimumSize,
				MinimumMVCCGarbageSize:            spanPolicy.ValueStoragePolicy.MinimumMVCCGarbageSize,
			},
			TieringPolicyIsSet: spanPolicy.TieringPolicy.IsSet(),
		},
		ValueSeparation: ValueSeparationManifest{
			Enabled:                  valueSeparation.Enabled,
			MinimumSize:              valueSeparation.MinimumSize,
			MinimumMVCCGarbageSize:   valueSeparation.MinimumMVCCGarbageSize,
			MaxBlobReferenceDepth:    valueSeparation.MaxBlobReferenceDepth,
			RewriteMinimumAgeNS:      valueSeparation.RewriteMinimumAge.Nanoseconds(),
			GarbageRatioLowPriority:  valueSeparation.GarbageRatioLowPriority,
			GarbageRatioHighPriority: valueSeparation.GarbageRatioHighPriority,
		},
		ValueBlocksEnabled: valueBlocksEnabled,
	}

	readers := make([]string, 0, len(opts.KeySchemas))
	for name := range opts.KeySchemas {
		readers = append(readers, name)
	}
	sort.Strings(readers)
	bundleSize, err := activeBundleSize(opts.KeySchema, configuredBundleSize)
	if err != nil {
		return StoragePolicyProvenance{}, SchemaProvenance{}, err
	}
	return storage, SchemaProvenance{
		ComparerName:      opts.Comparer.Name,
		ActiveWriter:      opts.KeySchema,
		RegisteredReaders: readers,
		ActiveBundleSize:  bundleSize,
	}, nil
}

func activeBundleSize(schemaName string, configured int) (int, error) {
	if strings.HasPrefix(schemaName, "DefaultKeySchema(") && strings.HasSuffix(schemaName, ")") {
		comma := strings.LastIndexByte(schemaName, ',')
		if comma < 0 {
			return 0, fmt.Errorf("default schema name has no bundle size: %q", schemaName)
		}
		parsed, err := strconv.Atoi(strings.TrimSuffix(schemaName[comma+1:], ")"))
		if err != nil || parsed <= 0 {
			return 0, fmt.Errorf("parse bundle size from schema %q", schemaName)
		}
		if configured != 0 && configured != parsed {
			return 0, fmt.Errorf("configured bundle size %d disagrees with active schema %q", configured, schemaName)
		}
		return parsed, nil
	}
	for _, candidate := range []struct {
		name string
		size int
	}{
		{name: fullkeyexperiment.NameB16, size: 16},
		{name: fullkeyexperiment.NameB32, size: 32},
		{name: fullkeyexperiment.NameB64, size: 64},
	} {
		if schemaName == candidate.name {
			if configured != 0 && configured != candidate.size {
				return 0, fmt.Errorf("configured bundle size %d disagrees with active schema %q", configured, schemaName)
			}
			return candidate.size, nil
		}
	}
	if configured <= 0 {
		return 0, fmt.Errorf("bundle size is required for active schema %q", schemaName)
	}
	return configured, nil
}

func summarizeLatencies(samples []time.Duration) LatencySummary {
	if len(samples) == 0 {
		return LatencySummary{}
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var total time.Duration
	for _, sample := range ordered {
		total += sample
	}
	return LatencySummary{
		Samples: len(ordered),
		MinNS:   ordered[0].Nanoseconds(),
		P50NS:   percentile(ordered, 0.50).Nanoseconds(),
		P95NS:   percentile(ordered, 0.95).Nanoseconds(),
		P99NS:   percentile(ordered, 0.99).Nanoseconds(),
		MaxNS:   ordered[len(ordered)-1].Nanoseconds(),
		TotalNS: total.Nanoseconds(),
	}
}

func percentile(ordered []time.Duration, quantile float64) time.Duration {
	position := int(math.Ceil(quantile*float64(len(ordered)))) - 1
	return ordered[max(0, min(position, len(ordered)-1))]
}

func compactBounds(entries []Entry) ([]byte, []byte) {
	if len(entries) == 0 {
		return []byte{0x00}, []byte{0xff}
	}
	start := bytes.Clone(entries[0].Key)
	end := append(bytes.Clone(entries[len(entries)-1].Key), 0x00)
	return start, end
}

func directoryBytes(root string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += uint64(info.Size())
		}
		return nil
	})
	return total, err
}

func processCPUTime() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return timevalDuration(usage.Utime) + timevalDuration(usage.Stime), nil
}

func timevalDuration(value syscall.Timeval) time.Duration {
	return time.Duration(value.Sec)*time.Second + time.Duration(value.Usec)*time.Microsecond
}
