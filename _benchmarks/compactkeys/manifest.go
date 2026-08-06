package compactkeys

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-bond/bond"
)

const ManifestVersion = 3

const approvedPebbleCommit = "8fb150d9135d6f94e183a874475e0bd1afb18f63"

const LogicalLayoutVersion = "bond/logical-key/pre-catalog"

type EngineSpec struct {
	Name         string                  `json:"name"`
	Performance  bond.PerformanceProfile `json:"performance"`
	Compression  bond.CompressionProfile `json:"compression"`
	TableFilter  bond.TableFilterProfile `json:"table_filter"`
	WriterSchema string                  `json:"writer_schema"`
	BundleSize   int                     `json:"bundle_size"`
}

type RunSpec struct {
	Warmups     int `json:"warmups"`
	Repetitions int `json:"repetitions"`
	Run         int `json:"run"`
	BatchSize   int `json:"batch_size"`
}

type Revisions struct {
	BondCommit        string `json:"bond_commit"`
	BondDirty         bool   `json:"bond_dirty"`
	SourceFingerprint string `json:"source_fingerprint"`
	PebbleVersion     string `json:"pebble_version"`
	PebbleCommit      string `json:"pebble_commit"`
	PebbleOriginURL   string `json:"pebble_origin_url"`
}

type RuntimeInfo struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	Compiler  string `json:"compiler"`
}

type MachineInfo struct {
	Hostname        string `json:"hostname"`
	Kernel          string `json:"kernel"`
	CPUModel        string `json:"cpu_model"`
	LogicalCPUs     int    `json:"logical_cpus"`
	MemoryBytes     uint64 `json:"memory_bytes"`
	FilesystemType  string `json:"filesystem_type"`
	FilesystemBytes uint64 `json:"filesystem_bytes"`
	FilesystemFree  uint64 `json:"filesystem_free_bytes"`
	FilesystemBlock uint64 `json:"filesystem_block_bytes"`
}

type LatencySummary struct {
	Samples int   `json:"samples"`
	MinNS   int64 `json:"min_ns"`
	P50NS   int64 `json:"p50_ns"`
	P95NS   int64 `json:"p95_ns"`
	P99NS   int64 `json:"p99_ns"`
	MaxNS   int64 `json:"max_ns"`
	TotalNS int64 `json:"total_ns"`
}

type LevelResult struct {
	Level     int    `json:"level"`
	Files     uint64 `json:"files"`
	Bytes     uint64 `json:"bytes"`
	Sublevels int32  `json:"sublevels"`
}

type SSTResults struct {
	Files                          int               `json:"files"`
	PhysicalBytes                  uint64            `json:"physical_bytes"`
	PropertyRawKeyBytes            uint64            `json:"property_raw_key_bytes"`
	PropertyRawValueBytes          uint64            `json:"property_raw_value_bytes"`
	PropertyDataBytes              uint64            `json:"property_data_bytes"`
	PropertyIndexUncompressedBytes uint64            `json:"property_index_uncompressed_bytes"`
	PropertyFilterBytes            uint64            `json:"property_filter_bytes"`
	PropertyValueBlockBytes        uint64            `json:"property_value_block_bytes"`
	CompressionProfiles            map[string]int    `json:"compression_profiles"`
	FilterFamilies                 map[string]int    `json:"filter_families"`
	KeySchemas                     map[string]int    `json:"key_schemas"`
	KeySchemaBytes                 map[string]uint64 `json:"key_schema_bytes"`
}

type FilterResults struct {
	AbsentProbes      int     `json:"absent_probes"`
	L6FiltersEnabled  bool    `json:"l6_filters_enabled"`
	UsefulNegatives   int64   `json:"useful_negatives"`
	FalsePositives    int64   `json:"false_positives"`
	ObservedChecks    int64   `json:"observed_checks"`
	FalsePositiveRate float64 `json:"false_positive_rate"`
}

type CacheResults struct {
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`
	Bytes  int64 `json:"bytes"`
}

type StorageMetricsSnapshot struct {
	PebbleCompactionDurationNS int64         `json:"pebble_compaction_duration_ns"`
	CompactionBytesRead        uint64        `json:"compaction_bytes_read"`
	CompactionBytesWritten     uint64        `json:"compaction_bytes_written"`
	WriteAmplification         float64       `json:"write_amplification"`
	WALBytesIn                 uint64        `json:"wal_bytes_in"`
	WALBytesWritten            uint64        `json:"wal_bytes_written"`
	MemtableBytes              uint64        `json:"memtable_bytes"`
	L0Sublevels                int32         `json:"l0_sublevels"`
	Levels                     []LevelResult `json:"levels"`
	Cache                      CacheResults  `json:"cache"`
}

type StorageSnapshot struct {
	Metrics StorageMetricsSnapshot `json:"metrics"`
	SST     SSTResults             `json:"sst"`
}

type CompactionDelta struct {
	PebbleDurationNS  int64  `json:"pebble_duration_ns"`
	TableBytesRead    uint64 `json:"table_bytes_read"`
	TableBytesWritten uint64 `json:"table_bytes_written"`
	BlobBytesRead     uint64 `json:"blob_bytes_read"`
	BlobBytesWritten  uint64 `json:"blob_bytes_written"`
}

type ValueStoragePolicyManifest struct {
	DisableSeparationBySuffix         bool `json:"disable_separation_by_suffix"`
	DisableBlobSeparation             bool `json:"disable_blob_separation"`
	OverrideBlobSeparationMinimumSize int  `json:"override_blob_separation_minimum_size"`
	MinimumMVCCGarbageSize            int  `json:"minimum_mvcc_garbage_size"`
}

type SpanPolicyManifest struct {
	KeyRangeStart         string                     `json:"key_range_start_hex"`
	KeyRangeEnd           string                     `json:"key_range_end_hex"`
	PreferFastCompression bool                       `json:"prefer_fast_compression"`
	ValueStorage          ValueStoragePolicyManifest `json:"value_storage"`
	TieringPolicyIsSet    bool                       `json:"tiering_policy_is_set"`
}

type ValueSeparationManifest struct {
	Enabled                  bool    `json:"enabled"`
	MinimumSize              int     `json:"minimum_size"`
	MinimumMVCCGarbageSize   int     `json:"minimum_mvcc_garbage_size"`
	MaxBlobReferenceDepth    int     `json:"max_blob_reference_depth"`
	RewriteMinimumAgeNS      int64   `json:"rewrite_minimum_age_ns"`
	GarbageRatioLowPriority  float64 `json:"garbage_ratio_low_priority"`
	GarbageRatioHighPriority float64 `json:"garbage_ratio_high_priority"`
}

type StoragePolicyProvenance struct {
	SpanPolicy         SpanPolicyManifest      `json:"span_policy"`
	ValueSeparation    ValueSeparationManifest `json:"value_separation"`
	ValueBlocksEnabled bool                    `json:"value_blocks_enabled"`
}

type SchemaProvenance struct {
	ComparerName      string   `json:"comparer_name"`
	ActiveWriter      string   `json:"active_writer"`
	RegisteredReaders []string `json:"registered_readers"`
	ActiveBundleSize  int      `json:"active_bundle_size"`
}

type LifecycleResults struct {
	Rows                   int             `json:"rows"`
	InitialEntries         int             `json:"initial_entries"`
	FinalEntries           int             `json:"final_entries"`
	InitialIndexEntries    int             `json:"initial_index_entries"`
	FinalIndexEntries      int             `json:"final_index_entries"`
	DatabaseBytes          uint64          `json:"database_bytes"`
	BytesPerRow            float64         `json:"bytes_per_row"`
	BytesPerIndexEntry     float64         `json:"bytes_per_index_entry"`
	LoadLatency            LatencySummary  `json:"load_latency"`
	MutationLatency        LatencySummary  `json:"mutation_latency"`
	HitLatency             LatencySummary  `json:"hit_latency"`
	PointMissLatency       LatencySummary  `json:"point_miss_latency"`
	FilterProbeLatency     LatencySummary  `json:"filter_probe_latency"`
	FlushNS                int64           `json:"flush_ns"`
	CompactionNS           int64           `json:"compaction_ns"`
	CheckpointNS           int64           `json:"checkpoint_ns"`
	CheckpointOpenNS       int64           `json:"checkpoint_open_ns"`
	ReopenNS               int64           `json:"reopen_ns"`
	ProcessCompactionCPUNS int64           `json:"process_compaction_cpu_ns"`
	AfterFlush             StorageSnapshot `json:"after_flush"`
	AfterCompaction        StorageSnapshot `json:"after_compaction"`
	CompactionDelta        CompactionDelta `json:"compaction_delta"`
	Filter                 FilterResults   `json:"filter"`
	PostProbeCache         CacheResults    `json:"post_probe_cache"`
}

type RunManifest struct {
	ManifestVersion  int                       `json:"manifest_version"`
	CapturedAt       time.Time                 `json:"captured_at"`
	Revisions        Revisions                 `json:"revisions"`
	Runtime          RuntimeInfo               `json:"runtime"`
	Machine          MachineInfo               `json:"machine"`
	Engine           EngineSpec                `json:"engine"`
	Run              RunSpec                   `json:"run"`
	Dataset          DatasetSpec               `json:"dataset"`
	DatasetDigest    string                    `json:"dataset_digest"`
	LogicalLayout    string                    `json:"logical_layout"`
	FormatMajor      uint64                    `json:"format_major"`
	ActiveKeySchema  string                    `json:"active_key_schema"`
	StoragePolicy    StoragePolicyProvenance   `json:"storage_policy"`
	Schema           SchemaProvenance          `json:"schema"`
	Compatibility    bond.StorageCompatibility `json:"storage_compatibility"`
	EffectiveOptions string                    `json:"effective_pebble_options"`
	Results          LifecycleResults          `json:"results"`
}

func (m RunManifest) JSON() ([]byte, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (m RunManifest) Write(path string) error {
	data, err := m.JSON()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func collectRevisions(repoRoot string) (Revisions, error) {
	bondCommit, err := commandOutput(repoRoot, "git", "rev-parse", "HEAD")
	if err != nil {
		return Revisions{}, err
	}
	status, err := commandOutput(repoRoot, "git", "status", "--porcelain")
	if err != nil {
		return Revisions{}, err
	}
	sourceFingerprint, err := fingerprintSources(repoRoot)
	if err != nil {
		return Revisions{}, err
	}

	type module struct {
		Version string
		Origin  struct {
			URL  string
			Hash string
		}
	}
	moduleJSON, err := commandOutput(repoRoot, "go", "list", "-m", "-json", "github.com/cockroachdb/pebble")
	if err != nil {
		return Revisions{}, err
	}
	var selectedModule module
	if err := json.Unmarshal([]byte(moduleJSON), &selectedModule); err != nil {
		return Revisions{}, fmt.Errorf("decode Pebble module: %w", err)
	}
	resolvedJSON, err := commandOutput(repoRoot, "go", "list", "-m", "-json", "github.com/cockroachdb/pebble@"+approvedPebbleCommit)
	if err != nil {
		return Revisions{}, err
	}
	var resolvedModule module
	if err := json.Unmarshal([]byte(resolvedJSON), &resolvedModule); err != nil {
		return Revisions{}, fmt.Errorf("decode resolved Pebble module: %w", err)
	}
	if selectedModule.Version != resolvedModule.Version || resolvedModule.Origin.Hash != approvedPebbleCommit {
		return Revisions{}, fmt.Errorf("Pebble module is not the approved commit: selected=%s resolved=%s origin=%s", selectedModule.Version, resolvedModule.Version, resolvedModule.Origin.Hash)
	}
	return Revisions{
		BondCommit:        bondCommit,
		BondDirty:         status != "",
		SourceFingerprint: sourceFingerprint,
		PebbleVersion:     selectedModule.Version,
		PebbleCommit:      resolvedModule.Origin.Hash,
		PebbleOriginURL:   resolvedModule.Origin.URL,
	}, nil
}

func collectMachine(storageRoot string) (MachineInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return MachineInfo{}, err
	}
	kernel, _ := commandOutput("", "uname", "-sr")
	cpuModel := firstProcValue("/proc/cpuinfo", "model name")
	memoryKB, _ := strconv.ParseUint(firstProcValue("/proc/meminfo", "MemTotal"), 10, 64)

	var stat syscall.Statfs_t
	if err := syscall.Statfs(storageRoot, &stat); err != nil {
		return MachineInfo{}, fmt.Errorf("stat filesystem %s: %w", storageRoot, err)
	}
	return MachineInfo{
		Hostname:        hostname,
		Kernel:          kernel,
		CPUModel:        cpuModel,
		LogicalCPUs:     runtime.NumCPU(),
		MemoryBytes:     memoryKB * 1024,
		FilesystemType:  fmt.Sprintf("0x%x", stat.Type),
		FilesystemBytes: uint64(stat.Blocks) * uint64(stat.Bsize),
		FilesystemFree:  uint64(stat.Bavail) * uint64(stat.Bsize),
		FilesystemBlock: uint64(stat.Bsize),
	}, nil
}

var fingerprintSourcePaths = []string{
	"go.mod",
	"go.sum",
	"internal/fullkeyexperiment",
	"keys.go",
	"options.go",
	"storage_compatibility.go",
	"_benchmarks/compactkeys",
}

func fingerprintSources(repoRoot string) (string, error) {
	var files []string
	for _, relative := range fingerprintSourcePaths {
		absolute := filepath.Join(repoRoot, relative)
		info, err := os.Stat(absolute)
		if err != nil {
			return "", err
		}
		if !info.IsDir() {
			files = append(files, absolute)
			continue
		}
		if err := filepath.WalkDir(absolute, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
				files = append(files, path)
			}
			return nil
		}); err != nil {
			return "", err
		}
	}
	sort.Strings(files)
	hash := sha256.New()
	for _, path := range files {
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return "", err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		hash.Write([]byte(relative))
		hash.Write([]byte{0})
		hash.Write(data)
		hash.Write([]byte{0xff})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func commandOutput(dir, name string, args ...string) (string, error) {
	command := exec.Command(name, args...)
	if dir != "" {
		command.Dir = dir
	}
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output)), nil
}

func firstProcValue(path, key string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(name) != key {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			return ""
		}
		if key == "MemTotal" {
			return fields[0]
		}
		return strings.TrimSpace(value)
	}
	return ""
}
