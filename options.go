package bond

import (
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-bond/bond/serializers"
)

const PebbleDBFormat = pebble.FormatNewest

const DefaultMaxConcurrentCompactions = 8

type Options struct {
	PebbleOptions *pebble.Options
	Serializer    Serializer[any]
}

func DefaultOptions(performanceProfile ...PerformanceProfile) *Options {
	opts := Options{
		Serializer: &serializers.CBORSerializer{},
	}
	if opts.PebbleOptions == nil {
		opts.PebbleOptions = DefaultPebbleOptions(performanceProfile...)
	}
	return &opts
}

// PerformanceProfile is a profile for the performance of the database
// depending on the intensity of the workload. Default is MediumPerformance.
type PerformanceProfile int

const (
	LowPerformance PerformanceProfile = iota
	MediumPerformance
	HighPerformance
)

// CompressionProfile identifies an attributable store-wide compression
// candidate. The legacy profile is Bond's pre-Phase 2 configured level layout;
// the other profiles are Pebble's definitions at the pinned module revision.
type CompressionProfile string

const (
	CompressionLegacy   CompressionProfile = "legacy"
	CompressionBalanced CompressionProfile = "balanced"
	CompressionGood     CompressionProfile = "good"
)

// TableFilterProfile identifies an attributable store-wide table-filter
// candidate. The progressive profiles deliberately delegate their per-level
// choices to Pebble so Bond does not silently drift from the pinned engine.
type TableFilterProfile string

const (
	TableFilterUniformBloom          TableFilterProfile = "uniform-bloom"
	TableFilterProgressiveBloom      TableFilterProfile = "progressive-bloom"
	TableFilterProgressiveBinaryFuse TableFilterProfile = "progressive-binary-fuse"
)

// The Phase 2 non-schema baseline. Keep these names in benchmark manifests so
// later schema experiments change only the active KeySchema.
const (
	DefaultCompressionProfile = CompressionLegacy
	DefaultTableFilterProfile = TableFilterUniformBloom
)

// PebbleOptionsConfig separates resource sizing from storage-policy
// experiments. Zero values intentionally select the Phase 2 baseline.
type PebbleOptionsConfig struct {
	Performance PerformanceProfile
	Compression CompressionProfile
	TableFilter TableFilterProfile
}

func DefaultPebbleOptions(performanceProfile ...PerformanceProfile) *pebble.Options {
	profile := MediumPerformance
	if len(performanceProfile) > 0 {
		switch performanceProfile[0] {
		case LowPerformance:
			profile = LowPerformance
		case MediumPerformance:
			profile = MediumPerformance
		case HighPerformance:
			profile = HighPerformance
		}
	}
	return BuildPebbleOptions(profile)
}

func LowPerformancePebbleOptions() *pebble.Options {
	return BuildPebbleOptions(LowPerformance)
}

func MediumPerformancePebbleOptions() *pebble.Options {
	return BuildPebbleOptions(MediumPerformance)
}

func HighPerformancePebbleOptions() *pebble.Options {
	return BuildPebbleOptions(HighPerformance)
}

type pebbleProfileSettings struct {
	cacheSize                   int64
	l0CompactionFileThreshold   int
	l0CompactionThreshold       int
	l0StopWritesThreshold       int
	lBaseMaxBytes               int64
	maxOpenFiles                int
	memTableSize                uint64
	memTableStopWritesThreshold int
	bytesPerSync                int
	walMinSyncInterval          time.Duration
	l0CompactionConcurrency     int
	blockSize                   int
	targetFileSize              int64
}

// BuildPebbleOptions applies Bond's common storage correctness settings and
// then the resource sizing for a single performance profile.
func BuildPebbleOptions(profile PerformanceProfile) *pebble.Options {
	opts, err := BuildPebbleOptionsWithConfig(PebbleOptionsConfig{Performance: profile})
	if err != nil {
		panic(err)
	}
	return opts
}

// BuildPebbleOptionsWithConfig builds an attributable Pebble configuration.
// It retains Bond's comparer, format, schema registry, and value policy across
// every compression and table-filter candidate.
func BuildPebbleOptionsWithConfig(config PebbleOptionsConfig) (*pebble.Options, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	settings := settingsForPerformanceProfile(config.Performance)
	opts := &pebble.Options{
		CacheSize:                   settings.cacheSize,
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		FormatMajorVersion:          PebbleDBFormat,
		L0CompactionFileThreshold:   settings.l0CompactionFileThreshold,
		L0CompactionThreshold:       settings.l0CompactionThreshold,
		L0StopWritesThreshold:       settings.l0StopWritesThreshold,
		LBaseMaxBytes:               settings.lBaseMaxBytes,
		MaxOpenFiles:                settings.maxOpenFiles,
		Levels:                      [7]pebble.LevelOptions{},
		MemTableSize:                settings.memTableSize,
		MemTableStopWritesThreshold: settings.memTableStopWritesThreshold,
		BytesPerSync:                settings.bytesPerSync,
		L0CompactionConcurrency:     settings.l0CompactionConcurrency,
		CompactionDebtConcurrency:   1 << 30,
		ReadSamplingMultiplier:      -1,
	}

	opts.WALMinSyncInterval = func() time.Duration {
		return settings.walMinSyncInterval
	}
	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.CompactionConcurrencyRange = func() (int, int) { return 1, max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) }
	opts.MaxConcurrentDownloads = func() int { return 2 }
	opts.EnableValueBlocks = func() bool { return true }
	opts.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:                  true,
			MinimumSize:              64,
			MinimumMVCCGarbageSize:   64,
			MaxBlobReferenceDepth:    10,
			RewriteMinimumAge:        60 * time.Second,
			GarbageRatioLowPriority:  0.10,
			GarbageRatioHighPriority: 0.20,
		}
	}

	// Disabled pending resolution of https://github.com/cockroachdb/pebble/issues/4139.
	opts.MultiLevelCompactionHeuristic = func() pebble.MultiLevelHeuristic {
		return pebble.NoMultiLevel{}
	}
	opts.SpanPolicyFunc = spanPolicyFunc

	opts.Levels[0] = pebble.LevelOptions{
		BlockSize:      settings.blockSize,
		IndexBlockSize: 256 << 10,
	}
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
	}
	applyCompressionProfile(opts, config.Compression)
	applyTableFilterProfile(opts, config.TableFilter)
	opts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.EnsureL1PlusDefaults(&opts.Levels[i-1])
	}

	opts.TargetFileSizes[0] = settings.targetFileSize
	opts.EnsureDefaults()
	return opts, nil
}

func (c PebbleOptionsConfig) withDefaults() PebbleOptionsConfig {
	if c.Compression == "" {
		c.Compression = DefaultCompressionProfile
	}
	if c.TableFilter == "" {
		c.TableFilter = DefaultTableFilterProfile
	}
	return c
}

func (c PebbleOptionsConfig) validate() error {
	if c.Performance < LowPerformance || c.Performance > HighPerformance {
		return fmt.Errorf("bond: unknown performance profile %d", c.Performance)
	}
	switch c.Compression {
	case CompressionLegacy, CompressionBalanced, CompressionGood:
	default:
		return fmt.Errorf("bond: unknown compression profile %q", c.Compression)
	}
	switch c.TableFilter {
	case TableFilterUniformBloom, TableFilterProgressiveBloom, TableFilterProgressiveBinaryFuse:
	default:
		return fmt.Errorf("bond: unknown table-filter profile %q", c.TableFilter)
	}
	return nil
}

func applyCompressionProfile(opts *pebble.Options, profile CompressionProfile) {
	switch profile {
	case CompressionLegacy:
		opts.Levels[0].Compression = func() *sstable.CompressionProfile {
			return sstable.SnappyCompression
		}
		for i := 1; i < len(opts.Levels); i++ {
			if i <= 1 {
				opts.Levels[i].Compression = func() *sstable.CompressionProfile {
					return sstable.SnappyCompression
				}
			} else {
				opts.Levels[i].Compression = func() *sstable.CompressionProfile {
					return sstable.ZstdCompression
				}
			}
		}
	case CompressionBalanced:
		opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
			return pebble.DBCompressionBalanced
		})
	case CompressionGood:
		opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
			return pebble.DBCompressionGood
		})
	}
}

func applyTableFilterProfile(opts *pebble.Options, profile TableFilterProfile) {
	switch profile {
	case TableFilterUniformBloom:
		opts.ApplyTableFilterPolicy(func() pebble.DBTableFilterPolicy {
			return pebble.DBTableFilterPolicyUniform
		})
	case TableFilterProgressiveBloom:
		opts.ApplyTableFilterPolicy(func() pebble.DBTableFilterPolicy {
			return pebble.DBTableFilterPolicyProgressive
		})
	case TableFilterProgressiveBinaryFuse:
		opts.ApplyTableFilterPolicy(func() pebble.DBTableFilterPolicy {
			return pebble.DBTableFilterPolicyBinaryFuseProgressive
		})
	}
}

func settingsForPerformanceProfile(profile PerformanceProfile) pebbleProfileSettings {
	maxOpenFiles := getMaxOpenFileLimit(slog.Default())
	switch profile {
	case LowPerformance:
		return pebbleProfileSettings{
			cacheSize:                   128 << 20,
			l0CompactionFileThreshold:   500,
			l0CompactionThreshold:       4,
			l0StopWritesThreshold:       500,
			lBaseMaxBytes:               64 << 20,
			maxOpenFiles:                min(maxOpenFiles, 2048),
			memTableSize:                64 << 20,
			memTableStopWritesThreshold: 2,
			bytesPerSync:                1024 << 10,
			walMinSyncInterval:          200 * time.Millisecond,
			l0CompactionConcurrency:     2,
			blockSize:                   32 << 10,
			targetFileSize:              2 << 20,
		}
	case HighPerformance:
		return pebbleProfileSettings{
			cacheSize:                   1024 << 20,
			l0CompactionFileThreshold:   1000,
			l0CompactionThreshold:       8,
			l0StopWritesThreshold:       2000,
			lBaseMaxBytes:               1024 << 20,
			maxOpenFiles:                maxOpenFiles,
			memTableSize:                256 << 20,
			memTableStopWritesThreshold: 8,
			bytesPerSync:                4096 << 10,
			walMinSyncInterval:          150 * time.Millisecond,
			l0CompactionConcurrency:     4,
			blockSize:                   64 << 10,
			targetFileSize:              4 << 20,
		}
	default:
		return pebbleProfileSettings{
			cacheSize:                   256 << 20,
			l0CompactionFileThreshold:   500,
			l0CompactionThreshold:       4,
			l0StopWritesThreshold:       1000,
			lBaseMaxBytes:               256 << 20,
			maxOpenFiles:                min(maxOpenFiles, defaultMaxOpenFiles),
			memTableSize:                128 << 20,
			memTableStopWritesThreshold: 4,
			bytesPerSync:                4096 << 10,
			walMinSyncInterval:          200 * time.Millisecond,
			l0CompactionConcurrency:     2,
			blockSize:                   32 << 10,
			targetFileSize:              2 << 20,
		}
	}
}

func spanPolicyFunc(_ pebble.UserKeyBounds) (pebble.SpanPolicy, error) {
	return pebble.SpanPolicy{
		ValueStoragePolicy: pebble.ValueStorageLowReadLatency,
	}, nil
}

func ToPerformanceProfile(performanceProfile string) PerformanceProfile {
	switch strings.ToLower(performanceProfile) {
	case "low":
		return LowPerformance
	case "medium":
		return MediumPerformance
	case "high":
		return HighPerformance
	}
	return MediumPerformance
}

func (p PerformanceProfile) String() string {
	return []string{"low", "medium", "high"}[p]
}

// defaultMaxOpenFiles is a fallback value if we can't get the system limit.
const defaultMaxOpenFiles = 5000

// getMaxOpenFileLimit attempts to get the system's soft limit for open files (RLIMIT_NOFILE)
// and returns 85% of that value. It falls back to defaultMaxOpenFiles if the limit
// cannot be determined. Max is 10_000.
func getMaxOpenFileLimit(log *slog.Logger) int {
	var rlim syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim)
	if err != nil {
		log.Warn(fmt.Sprintf("Failed to get RLIMIT_NOFILE: %v. Falling back to default %d", err, defaultMaxOpenFiles))
		return defaultMaxOpenFiles
	}

	// Calculate 85% of the current soft limit (rlim.Cur) or 10_000 max.
	limit := min(int(float64(rlim.Cur)*0.85), 10_000)

	// Ensure we don't return a limit less than a minimum reasonable value (e.g., 1024)
	// Pebble might have its own internal minimums too.
	const minReasonableLimit = 1024
	if limit < minReasonableLimit {
		log.Warn(fmt.Sprintf("Calculated file descriptor limit (%d) is below minimum reasonable %d. Using %d.", limit, minReasonableLimit, minReasonableLimit))
		return minReasonableLimit
	}

	log.Debug(fmt.Sprintf("System RLIMIT_NOFILE is %d. Using %d for MaxOpenFiles.", rlim.Cur, limit))
	return limit
}
