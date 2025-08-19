package bond

import (
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-bond/bond/serializers"
)

const PebbleDBFormat = pebble.FormatV2BlobFiles

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

func DefaultPebbleOptions(performanceProfile ...PerformanceProfile) *pebble.Options {
	if len(performanceProfile) > 0 {
		switch performanceProfile[0] {
		case LowPerformance:
			return LowPerformancePebbleOptions()
		case MediumPerformance:
			return MediumPerformancePebbleOptions()
		case HighPerformance:
			return HighPerformancePebbleOptions()
		}
	}
	return MediumPerformancePebbleOptions()
}

func LowPerformancePebbleOptions() *pebble.Options {
	maxOpenFileLimit := min(getMaxOpenFileLimit(slog.Default()), 2048)

	opts := &pebble.Options{
		CacheSize:                   128 << 20, // 128 MB
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionFileThreshold:   500, // default in pebble
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       500,
		LBaseMaxBytes:               64 << 20, // 64 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      [7]pebble.LevelOptions{},
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 2,
		BytesPerSync:                1024 << 10, // 1024 KB
	}

	opts.FormatMajorVersion = PebbleDBFormat

	opts.WALMinSyncInterval = func() time.Duration {
		return 200 * time.Millisecond
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 64 << 20 // 64 MB

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 512 << 20 // 512 MB

	opts.CompactionConcurrencyRange = func() (int, int) { return 1, max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) }
	opts.MaxConcurrentDownloads = func() int { return 2 }

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB

	// opts.Experimental.EnableValueBlocks = func() bool { return false }
	opts.Experimental.EnableValueBlocks = func() bool { return true }
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           64,
			MaxBlobReferenceDepth: 10,
			RewriteMinimumAge:     60 * time.Second,
			TargetGarbageRatio:    0.20,
		}
	}

	opts.Experimental.ReadSamplingMultiplier = -1

	// disable multi-level compaction, see https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = func() pebble.MultiLevelHeuristic {
		return pebble.NoMultiLevel{}
	}

	opts.Experimental.SpanPolicyFunc = spanPolicyFunc

	opts.Levels[0] = pebble.LevelOptions{
		BlockSize:      32 << 10,  // 32 KB
		IndexBlockSize: 256 << 10, // 256 KB
		FilterPolicy:   bloom.FilterPolicy(10),
		FilterType:     pebble.TableFilter,
		Compression: func() *sstable.CompressionProfile {
			return sstable.SnappyCompression
		},
	}
	opts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i <= 1 {
			l.Compression = func() *sstable.CompressionProfile {
				return sstable.SnappyCompression
			}
		} else {
			l.Compression = func() *sstable.CompressionProfile {
				return sstable.ZstdCompression
			}
		}
		l.EnsureL1PlusDefaults(&opts.Levels[i-1])
	}

	opts.TargetFileSizes[0] = 2 << 20 // 2 MB
	// opts.TargetFileSizes[0] = 4 << 20   // 4 MB

	opts.EnsureDefaults()

	return opts
}

func MediumPerformancePebbleOptions() *pebble.Options {
	maxOpenFileLimit := min(getMaxOpenFileLimit(slog.Default()), defaultMaxOpenFiles)

	opts := &pebble.Options{
		CacheSize:                   256 << 20, // 256 MB
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionFileThreshold:   500,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               256 << 20, // 256 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      [7]pebble.LevelOptions{},
		MemTableSize:                128 << 20, // 128 MB
		MemTableStopWritesThreshold: 4,
		BytesPerSync:                4096 << 10, // 4096 KB
	}

	opts.FormatMajorVersion = PebbleDBFormat

	opts.WALMinSyncInterval = func() time.Duration {
		return 200 * time.Millisecond
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB

	opts.CompactionConcurrencyRange = func() (int, int) { return 1, max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) }
	opts.MaxConcurrentDownloads = func() int { return 2 }

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB

	// opts.Experimental.EnableValueBlocks = func() bool { return false }
	opts.Experimental.EnableValueBlocks = func() bool { return true }
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           64,
			MaxBlobReferenceDepth: 10,
			RewriteMinimumAge:     60 * time.Second,
			TargetGarbageRatio:    0.20,
		}
	}

	opts.Experimental.ReadSamplingMultiplier = -1

	// disable multi-level compaction, see https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = func() pebble.MultiLevelHeuristic {
		return pebble.NoMultiLevel{}
	}

	opts.Experimental.SpanPolicyFunc = spanPolicyFunc

	opts.Levels[0] = pebble.LevelOptions{
		BlockSize:      32 << 10,  // 32 KB
		IndexBlockSize: 256 << 10, // 256 KB
		FilterPolicy:   bloom.FilterPolicy(10),
		FilterType:     pebble.TableFilter,
		Compression: func() *sstable.CompressionProfile {
			return sstable.SnappyCompression
		},
	}
	opts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i <= 1 {
			l.Compression = func() *sstable.CompressionProfile {
				return sstable.SnappyCompression
			}
		} else {
			l.Compression = func() *sstable.CompressionProfile {
				return sstable.ZstdCompression
			}
		}
		l.EnsureL1PlusDefaults(&opts.Levels[i-1])
	}

	opts.TargetFileSizes[0] = 2 << 20 // 2 MB
	// opts.TargetFileSizes[0] = 4 << 20   // 4 MB

	opts.EnsureDefaults()

	return opts
}

func HighPerformancePebbleOptions() *pebble.Options {
	maxOpenFileLimit := getMaxOpenFileLimit(slog.Default())

	opts := &pebble.Options{
		CacheSize:                   1024 << 20, // 1024 MB
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionFileThreshold:   1000,
		L0CompactionThreshold:       8,
		L0StopWritesThreshold:       2000,
		LBaseMaxBytes:               1024 << 20, // 1024 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      [7]pebble.LevelOptions{},
		MemTableSize:                256 << 20, // 256 MB
		MemTableStopWritesThreshold: 8,
		BytesPerSync:                8192 << 10, // 8192 KB
	}

	opts.FormatMajorVersion = PebbleDBFormat

	opts.WALMinSyncInterval = func() time.Duration {
		return 150 * time.Millisecond
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 256 << 20 // 256 MB

	opts.CompactionConcurrencyRange = func() (int, int) { return 1, max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) }
	opts.MaxConcurrentDownloads = func() int { return 2 }

	opts.Experimental.L0CompactionConcurrency = 4
	opts.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB

	// opts.Experimental.EnableValueBlocks = func() bool { return false }
	opts.Experimental.EnableValueBlocks = func() bool { return true }
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           64,
			MaxBlobReferenceDepth: 10,
			RewriteMinimumAge:     60 * time.Second,
			TargetGarbageRatio:    0.20,
		}
	}

	opts.Experimental.ReadSamplingMultiplier = -1

	// disable multi-level compaction, see https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = func() pebble.MultiLevelHeuristic {
		return pebble.NoMultiLevel{}
	}

	opts.Experimental.SpanPolicyFunc = spanPolicyFunc

	// TODO, collect these stats
	// opts.EventListener = &pebble.EventListener{
	// 	CompactionBegin: db.onCompactionBegin,
	// 	CompactionEnd:   db.onCompactionEnd,
	// 	WriteStallBegin: db.onWriteStallBegin,
	// 	WriteStallEnd:   db.onWriteStallEnd,
	// }

	opts.Levels[0] = pebble.LevelOptions{
		BlockSize:      64 << 10,  // 64 KB
		IndexBlockSize: 256 << 10, // 256 KB
		FilterPolicy:   bloom.FilterPolicy(10),
		FilterType:     pebble.TableFilter,
		Compression: func() *sstable.CompressionProfile {
			return sstable.SnappyCompression
		},
	}
	opts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i <= 1 {
			l.Compression = func() *sstable.CompressionProfile {
				return sstable.SnappyCompression
			}
		} else {
			l.Compression = func() *sstable.CompressionProfile {
				return sstable.ZstdCompression
			}
		}
		l.EnsureL1PlusDefaults(&opts.Levels[i-1])
	}

	// opts.TargetFileSizes[0] = 2 << 20 // 2 MB
	opts.TargetFileSizes[0] = 4 << 20 // 4 MB

	opts.EnsureDefaults()

	return opts
}

func spanPolicyFunc(startKey []byte) (policy pebble.SpanPolicy, endKey []byte, err error) {
	policy.PreferFastCompression = true
	policy.DisableValueSeparationBySuffix = true
	policy.ValueStoragePolicy = pebble.ValueStorageLowReadLatency
	return
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
