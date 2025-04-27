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
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-bond/bond/serializers"
)

const DefaultMaxConcurrentCompactions = 4
const DefaultMaxWriterConcurrency = 8

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
		CacheSize:                   32 << 20, // 32 MB
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       500,
		LBaseMaxBytes:               16 << 20, // 16 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    func() int { return max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) },
		MemTableSize:                8 << 20, // 8 MB
		MemTableStopWritesThreshold: 2,
		BytesPerSync:                512 << 10, // 512 KB
	}

	opts.EnsureDefaults()

	opts.WALMinSyncInterval = func() time.Duration {
		return 250 * time.Millisecond
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 64 << 20 // 64 MB

	opts.MaxConcurrentCompactions = func() int { return 6 }

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 512 << 20 // 512 MB

	// max writer is used for compression concurrency
	opts.Experimental.ReadSamplingMultiplier = -1

	// disable multi-level compaction, see https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}

	opts.Experimental.EnableColumnarBlocks = func() bool {
		return true
	}

	opts.FormatMajorVersion = pebble.FormatTableFormatV6

	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.EnsureDefaults()

		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB

		// compression
		if i <= 1 {
			l.Compression = func() pebble.Compression {
				return pebble.NoCompression
			}
		} else {
			l.Compression = func() pebble.Compression {
				return pebble.ZstdCompression
			}
		}

		// l.Compression = func() pebble.Compression {
		// 	return pebble.ZstdCompression
		// }

		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			// L0 is 2MB, and grows from there
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}

	return opts
}

func MediumPerformancePebbleOptions() *pebble.Options {
	maxOpenFileLimit := min(getMaxOpenFileLimit(slog.Default()), defaultMaxOpenFiles)

	opts := &pebble.Options{
		CacheSize:                   128 << 20, // 128 MB
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    func() int { return max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) },
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
		BytesPerSync:                1024 << 10, // 1024 KB
	}

	opts.EnsureDefaults()

	opts.WALMinSyncInterval = func() time.Duration {
		return 250 * time.Millisecond
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB

	opts.MaxConcurrentCompactions = func() int { return 6 }

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB

	// max writer is used for compression concurrency
	opts.Experimental.ReadSamplingMultiplier = -1

	// disable multi-level compaction, see https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}

	opts.Experimental.EnableColumnarBlocks = func() bool {
		return true
	}

	opts.FormatMajorVersion = pebble.FormatTableFormatV6

	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.EnsureDefaults()

		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB

		// compression
		if i <= 1 {
			l.Compression = func() pebble.Compression {
				return pebble.NoCompression
			}
		} else {
			l.Compression = func() pebble.Compression {
				return pebble.ZstdCompression
			}
		}

		// l.Compression = func() pebble.Compression {
		// 	return pebble.ZstdCompression
		// }

		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			// L0 is 2MB, and grows from there
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}

	return opts
}

func HighPerformancePebbleOptions() *pebble.Options {
	maxOpenFileLimit := getMaxOpenFileLimit(slog.Default())

	opts := &pebble.Options{
		CacheSize:                   512 << 20, // 512 MB
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               128 << 20, // 128 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    func() int { return max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) },
		MemTableSize:                128 << 20, // 128 MB
		MemTableStopWritesThreshold: 4,
		BytesPerSync:                4024 << 10, // 4024 KB
	}

	opts.EnsureDefaults()

	opts.WALMinSyncInterval = func() time.Duration {
		return 250 * time.Millisecond
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB

	// opts.MaxConcurrentCompactions = func() int { return 8 }
	opts.MaxConcurrentCompactions = func() int { return 6 }

	opts.Experimental.L0CompactionConcurrency = 2
	opts.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB

	// max writer is used for compression concurrency
	// opts.Experimental.MaxWriterConcurrency = max(DefaultMaxWriterConcurrency, runtime.NumCPU())
	opts.Experimental.ReadSamplingMultiplier = -1

	// disable multi-level compaction, see https://github.com/cockroachdb/pebble/issues/4139
	opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}

	opts.Experimental.EnableColumnarBlocks = func() bool {
		return true
	}

	opts.FormatMajorVersion = pebble.FormatTableFormatV6

	// TODO, collect these stats
	// opts.EventListener = &pebble.EventListener{
	// 	CompactionBegin: db.onCompactionBegin,
	// 	CompactionEnd:   db.onCompactionEnd,
	// 	WriteStallBegin: db.onWriteStallBegin,
	// 	WriteStallEnd:   db.onWriteStallEnd,
	// }

	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.EnsureDefaults()

		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB

		// compression
		if i <= 1 {
			l.Compression = func() pebble.Compression {
				return pebble.NoCompression
			}
		} else {
			l.Compression = func() pebble.Compression {
				return pebble.ZstdCompression
			}
		}

		// l.Compression = func() pebble.Compression {
		// 	return pebble.ZstdCompression
		// }

		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			// L0 is 2MB, and grows from there
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}

	return opts
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
