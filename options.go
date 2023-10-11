package bond

import (
	"runtime"
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

	Serializer Serializer[any]
}

func DefaultOptions() *Options {
	opts := Options{
		Serializer: &serializers.CBORSerializer{},
	}

	if opts.PebbleOptions == nil {
		opts.PebbleOptions = DefaultPebbleOptions()
	}

	return &opts
}

func DefaultPebbleOptions() *pebble.Options {
	// TODO: query this with syscall to get file descriptor limit,
	// and set the value to 80%. We should also record this value on /status
	var maxOpenFileLimit = 5_000

	pCache := pebble.NewCache(256 << 20) // 256 MB
	defer pCache.Unref()

	pTableCache := pebble.NewTableCache(pCache, runtime.GOMAXPROCS(0), maxOpenFileLimit)

	opts := &pebble.Options{
		Cache:                       pCache,
		TableCache:                  pTableCache,
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    func() int { return max(DefaultMaxConcurrentCompactions, runtime.NumCPU()) },
		MemTableSize:                128 << 20, // 128 MB
		MemTableStopWritesThreshold: 4,
		Merger:                      defaultMerger.Merger,
	}
	opts.EnsureDefaults()

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB

	opts.Experimental.MaxWriterConcurrency = max(DefaultMaxWriterConcurrency, runtime.NumCPU())
	opts.Experimental.ReadSamplingMultiplier = -1

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

		// TODO/NOTE: benchmarks fail with background error whenever
		// using zstd compression
		// l.Compression = pebble.ZstdCompression

		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			// L0 is 2MB, and grows from there
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}

	return opts
}
