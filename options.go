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
	var maxOpenFileLimit = 10000

	pCache := pebble.NewCache(128 << 20) // 128 MB
	defer func() {
		pCache.Unref()
	}()

	pTableCache := pebble.NewTableCache(pCache, runtime.GOMAXPROCS(0), maxOpenFileLimit)

	opts := &pebble.Options{
		Cache:                       pCache,
		TableCache:                  pTableCache,
		FS:                          vfs.Default,
		Comparer:                    DefaultKeyComparer(),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,     // TODO: why..?
		LBaseMaxBytes:               64 << 20, // 64 MB
		MaxOpenFiles:                maxOpenFileLimit,
		Levels:                      make([]pebble.LevelOptions, 7), // TODO: how many levels do we want..?
		MaxConcurrentCompactions:    func() int { return DefaultMaxConcurrentCompactions },
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
	}
	opts.EnsureDefaults()

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB
	opts.Experimental.MaxWriterConcurrency = DefaultMaxWriterConcurrency

	// opts.EventListener // TODO: listen on compactions ,etc.

	for i := range opts.Levels {
		l := &opts.Levels[i]
		l.EnsureDefaults()

		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.Compression = pebble.ZstdCompression
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}

	return opts
}
