package bond

import (
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
	opts := &pebble.Options{
		FS:                          vfs.Default,
		Comparer:                    DefaultPebbleComparer(),
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    func() int { return DefaultMaxConcurrentCompactions },
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
	}

	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second

	opts.Experimental.MinDeletionRate = 128 << 20 // 128 MB
	opts.Experimental.MaxWriterConcurrency = DefaultMaxWriterConcurrency

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}

	return opts
}

func DefaultPebbleComparer() *pebble.Comparer {
	comparer := *pebble.DefaultComparer
	comparer.Split = _KeyPrefixSplitIndex
	return &comparer
}