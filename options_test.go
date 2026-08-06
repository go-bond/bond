package bond

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/stretchr/testify/require"
)

const (
	approvedPebbleCommit  = "8fb150d9135d6f94e183a874475e0bd1afb18f63"
	approvedPebbleVersion = "v0.0.0-20260707124150-8fb150d9135d"
)

func TestBuildPebbleOptionsProfiles(t *testing.T) {
	testCases := []struct {
		name                    string
		profile                 PerformanceProfile
		cacheSize               int64
		memTableSize            uint64
		memTableStopThreshold   int
		l0CompactionThreshold   int
		l0StopWritesThreshold   int
		l0CompactionConcurrency int
		l0BlockSize             int
		targetFileSize          int64
		walMinSyncInterval      time.Duration
	}{
		{
			name:                    "low",
			profile:                 LowPerformance,
			cacheSize:               128 << 20,
			memTableSize:            64 << 20,
			memTableStopThreshold:   2,
			l0CompactionThreshold:   4,
			l0StopWritesThreshold:   500,
			l0CompactionConcurrency: 2,
			l0BlockSize:             32 << 10,
			targetFileSize:          2 << 20,
			walMinSyncInterval:      200 * time.Millisecond,
		},
		{
			name:                    "medium",
			profile:                 MediumPerformance,
			cacheSize:               256 << 20,
			memTableSize:            128 << 20,
			memTableStopThreshold:   4,
			l0CompactionThreshold:   4,
			l0StopWritesThreshold:   1000,
			l0CompactionConcurrency: 2,
			l0BlockSize:             32 << 10,
			targetFileSize:          2 << 20,
			walMinSyncInterval:      200 * time.Millisecond,
		},
		{
			name:                    "high",
			profile:                 HighPerformance,
			cacheSize:               1024 << 20,
			memTableSize:            256 << 20,
			memTableStopThreshold:   8,
			l0CompactionThreshold:   8,
			l0StopWritesThreshold:   2000,
			l0CompactionConcurrency: 4,
			l0BlockSize:             64 << 10,
			targetFileSize:          4 << 20,
			walMinSyncInterval:      150 * time.Millisecond,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := BuildPebbleOptions(tc.profile)

			require.NoError(t, opts.Validate())
			require.Equal(t, pebble.FormatNewest, opts.FormatMajorVersion)
			require.Equal(t, pebble.FormatMajorVersion(30), opts.FormatMajorVersion)
			require.Equal(t, DefaultKeyComparer().Name, opts.Comparer.Name)
			require.NotEmpty(t, opts.KeySchema)
			require.Contains(t, opts.KeySchemas, opts.KeySchema)
			require.Equal(t, bloom.FilterPolicy(10).Name(), opts.Levels[0].TableFilterPolicy().Name())
			require.Same(t, sstable.SnappyCompression, opts.Levels[0].Compression())
			require.Same(t, sstable.SnappyCompression, opts.Levels[1].Compression())
			for level := 2; level < len(opts.Levels); level++ {
				require.Same(t, sstable.ZstdCompression, opts.Levels[level].Compression())
			}

			valuePolicy := opts.ValueSeparationPolicy()
			require.True(t, valuePolicy.Enabled)
			require.Equal(t, 64, valuePolicy.MinimumSize)
			require.Equal(t, 64, valuePolicy.MinimumMVCCGarbageSize)
			require.Equal(t, 10, valuePolicy.MaxBlobReferenceDepth)
			require.Equal(t, 60*time.Second, valuePolicy.RewriteMinimumAge)

			spanPolicy, err := opts.SpanPolicyFunc(pebble.UserKeyBounds{})
			require.NoError(t, err)
			require.True(t, spanPolicy.PreferFastCompression)
			require.True(t, spanPolicy.ValueStoragePolicy.DisableSeparationBySuffix)
			require.True(t, spanPolicy.ValueStoragePolicy.DisableBlobSeparation)

			require.Equal(t, tc.cacheSize, opts.CacheSize)
			require.Equal(t, tc.memTableSize, opts.MemTableSize)
			require.Equal(t, tc.memTableStopThreshold, opts.MemTableStopWritesThreshold)
			require.Equal(t, tc.l0CompactionThreshold, opts.L0CompactionThreshold)
			require.Equal(t, tc.l0StopWritesThreshold, opts.L0StopWritesThreshold)
			require.Equal(t, tc.l0CompactionConcurrency, opts.L0CompactionConcurrency)
			require.Equal(t, tc.l0BlockSize, opts.Levels[0].BlockSize)
			require.Equal(t, tc.targetFileSize, opts.TargetFileSizes[0])
			require.Equal(t, tc.walMinSyncInterval, opts.WALMinSyncInterval())
		})
	}
}

func TestPebbleModuleOrigin(t *testing.T) {
	type module struct {
		Path    string
		Version string
		Query   string
		Dir     string
		Origin  struct {
			VCS  string
			URL  string
			Hash string
		}
	}

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repoDir := filepath.Dir(filename)
	goListModule := func(t *testing.T, dir, query string) module {
		t.Helper()
		cmd := exec.Command("go", "list", "-m", "-json", query)
		cmd.Dir = dir
		output, err := cmd.Output()
		require.NoError(t, err)
		var result module
		require.NoError(t, json.Unmarshal(output, &result))
		return result
	}

	selected := goListModule(t, repoDir, "github.com/cockroachdb/pebble")
	require.Equal(t, approvedPebbleVersion, selected.Version)
	require.NotEmpty(t, selected.Dir)
	require.Contains(t, filepath.Base(selected.Dir), approvedPebbleVersion)

	resolved := goListModule(t, repoDir, "github.com/cockroachdb/pebble@"+approvedPebbleCommit)
	require.Equal(t, approvedPebbleVersion, resolved.Version)
	require.Equal(t, approvedPebbleCommit, resolved.Query)
	require.Equal(t, "git", resolved.Origin.VCS)
	require.Equal(t, "https://github.com/cockroachdb/pebble", resolved.Origin.URL)
	require.Equal(t, approvedPebbleCommit, resolved.Origin.Hash)
}
