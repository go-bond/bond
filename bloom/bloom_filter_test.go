package bloom

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type filterStorer struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func (f *filterStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	keyData, ok := f.data[cleanKey(key)]
	if !ok {
		return nil, nil, bond.ErrNotFound
	}
	return keyData, io.NopCloser(nil), nil
}

func (f *filterStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	f.data[cleanKey(key)] = valueCopy
	return nil
}

func (f *filterStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for key, _ := range f.data {
		if strings.HasPrefix(key, cleanKey(start)) {
			delete(f.data, key)
		}

		if strings.Compare(key, string(end)) == 0 {
			break
		}
	}
	return nil
}

func cleanKey(key []byte) string {
	cleanKey := make([]rune, 0, len(key))
	for _, r := range string(key) {
		if r >= 32 && r <= 126 { // Keep printable ASCII characters
			cleanKey = append(cleanKey, r)
		}
	}
	return string(cleanKey)
}

func TestBloomFilter(t *testing.T) {
	filePath := "./tests/test_bloom_filter/"
	defer os.RemoveAll(filePath)

	variants := []struct {
		Name       string
		FileStorer func() bond.FilterStorer
	}{
		{
			Name: "InMemoryFileStorer",
			FileStorer: func() bond.FilterStorer {
				return &filterStorer{
					data: make(map[string][]byte),
				}
			},
		},
		{
			Name: "FSFileStorer",
			FileStorer: func() bond.FilterStorer {
				return filters.NewFSFilterStorer(filePath)
			},
		},
	}

	for _, variant := range variants {
		t.Run(variant.Name+"_BloomFilter_Add_MayContain", func(t *testing.T) {
			bf := NewBloomFilter(10, 0.01, 1000)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			for _, key := range keys {
				assert.True(t, bf.MayContain(context.Background(), key))
			}
		})

		t.Run(variant.Name+"_BloomFilter_Save_Load_Clear", func(t *testing.T) {
			bf := NewBloomFilter(10, 0.01, 1000)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			store := variant.FileStorer()

			// Save the filter to the store
			err := bf.Save(context.Background(), store)
			require.NoError(t, err)

			// Load the filter from the store
			bf2 := NewBloomFilter(10, 0.01, 1000)
			err = bf2.Load(context.Background(), store)
			require.NoError(t, err)

			// Check if the filter contains the keys
			for _, key := range keys {
				assert.True(t, bf2.MayContain(context.Background(), key))
			}

			// Clear the filter
			err = bf2.Clear(context.Background(), store)
			require.NoError(t, err)

			// Try to load the filter again, should fail because the filter is cleared
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)

			// Build a new filter
			bf3 := NewBloomFilter(10, 0.01, 1000)

			keys = KeyGenerate(200, 128)
			for _, key := range keys {
				bf3.Add(context.Background(), key)
			}

			// Try to save a new filter
			err = bf3.Save(context.Background(), store)
			require.NoError(t, err)

			// Try to load the new filter
			err = bf3.Load(context.Background(), store)
			require.NoError(t, err)

			// Check if the filter contains the keys
			for _, key := range keys {
				assert.True(t, bf3.MayContain(context.Background(), key))
			}
		})

		t.Run(variant.Name+"_BloomFilter_Load_Config_Changed", func(t *testing.T) {
			bf := NewBloomFilter(10, 0.01, 1000)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			store := variant.FileStorer()

			err := bf.Save(context.Background(), store)
			require.NoError(t, err)

			bf2 := NewBloomFilter(100, 0.01, 1000)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)

			bf2 = NewBloomFilter(10, 0.02, 1000)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)

			bf2 = NewBloomFilter(10, 0.01, 100)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)
		})

		t.Run(variant.Name+"_BloomFilter_Load_FileCorrupted", func(t *testing.T) {
			bf := NewBloomFilter(100, 0.01, 5)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			store := variant.FileStorer()

			err := bf.Save(context.Background(), store)
			require.NoError(t, err)

			// Corrupt the file
			switch variant.Name {
			case "InMemoryFileStorer":
				store.(*filterStorer).data["bf_0"] = []byte("corrupted")
			case "FSFileStorer":
				err = os.WriteFile(path.Join(filePath, "bf_0"), []byte("corrupted"), 0660)
				require.NoError(t, err)
			}

			bf2 := NewBloomFilter(100, 0.01, 5)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)
		})
	}
}

func TestBloomFilter_IncrementalSave(t *testing.T) {
	const (
		bloomNumItems  = 1000
		bloomFp        = 0.01
		bloomBucketNum = 10
	)

	// Test saving incremental changes to the Bloom filter and verify that all
	// keys are present after multiple saves
	t.Run("Save incremental changes", func(t *testing.T) {
		initialKeys := [][]byte{
			[]byte("key1"),
			[]byte("key2"),
			[]byte("key3"),
		}

		newKeys := [][]byte{
			[]byte("key4"),
			[]byte("key5"),
			[]byte("key6"),
		}

		store := newStorer()

		// First save with initial keys
		t.Run("Add keys and save", func(t *testing.T) {
			bf := NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)

			for _, key := range initialKeys {
				bf.Add(context.Background(), key)
			}

			err := bf.Save(context.Background(), store)
			require.NoError(t, err)
		})

		// Load, add new keys, and save again
		t.Run("Load, add new keys, save again, and verify", func(t *testing.T) {
			// Load the filter from the store
			bf2 := NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)

			err := bf2.Load(context.Background(), store)
			require.NoError(t, err)

			// Add NEW keys to a loaded filter
			for _, key := range newKeys {
				bf2.Add(context.Background(), key)
			}

			// All keys (initial + new) should be present before saving
			for _, key := range initialKeys {
				assert.True(t, bf2.MayContain(context.Background(), key))
			}
			for _, key := range newKeys {
				assert.True(t, bf2.MayContain(context.Background(), key))
			}

			// Second save - should save the changes
			err = bf2.Save(context.Background(), store)
			require.NoError(t, err)
		})

		// Finally, load into a fresh filter and verify all keys are present
		t.Run("Load into fresh filter and verify all keys", func(t *testing.T) {
			// Load the filter from the store
			bf3 := NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			err := bf3.Load(context.Background(), store)
			require.NoError(t, err)

			// Check initial keys
			for _, key := range initialKeys {
				assert.True(
					t,
					bf3.MayContain(context.Background(), key),
					"Initial key %s should be present",
					key,
				)
			}

			// Check new keys
			for _, key := range newKeys {
				assert.True(
					t,
					bf3.MayContain(context.Background(), key),
					"Key %s added after load should be present",
					key,
				)
			}
		})
	})

	// Test that false negative are not introduced after incremental saves
	t.Run("False negative rate after incremental saves", func(t *testing.T) {
		store := newStorer()

		initialKeys := [][]byte{
			[]byte("key1"),
			[]byte("key2"),
			[]byte("key3"),
		}

		newKey := []byte("key4")

		t.Run("Add initial keys and save", func(t *testing.T) {
			bf := NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)

			for _, key := range initialKeys {
				bf.Add(context.Background(), key)
			}

			err := bf.Save(context.Background(), store)
			require.NoError(t, err)
		})

		t.Run("Load and verify initial keys", func(t *testing.T) {
			bf2 := NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)

			err := bf2.Load(context.Background(), store)
			require.NoError(t, err)

			for _, key := range initialKeys {
				assert.True(
					t,
					bf2.MayContain(context.Background(), key),
					"Initial key %s should be present after load",
					key,
				)
			}

			// Add a new key and save again
			bf2.Add(context.Background(), newKey)

			// Verify all initial keys and the new key are present before saving
			allKeys := append(initialKeys, newKey)
			for _, key := range allKeys {
				assert.True(
					t,
					bf2.MayContain(context.Background(), key),
					"Key %s should be present before saving after adding new key",
					key,
				)
			}

			// Save the updated filter
			err = bf2.Save(context.Background(), store)
		})

		t.Run("Load into fresh filter and verify the new key", func(t *testing.T) {
			bf3 := NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)

			err := bf3.Load(context.Background(), store)
			require.NoError(t, err)

			// Verify the new key
			assert.True(
				t,
				bf3.MayContain(context.Background(), newKey),
				"New key should be present after loading into fresh filter",
			)
		})
	})

	// Test that adding a duplicate key does not set hasChanges to true
	t.Run("Save optimization", func(t *testing.T) {
		store := newStorer()

		bf := NewBloomFilter(
			bloomNumItems,
			bloomFp,
			1, // Single bucket to make it deterministic
		)

		key := []byte("duplicate_key")

		// Add key once
		bf.Add(context.Background(), key)

		// Save
		err := bf.Save(context.Background(), store)
		require.NoError(t, err)

		// Manually check hasChanges flag after save
		bucket := bf.buckets[0]
		bucket.mu.Lock()
		assert.False(t, bucket.hasChanges, "After save, hasChanges should be false")
		bucket.mu.Unlock()

		// Add the SAME key again
		bf.Add(context.Background(), key)

		// Now hasChanges should remain false
		bucket.mu.Lock()
		hasChangesAfterDuplicate := bucket.hasChanges
		bucket.mu.Unlock()

		assert.False(t,
			hasChangesAfterDuplicate,
			"hasChanges should remain false for duplicate key (optimization)",
		)

		// This means no unnecessary save will happen
		err = bf.Save(context.Background(), store)
		require.NoError(t, err)
	})
}

func TestBloomFilter_ConcurrentAccess(t *testing.T) {
	t.Run("Concurrent Add operations", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01, 10)

		const numGoroutines = 100
		const keysPerGoroutine = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Concurrent adds
		for i := 0; i < numGoroutines; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < keysPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("worker_%d_key_%d", workerID, j))
					bf.Add(context.Background(), key)
				}
			}(i)
		}

		wg.Wait()

		// Verify all keys are present
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < keysPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("worker_%d_key_%d", i, j))
				assert.True(t, bf.MayContain(context.Background(), key),
					"Key should be present after concurrent add")
			}
		}
	})

	t.Run("Concurrent Add and MayContain operations", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01, 10)

		const numWriters = 50
		const numReaders = 50
		const keysPerWriter = 100

		// Pre-populate some keys
		prePopulatedKeys := make([][]byte, 100)
		for i := range prePopulatedKeys {
			key := []byte(fmt.Sprintf("prepopulated_key_%d", i))
			prePopulatedKeys[i] = key
			bf.Add(context.Background(), key)
		}

		var wg sync.WaitGroup
		wg.Add(numWriters + numReaders)

		// Writers
		for i := 0; i < numWriters; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < keysPerWriter; j++ {
					key := []byte(fmt.Sprintf("writer_%d_key_%d", workerID, j))
					bf.Add(context.Background(), key)
				}
			}(i)
		}

		// Readers
		for i := 0; i < numReaders; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < keysPerWriter; j++ {
					// Check pre-populated keys
					key := prePopulatedKeys[j%len(prePopulatedKeys)]
					bf.MayContain(context.Background(), key)

					// Check random keys (may or may not exist)
					randomKey := []byte(fmt.Sprintf("random_key_%d_%d", workerID, j))
					bf.MayContain(context.Background(), randomKey)
				}
			}(i)
		}

		wg.Wait()

		// Verify pre-populated keys are still present
		for _, key := range prePopulatedKeys {
			assert.True(t, bf.MayContain(context.Background(), key),
				"Pre-populated key should still be present")
		}

		// Verify writer keys are present
		for i := 0; i < numWriters; i++ {
			for j := 0; j < keysPerWriter; j++ {
				key := []byte(fmt.Sprintf("writer_%d_key_%d", i, j))
				assert.True(t, bf.MayContain(context.Background(), key),
					"Writer key should be present")
			}
		}

		// Verify stats were updated
		stats := bf.Stats()
		assert.Greater(t, stats.HitCount+stats.MissCount, uint64(0),
			"Stats should have been updated")
	})

	t.Run("Concurrent Save and Load operations", func(t *testing.T) {
		const numSavers = 10
		const numLoaders = 10

		var wg sync.WaitGroup
		wg.Add(numSavers + numLoaders)

		// Create separate stores for each operation to avoid conflicts
		stores := make([]bond.FilterStorer, numSavers)
		for i := range stores {
			stores[i] = newStorer()
		}

		// Concurrent saves
		for i := 0; i < numSavers; i++ {
			go func(workerID int) {
				defer wg.Done()
				bf := NewBloomFilter(100, 0.01, 10)

				// Add some keys
				for j := 0; j < 50; j++ {
					key := []byte(fmt.Sprintf("save_worker_%d_key_%d", workerID, j))
					bf.Add(context.Background(), key)
				}

				// Save
				err := bf.Save(context.Background(), stores[workerID])
				assert.NoError(t, err, "Save should succeed")
			}(i)
		}

		// Concurrent loads (after a brief delay to allow some saves)
		for i := 0; i < numLoaders; i++ {
			go func(workerID int) {
				defer wg.Done()

				// Use the store from the corresponding saver
				storeIndex := workerID % numSavers

				bf := NewBloomFilter(100, 0.01, 10)

				// Try to load - may succeed or fail depending on timing
				_ = bf.Load(context.Background(), stores[storeIndex])
			}(i)
		}

		wg.Wait()
	})

	t.Run("Concurrent operations on same bucket", func(t *testing.T) {
		// Use 1 bucket to force contention
		bf := NewBloomFilter(1000, 0.01, 1)

		const numGoroutines = 100
		const opsPerGoroutine = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Mix of Add and MayContain on the same bucket
		for i := 0; i < numGoroutines; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("bucket_test_%d_%d", workerID, j))

					if j%2 == 0 {
						bf.Add(context.Background(), key)
					} else {
						bf.MayContain(context.Background(), key)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify keys added by even iterations
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < opsPerGoroutine; j += 2 {
				key := []byte(fmt.Sprintf("bucket_test_%d_%d", i, j))
				assert.True(t, bf.MayContain(context.Background(), key),
					"Key added in even iteration should be present")
			}
		}
	})

	t.Run("Concurrent Add with duplicate keys", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01, 5)

		const numGoroutines = 50
		const duplicateKey = "shared_duplicate_key"

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// All goroutines add the same key
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				bf.Add(context.Background(), []byte(duplicateKey))
			}()
		}

		wg.Wait()

		// Key should be present
		assert.True(t, bf.MayContain(context.Background(), []byte(duplicateKey)),
			"Duplicate key should be present")

		// Check that only one bucket should have changes (the one that hashes to this key)
		changedBuckets := CountBucketsWithPendingChanges(bf)
		assert.LessOrEqual(t, changedBuckets, 1,
			"Only the bucket containing the key should have changes")
	})

	t.Run("Race detector test - concurrent stats access", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01, 10)

		const numGoroutines = 50
		const opsPerGoroutine = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("stats_test_%d_%d", workerID, j))

					// Mix operations
					switch j % 4 {
					case 0:
						bf.Add(context.Background(), key)
					case 1:
						bf.MayContain(context.Background(), key)
					case 2:
						bf.RecordFalsePositive()
					case 3:
						_ = bf.Stats()
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify that stats can be retrieved
		stats := bf.Stats()
		assert.Greater(t, stats.HitCount+stats.MissCount, uint64(0),
			"Should have some hits or misses")
	})
}

func KeyGenerate(n int, keySize int) (ret [][]byte) {
	for i := 0; i < n; i++ {
		ret = append(ret, []byte(RandomString(keySize)))
	}
	return
}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func newStorer() bond.FilterStorer {
	return &filterStorer{
		data: make(map[string][]byte),
	}
}
