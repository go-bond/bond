package bloom

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type filterStorer struct {
	data map[string][]byte
}

func (f *filterStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	keyData, ok := f.data[cleanKey(key)]
	if !ok {
		return nil, nil, bond.ErrNotFound
	}
	return keyData, io.NopCloser(nil), nil
}

func (f *filterStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	f.data[cleanKey(key)] = valueCopy
	return nil
}

func (f *filterStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
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
