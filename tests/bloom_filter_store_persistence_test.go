package bond_tests

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/bloom"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bucketAwareStorer tracks which buckets have been touched during Set operations
type bucketAwareStorer struct {
	data           map[string][]byte
	touchedBuckets map[int]bool
	keyPrefix      string
}

func newBucketAwareStorer() *bucketAwareStorer {
	// The default key prefix used by bloom filter
	keyPrefix := bond.KeyEncode(bond.Key{
		TableID:    0,
		IndexID:    0,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("bf_"),
	})

	return &bucketAwareStorer{
		data:           make(map[string][]byte),
		touchedBuckets: make(map[int]bool),
		keyPrefix:      string(keyPrefix),
	}
}

func (b *bucketAwareStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	keyStr := string(key)
	keyData, ok := b.data[keyStr]
	if !ok {
		return nil, nil, bond.ErrNotFound
	}
	return keyData, io.NopCloser(nil), nil
}

func (b *bucketAwareStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	keyStr := string(key)

	// Check if this is a bucket key (not the bucket number key "bn")
	if strings.HasPrefix(keyStr, b.keyPrefix) && !strings.HasSuffix(keyStr, "bn") {
		// Extract the bucket number from the end of the key
		suffix := strings.TrimPrefix(keyStr, b.keyPrefix)
		if bucketNum, err := strconv.Atoi(suffix); err == nil {
			b.touchedBuckets[bucketNum] = true
		}
	}

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	b.data[keyStr] = valueCopy
	return nil
}

func (b *bucketAwareStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	// Reset touched buckets on clear
	b.touchedBuckets = make(map[int]bool)

	startStr := string(start)
	endStr := string(end)

	for key := range b.data {
		if key >= startStr && key < endStr {
			delete(b.data, key)
		}
	}
	return nil
}

func (b *bucketAwareStorer) getTouchedBucketCount() int {
	return len(b.touchedBuckets)
}

func (b *bucketAwareStorer) getTouchedBuckets() []int {
	buckets := make([]int, 0, len(b.touchedBuckets))
	for bucket := range b.touchedBuckets {
		buckets = append(buckets, bucket)
	}
	return buckets
}

func (b *bucketAwareStorer) resetTouchedBuckets() {
	b.touchedBuckets = make(map[int]bool)
}

func genTestKey(i int) []byte {
	return []byte(fmt.Sprintf("test_key_%d", i))
}

func TestFilter_PersistToBucket(t *testing.T) {
	const (
		bloomNumItems  = 1000
		bloomFp        = 0.01
		bloomBucketNum = 10
	)

	var (
		store       = newBucketAwareStorer()
		addedKeySet = make(map[string]bool)

		lastAddedIndex = 1
	)

	t.Run("Create new filter with bucket-aware store", func(t *testing.T) {
		require.Empty(t, addedKeySet, "No keys should be added yet")

		ctx := context.Background()

		bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
		bloomFilter := bond.NewFilterInitializable(bf)

		// Initialize empty filter (will fail to load, then clear)
		err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{})
		require.NoError(t, err)
		assert.True(t, bloomFilter.IsInitialized())

		touchedCount := store.getTouchedBucketCount()
		assert.Equal(t, 0, touchedCount, "No buckets should be touched after initializing empty filter")

		pendingChanges := bloom.CountBucketsWithPendingChanges(bf)
		assert.Equal(t, 0, pendingChanges, "No buckets should have pending changes after initializing empty filter")

		// Now add items until one bucket is touched
		for pendingChanges < 1 {
			testKey := genTestKey(lastAddedIndex)
			bloomFilter.Add(ctx, testKey)

			addedKeySet[string(testKey)] = true

			// Check how many buckets have been touched so far
			pendingChanges = bloom.CountBucketsWithPendingChanges(bf)

			lastAddedIndex++
		}

		require.GreaterOrEqual(t, pendingChanges, 1, "At least 1 bucket should have pending changes")

		// Ensure all added keys are in the filter
		for keyStr := range addedKeySet {
			require.True(t, bloomFilter.MayContain(ctx, []byte(keyStr)), "Key %s should be in filter", keyStr)
		}

		err = bloomFilter.Save(ctx, store)
		require.NoError(t, err)

		touchedCount = store.getTouchedBucketCount()

		// First save should have touched all buckets (hasBeenWritten = false for all)
		assert.Equal(t, bloomBucketNum, touchedCount, "First save should touch all %d buckets, but touched %d", bloomBucketNum, touchedCount)
	})

	t.Run("Load filter and add items - track bucket touches", func(t *testing.T) {
		require.NotEmpty(t, addedKeySet, "There should be previously added keys to verify after load")

		ctx := context.Background()

		bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
		bloomFilter := bond.NewFilterInitializable(bf)

		// Initialize with previous store
		err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{})
		require.NoError(t, err)
		assert.True(t, bloomFilter.IsInitialized())

		store.resetTouchedBuckets()

		touchedCount := store.getTouchedBucketCount()
		assert.Equal(t, 0, touchedCount, "No buckets should be touched after initializing empty filter")

		// Ensure all previously added keys persist after load
		for keyStr := range addedKeySet {
			require.True(t, bloomFilter.MayContain(ctx, []byte(keyStr)), "Key %s should be in filter after load", keyStr)
		}

		// Add items until one bucket is touched
		pendingChanges := bloom.CountBucketsWithPendingChanges(bf)
		assert.Equal(t, 0, pendingChanges, "No buckets should have pending changes at start")

		for pendingChanges < 1 {
			testKey := genTestKey(lastAddedIndex)
			bloomFilter.Add(ctx, testKey)

			addedKeySet[string(testKey)] = true

			// Check how many buckets have been touched so far
			pendingChanges = bloom.CountBucketsWithPendingChanges(bf)

			lastAddedIndex++
		}

		t.Logf("Last added index: %d", lastAddedIndex)
		require.GreaterOrEqual(t, pendingChanges, 1, "At least 1 bucket should have pending changes after adding items")

		// Ensure all added keys are in the filter
		for keyStr := range addedKeySet {
			require.True(t, bloomFilter.MayContain(ctx, []byte(keyStr)), "Key %s should be in filter", keyStr)
		}

		t.Logf("Pending changes before save: %d buckets", pendingChanges)

		err = bloomFilter.Save(ctx, store)
		require.NoError(t, err)

		touchedCount = store.getTouchedBucketCount()
		assert.GreaterOrEqual(t, touchedCount, pendingChanges, "Save after adding items should touch at least %d buckets, but touched %d", pendingChanges, touchedCount)
	})

	t.Run("Add items to touch ALL buckets", func(t *testing.T) {
		require.NotEmpty(t, addedKeySet, "There should be previously added keys to verify after load")

		ctx := context.Background()

		bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
		bloomFilter := bond.NewFilterInitializable(bf)

		// Initialize with previous store
		err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{})
		require.NoError(t, err)

		store.resetTouchedBuckets()

		touchedCount := store.getTouchedBucketCount()
		assert.Equal(t, 0, touchedCount, "No buckets should be touched after initializing empty filter")

		// Ensure all previously added keys persist after load
		for keyStr := range addedKeySet {
			assert.True(t, bloomFilter.MayContain(ctx, []byte(keyStr)), "Key %s should be in filter after load", keyStr)
		}

		// Add items until all buckets are touched
		pendingChanges := bloom.CountBucketsWithPendingChanges(bf)
		assert.Equal(t, 0, pendingChanges, "No buckets should have pending changes at start")

		for pendingChanges < bloomBucketNum {
			testKey := genTestKey(lastAddedIndex)
			bloomFilter.Add(ctx, testKey)
			addedKeySet[string(testKey)] = true
			pendingChanges = bloom.CountBucketsWithPendingChanges(bf)

			lastAddedIndex++
		}

		require.Equal(t, bloomBucketNum, pendingChanges, "All %d buckets should have pending changes", bloomBucketNum)

		// Ensure all added keys are in the filter
		for keyStr := range addedKeySet {
			require.True(t, bloomFilter.MayContain(ctx, []byte(keyStr)), "Key %s should be in filter", keyStr)
		}

		t.Logf("Pending changes before save: %d buckets", pendingChanges)
		err = bloomFilter.Save(ctx, store)
		require.NoError(t, err)

		touchedCount = store.getTouchedBucketCount()
		t.Logf("touchedCount after save: %d", touchedCount)
		assert.Equal(t, bloomBucketNum, touchedCount, "Save after adding items should touch all %d buckets", bloomBucketNum)
	})

	t.Run("Ensure all added keys persist after multiple loads", func(t *testing.T) {
		require.NotEmpty(t, addedKeySet, "There should be previously added keys to verify after load")

		t.Logf("Verifying total of %d added keys", len(addedKeySet))

		ctx := context.Background()
		bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
		bloomFilter := bond.NewFilterInitializable(bf)

		// Initialize with previous store
		err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{})
		require.NoError(t, err)

		// Ensure all previously added keys persist after load
		sortedKeys := make([]string, 0, len(addedKeySet))
		for keyStr := range addedKeySet {
			sortedKeys = append(sortedKeys, keyStr)
		}
		sort.Strings(sortedKeys)
		for _, keyStr := range sortedKeys {
			require.True(t, bloomFilter.MayContain(ctx, []byte(keyStr)), "Key %s should be in filter after load", keyStr)
		}

		t.Logf("Successfully verified %d keys after multiple loads", len(addedKeySet))
	})
}
