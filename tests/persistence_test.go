package bond_tests

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/bloom"
	bondmock "github.com/go-bond/bond/tests/mock/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// filterStorer is an in-memory implementation of bond.FilterStorer for testing
type filterStorer struct {
	data map[string][]byte
}

func (f *filterStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	keyData, ok := f.data[cleanKey(key)]
	if !ok {
		return nil, nil, bond.ErrNotFound
	}
	// Return io.NopCloser instead of nil
	return keyData, io.NopCloser(nil), nil
}

func (f *filterStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	f.data[cleanKey(key)] = valueCopy
	return nil
}

func (f *filterStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	for key := range f.data {
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

func newStorer() bond.FilterStorer {
	return &filterStorer{
		data: make(map[string][]byte),
	}
}

func TestFilterInitializable_IncrementalSave(t *testing.T) {
	const (
		bloomNumItems  = 1000
		bloomFp        = 0.01
		bloomBucketNum = 10
	)

	// Test initializing filter from table scanner, then adding incremental changes
	t.Run("Initialize from scanner, add keys, and save incrementally", func(t *testing.T) {
		ctx := context.Background()

		// Create a real bond.DB instance
		dbPath := t.TempDir() + "/test_incremental.db"
		db, err := bond.Open(dbPath, &bond.Options{})
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		// Use a shared store across all subtests in this group
		store := newStorer()

		// Keys that will be "scanned" from table (simulating existing data)
		scannedKeys := []bond.KeyBytes{
			bond.KeyBytes("scanned_key1"),
			bond.KeyBytes("scanned_key2"),
			bond.KeyBytes("scanned_key3"),
		}

		// Keys that will be added incrementally after initialization
		newKeys := []bond.KeyBytes{
			bond.KeyBytes("new_key1"),
			bond.KeyBytes("new_key2"),
			bond.KeyBytes("new_key3"),
		}

		// First initialization: Load fails, so it scans the table
		t.Run("Initialize from table scanner", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockScanner := bondmock.NewMockTableScanner[any](ctrl)

			bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter := bond.NewFilterInitializable(bf)

			mockScanner.EXPECT().
				ScanForEach(gomock.Any(), gomock.Any(), false).
				DoAndReturn(func(ctx context.Context, f func(bond.KeyBytes, bond.Lazy[any]) (bool, error), reverse bool, optBatch ...bond.Batch) error {
					for _, key := range scannedKeys {
						_, err := f(key, bond.Lazy[any]{})
						if err != nil {
							return err
						}
					}
					return nil
				}).
				Times(1)

			err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{mockScanner})
			require.NoError(t, err)
			assert.True(t, bloomFilter.IsInitialized())

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter.MayContain(ctx, []byte(key)))
			}

			err = bloomFilter.Save(ctx, store)
			require.NoError(t, err)
		})

		// Load the saved filter, add new keys, and save again
		t.Run("Load saved filter, add new keys, and save incrementally", func(t *testing.T) {
			bf2 := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter2 := bond.NewFilterInitializable(bf2)

			err := bloomFilter2.Initialize(ctx, store, []bond.TableScanner[any]{})
			require.NoError(t, err)
			assert.True(t, bloomFilter2.IsInitialized())

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter2.MayContain(ctx, []byte(key)))
			}

			for _, key := range newKeys {
				bloomFilter2.Add(ctx, []byte(key))
			}

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter2.MayContain(ctx, []byte(key)))
			}
			for _, key := range newKeys {
				assert.True(t, bloomFilter2.MayContain(ctx, []byte(key)))
			}

			err = bloomFilter2.Save(ctx, store)
			require.NoError(t, err)
		})

		// Load into fresh filter and verify all keys are present
		t.Run("Load into fresh filter and verify all keys", func(t *testing.T) {
			bf3 := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter3 := bond.NewFilterInitializable(bf3)

			err := bloomFilter3.Initialize(ctx, store, []bond.TableScanner[any]{})
			require.NoError(t, err)

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter3.MayContain(ctx, []byte(key)), "Scanned key %s should be present", key)
			}
			for _, key := range newKeys {
				assert.True(t, bloomFilter3.MayContain(ctx, []byte(key)), "Key %s added after initialization should be present", key)
			}
		})
	})

	// Test the lazy initialization scenario where keys are added before bloom filter is initialized or during initialization.
	t.Run("Add keys, initialize from store, and verify all keys, save, and load from store", func(t *testing.T) {
		ctx := context.Background()

		// Create a real bond.DB instance
		dbPath := t.TempDir() + "/test_add_init_from_store_check.db"
		db, err := bond.Open(dbPath, &bond.Options{})
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		// Use a shared store across all subtests in this group
		store := newStorer()

		// Keys that will be "scanned" from table (simulating existing data)
		scannedKeys := []bond.KeyBytes{
			bond.KeyBytes("scanned_key1"),
			bond.KeyBytes("scanned_key2"),
			bond.KeyBytes("scanned_key3"),
		}

		// Keys that will be added incrementally after initialization
		newKeys := []bond.KeyBytes{
			bond.KeyBytes("new_key1"),
			bond.KeyBytes("new_key2"),
			bond.KeyBytes("new_key3"),
		}

		bf := bond.NewFilterInitializable(
			bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum),
		)

		// Check keys before initialization
		t.Run("Check keys before initialization", func(t *testing.T) {
			for _, key := range newKeys {
				// Since the filter is not initialized, all keys should be maybe present
				assert.True(t, bf.MayContain(ctx, []byte(key)), "Key %s should not be present before initialization", key)
			}

			for _, key := range scannedKeys {
				assert.True(t, bf.MayContain(ctx, []byte(key)), "Scanned key %s should be present before initialization", key)
			}
		})

		// Add keys before initialziation to check if they are added correctly after initialization
		t.Run("Add keys before initialziation", func(t *testing.T) {
			for _, key := range newKeys {
				// Add the key to the filter
				bf.Add(ctx, []byte(key))
			}
		})

		// Initialize from store
		t.Run("Initialize from store", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockScanner := bondmock.NewMockTableScanner[any](ctrl)

			mockScanner.EXPECT().
				ScanForEach(gomock.Any(), gomock.Any(), false).
				DoAndReturn(func(ctx context.Context, f func(bond.KeyBytes, bond.Lazy[any]) (bool, error), reverse bool, optBatch ...bond.Batch) error {
					for _, key := range scannedKeys {
						_, err := f(key, bond.Lazy[any]{})
						if err != nil {
							return err
						}
					}
					return nil
				}).
				Times(1)

			err := bf.Initialize(ctx, store, []bond.TableScanner[any]{mockScanner})
			require.NoError(t, err)
			require.True(t, bf.IsInitialized(), "Filter should be initialized")
		})

		// Check keys after initialization
		t.Run("Check keys after initialization", func(t *testing.T) {
			for _, key := range newKeys {
				assert.True(t, bf.MayContain(ctx, []byte(key)), "Key %s should be present after initialization", key)
			}

			for _, key := range scannedKeys {
				assert.True(t, bf.MayContain(ctx, []byte(key)), "Scanned key %s should be present after initialization", key)
			}
		})

		// Check not present key that should not be present after initializarion
		t.Run("Check key that is not present after initializarion", func(t *testing.T) {
			notPresentKey := "not_present_key"
			assert.False(t, bf.MayContain(ctx, []byte(notPresentKey)), "Key %s should not be present after initialization", notPresentKey)
		})

		// Save the filter
		t.Run("Save the filter", func(t *testing.T) {
			err := bf.Save(ctx, store)
			require.NoError(t, err)
		})

		bf = bond.NewFilterInitializable(
			bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum),
		)

		// Initialize from store
		t.Run("Initialize from store", func(t *testing.T) {
			err := bf.Initialize(ctx, store, []bond.TableScanner[any]{})
			require.NoError(t, err)
			require.True(t, bf.IsInitialized(), "Filter should be initialized")
		})

		// Check keys after initialization
		t.Run("Check keys after initialization", func(t *testing.T) {
			for _, key := range newKeys {
				assert.True(t, bf.MayContain(ctx, []byte(key)), "Key %s should be present after initialization", key)
			}

			for _, key := range scannedKeys {
				assert.True(t, bf.MayContain(ctx, []byte(key)), "Scanned key %s should be present after initialization", key)
			}
		})
	})

	// Test that false negatives are not introduced after incremental saves
	t.Run("No false negatives after incremental saves with Initialize", func(t *testing.T) {
		ctx := context.Background()

		dbPath := t.TempDir() + "/test_no_false_neg.db"
		db, err := bond.Open(dbPath, &bond.Options{})
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		store := newStorer()

		scannedKeys := []bond.KeyBytes{bond.KeyBytes("initial1"), bond.KeyBytes("initial2")}
		newKey := bond.KeyBytes("incremental_key")

		t.Run("Initialize and save", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockScanner := bondmock.NewMockTableScanner[any](ctrl)

			bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter := bond.NewFilterInitializable(bf)

			mockScanner.EXPECT().
				ScanForEach(gomock.Any(), gomock.Any(), false).
				DoAndReturn(func(ctx context.Context, f func(bond.KeyBytes, bond.Lazy[any]) (bool, error), reverse bool, optBatch ...bond.Batch) error {
					for _, key := range scannedKeys {
						_, err := f(key, bond.Lazy[any]{})
						if err != nil {
							return err
						}
					}
					return nil
				}).
				Times(1)

			err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{mockScanner})
			require.NoError(t, err)

			err = bloomFilter.Save(ctx, store)
			require.NoError(t, err)
		})

		t.Run("Load, add new key, and verify before save", func(t *testing.T) {
			bf2 := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter2 := bond.NewFilterInitializable(bf2)

			err := bloomFilter2.Initialize(ctx, store, []bond.TableScanner[any]{})
			require.NoError(t, err)

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter2.MayContain(ctx, []byte(key)), "Scanned key %s should be present after load", key)
			}

			bloomFilter2.Add(ctx, []byte(newKey))

			allKeys := append(scannedKeys, newKey)
			for _, key := range allKeys {
				assert.True(t, bloomFilter2.MayContain(ctx, []byte(key)), "Key %s should be present before saving", key)
			}

			err = bloomFilter2.Save(ctx, store)
			require.NoError(t, err)
		})

		t.Run("Load into fresh filter and verify new key", func(t *testing.T) {
			bf3 := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter3 := bond.NewFilterInitializable(bf3)

			err := bloomFilter3.Initialize(ctx, store, []bond.TableScanner[any]{})
			require.NoError(t, err)

			assert.True(t, bloomFilter3.MayContain(ctx, []byte(newKey)), "Incrementally added key should be present after loading into fresh filter")

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter3.MayContain(ctx, []byte(key)), "Scanned key %s should still be present", key)
			}
		})
	})

	// Test initialization behavior when filter already exists vs needs scanning
	t.Run("Initialize behavior: existing filter vs table scan", func(t *testing.T) {
		ctx := context.Background()

		dbPath := t.TempDir() + "/test_init_behavior.db"
		db, err := bond.Open(dbPath, &bond.Options{})
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		store := newStorer()

		scannedKeys := []bond.KeyBytes{bond.KeyBytes("table_key1"), bond.KeyBytes("table_key2")}

		t.Run("First initialize: should scan table", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockScanner := bondmock.NewMockTableScanner[any](ctrl)

			bf := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter := bond.NewFilterInitializable(bf)

			mockScanner.EXPECT().
				ScanForEach(gomock.Any(), gomock.Any(), false).
				DoAndReturn(func(ctx context.Context, f func(bond.KeyBytes, bond.Lazy[any]) (bool, error), reverse bool, optBatch ...bond.Batch) error {
					for _, key := range scannedKeys {
						_, err := f(key, bond.Lazy[any]{})
						if err != nil {
							return err
						}
					}
					return nil
				}).
				Times(1)

			err := bloomFilter.Initialize(ctx, store, []bond.TableScanner[any]{mockScanner})
			require.NoError(t, err)

			err = bloomFilter.Save(ctx, store)
			require.NoError(t, err)
		})

		t.Run("Second initialize: should load existing filter, not scan", func(t *testing.T) {
			bf2 := bloom.NewBloomFilter(bloomNumItems, bloomFp, bloomBucketNum)
			bloomFilter2 := bond.NewFilterInitializable(bf2)

			err := bloomFilter2.Initialize(ctx, store, []bond.TableScanner[any]{})
			require.NoError(t, err)

			for _, key := range scannedKeys {
				assert.True(t, bloomFilter2.MayContain(ctx, []byte(key)), "Key from loaded filter should be present", key)
			}
		})
	})
}
