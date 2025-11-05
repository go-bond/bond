package bloom

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-bond/bond"
	"github.com/go-bond/bond/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/lithammer/go-jump-consistent-hash"
)

var _buffPool = utils.NewPreAllocatedSyncPool[[]byte](func() any {
	return make([]byte, 128<<10) // 128 KB
}, 10) // Pre-allocate 10 buffers

type _bucket struct {
	num            int
	hasChanges     bool
	hasBeenWritten bool

	filter *bloom.BloomFilter

	mu sync.RWMutex
}

type BloomFilter struct {
	hasher utils.SyncPool[jump.KeyHasher]

	keyPrefix    []byte
	numOfBuckets int
	buckets      []*_bucket

	stats bond.FilterStats

	// Track unsaved changes
	unsavedChangeCount uint64

	mu sync.RWMutex
}

func NewBloomFilter(n uint, fp float64, numOfBuckets int, keyPrefixes ...[]byte) *BloomFilter {
	hasher := utils.NewPreAllocatedSyncPool[jump.KeyHasher](func() any {
		return jump.NewCRC32()
	}, 4) // Pre-allocate 4 hashers

	buckets := make([]*_bucket, 0, numOfBuckets)
	for i := 0; i < numOfBuckets; i++ {
		buckets = append(buckets, &_bucket{
			num:            i,
			hasChanges:     false,
			hasBeenWritten: false,
			filter:         bloom.NewWithEstimates(n, fp),
		})
	}

	keyPrefix := bond.KeyEncode(bond.Key{
		TableID:    0,
		IndexID:    0,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("bf_"),
	})
	if len(keyPrefixes) > 0 {
		keyPrefix = keyPrefixes[0]
	}

	slog.Info("bond: BloomFilter: created new bloom filter",
		"n", n,
		"fp", fp,
		"num_buckets", numOfBuckets,
		"key_prefix", fmt.Sprintf("%x", keyPrefix))

	return &BloomFilter{
		hasher:             hasher,
		keyPrefix:          keyPrefix,
		numOfBuckets:       numOfBuckets,
		buckets:            buckets,
		unsavedChangeCount: 0,
		mu:                 sync.RWMutex{},
	}
}

func (b *BloomFilter) Add(_ context.Context, key []byte) {
	bucketIndex := b.hash(key)
	bucket := b.buckets[bucketIndex]

	// Get current unsaved change count before add
	currentUnsaved := atomic.LoadUint64(&b.unsavedChangeCount)

	slog.Debug("bond: BloomFilter.Add: adding key to filter",
		"key", fmt.Sprintf("%x", key),
		"key_length", len(key),
		"bucket_index", bucketIndex,
		"num_buckets", b.numOfBuckets,
		"unsaved_changes_before", currentUnsaved)

	// TestOrAdd is thread-safe, but we lock to protect hasChanges update
	// Lock the specific bucket
	bucket.mu.Lock()
	added := bucket.filter.TestOrAdd(key)
	if added {
		bucket.hasChanges = true
		// Increment unsaved change count
		atomic.AddUint64(&b.unsavedChangeCount, 1)
	}
	bucket.mu.Unlock()

	newUnsaved := atomic.LoadUint64(&b.unsavedChangeCount)

	slog.Debug("bond: BloomFilter.Add: key add complete",
		"key", fmt.Sprintf("%x", key),
		"was_new", added,
		"bucket_has_changes", bucket.hasChanges,
		"unsaved_changes_after", newUnsaved)
}

func (b *BloomFilter) MayContain(_ context.Context, key []byte) bool {
	bucketIndex := b.hash(key)
	bucket := b.buckets[bucketIndex]

	// Get current unsaved change count
	currentUnsaved := atomic.LoadUint64(&b.unsavedChangeCount)

	slog.Info("bond: BloomFilter.MayContain: checking key",
		"key", fmt.Sprintf("%x", key),
		"key_length", len(key),
		"bucket_index", bucketIndex,
		"num_buckets", b.numOfBuckets,
		"unsaved_changes", currentUnsaved)

	// Read-lock the specific bucket
	bucket.mu.RLock()
	filterIsNil := bucket.filter == nil
	bucketHasChanges := bucket.hasChanges
	bucketHasBeenWritten := bucket.hasBeenWritten
	var contains bool
	if !filterIsNil {
		contains = bucket.filter.Test(key)
	} else {
		contains = false
	}
	bucket.mu.RUnlock()

	// If we have unsaved changes and the key is not found,
	// this could be a race condition where:
	// 1. Key was added (unsaved_changes > 0)
	// 2. MayContain was called before Save
	// 3. System crashed/restarted
	// 4. Filter loaded from old state (without the added key)
	if !contains && currentUnsaved > 0 {
		slog.Info("bond: BloomFilter.MayContain: POTENTIAL RACE CONDITION DETECTED",
			"key", fmt.Sprintf("%x", key),
			"contains", contains,
			"unsaved_changes", currentUnsaved,
			"bucket_has_changes", bucketHasChanges,
			"bucket_has_been_written", bucketHasBeenWritten,
			"filter_is_nil", filterIsNil)
	}

	slog.Info("bond: BloomFilter.MayContain: check complete",
		"key", fmt.Sprintf("%x", key),
		"contains", contains,
		"filter_is_nil", filterIsNil,
		"bucket_index", bucketIndex,
		"bucket_has_changes", bucketHasChanges,
		"bucket_has_been_written", bucketHasBeenWritten,
		"unsaved_changes", currentUnsaved)

	if !contains {
		atomic.AddUint64(&b.stats.MissCount, 1)
	} else {
		atomic.AddUint64(&b.stats.HitCount, 1)
	}

	return contains
}

func (b *BloomFilter) Stats() bond.FilterStats {
	return bond.FilterStats{
		HitCount:       atomic.LoadUint64(&b.stats.HitCount),
		MissCount:      atomic.LoadUint64(&b.stats.MissCount),
		FalsePositives: atomic.LoadUint64(&b.stats.FalsePositives),
	}
}

func (b *BloomFilter) RecordFalsePositive() {
	atomic.AddUint64(&b.stats.FalsePositives, 1)
}

func (b *BloomFilter) Load(_ context.Context, store bond.FilterStorer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentUnsaved := atomic.LoadUint64(&b.unsavedChangeCount)

	slog.Info("bond: BloomFilter.Load: starting load",
		"num_buckets", b.numOfBuckets,
		"key_prefix", fmt.Sprintf("%x", b.keyPrefix),
		"unsaved_changes_before_load", currentUnsaved)

	// WARNING: If we have unsaved changes and we're loading, this is a serious issue
	if currentUnsaved > 0 {
		slog.Info("bond: BloomFilter.Load: WARNING - loading filter with unsaved changes",
			"unsaved_changes", currentUnsaved,
			"this_may_indicate_crash_recovery_or_race_condition", true)
	}

	var keyBuff [1024]byte

	bucketNum, bucketNumCloser, err := store.Get(buildBucketNumKey(keyBuff[:0], b.keyPrefix))
	if err != nil {
		slog.Info("bond: BloomFilter.Load: failed to get bucket num",
			"error", err)
		return fmt.Errorf("configuration changed")
	}

	storedNumBuckets := binary.BigEndian.Uint64(bucketNum)
	_ = bucketNumCloser.Close()

	if storedNumBuckets != uint64(b.numOfBuckets) {
		slog.Info("bond: BloomFilter.Load: bucket count mismatch",
			"stored_buckets", storedNumBuckets,
			"current_buckets", b.numOfBuckets)
		return fmt.Errorf("configuration changed")
	}

	slog.Info("bond: BloomFilter.Load: bucket count matches, loading buckets",
		"num_buckets", b.numOfBuckets)

	for _, bucket := range b.buckets {
		slog.Info("bond: BloomFilter.Load: loading bucket",
			"bucket_num", bucket.num)

		data, closer, err := store.Get(buildKey(keyBuff[:0], b.keyPrefix, bucket.num))
		if err != nil {
			slog.Info("bond: BloomFilter.Load: failed to get bucket data",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		filter := bloom.New(0, 0)
		zr, err := zstd.NewReader(bytes.NewBuffer(data))
		if err != nil {
			slog.Info("bond: BloomFilter.Load: failed to create zstd reader",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		_, err = filter.ReadFrom(zr)
		if err != nil {
			slog.Info("bond: BloomFilter.Load: failed to read filter data",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		zr.Close()
		_ = closer.Close()

		// Lock the specific bucket before modifying it
		bucket.mu.Lock()
		err = bucket.filter.Merge(filter)
		if err != nil {
			bucket.mu.Unlock()
			slog.Info("bond: BloomFilter.Load: failed to merge filter",
				"bucket_num", bucket.num,
				"error", err)
			return fmt.Errorf("configuration changed (incompatible filter parameters): %w", err)
		}
		bucket.hasBeenWritten = true
		bucket.mu.Unlock()

		slog.Info("bond: BloomFilter.Load: bucket loaded successfully",
			"bucket_num", bucket.num)
	}

	// Reset unsaved change count after successful load
	// This is correct because we just loaded the saved state
	atomic.StoreUint64(&b.unsavedChangeCount, 0)

	slog.Info("bond: BloomFilter.Load: all buckets loaded successfully",
		"num_buckets", b.numOfBuckets,
		"unsaved_changes_reset_to", 0)

	return nil
}

func (b *BloomFilter) Save(_ context.Context, store bond.FilterStorer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentUnsaved := atomic.LoadUint64(&b.unsavedChangeCount)

	slog.Info("bond: BloomFilter.Save: starting save",
		"num_buckets", b.numOfBuckets,
		"key_prefix", fmt.Sprintf("%x", b.keyPrefix),
		"unsaved_changes", currentUnsaved)

	var keyBuff [1024]byte
	dataBuff := _buffPool.Get()
	defer _buffPool.Put(dataBuff[:0])

	binary.BigEndian.PutUint64(dataBuff[:8], uint64(b.numOfBuckets))
	err := store.Set(buildBucketNumKey(keyBuff[:0], b.keyPrefix), dataBuff[:8], bond.Sync)
	if err != nil {
		slog.Info("bond: BloomFilter.Save: failed to save bucket num",
			"error", err)
		return err
	}

	bucketsToSave := 0
	bucketsSkipped := 0

	for _, bucket := range b.buckets {
		// Lock the bucket early to check flags and potentially write
		bucket.mu.Lock()
		if !bucket.hasChanges && bucket.hasBeenWritten {
			bucket.mu.Unlock() // Unlock if we skip
			bucketsSkipped++
			slog.Info("bond: BloomFilter.Save: skipping unchanged bucket",
				"bucket_num", bucket.num)
			continue
		}

		slog.Info("bond: BloomFilter.Save: saving bucket",
			"bucket_num", bucket.num,
			"has_changes", bucket.hasChanges,
			"has_been_written", bucket.hasBeenWritten)

		bucketsToSave++

		// Use a temporary buffer from the pool for writing
		writeBuff := _buffPool.Get()
		buff := bytes.NewBuffer(writeBuff[:0]) // Use the pooled buffer

		zw, err := zstd.NewWriter(buff)
		if err != nil {
			bucket.mu.Unlock()
			slog.Info("bond: BloomFilter.Save: failed to create zstd writer",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		_, err = bucket.filter.WriteTo(zw)
		if err != nil {
			bucket.mu.Unlock()
			slog.Info("bond: BloomFilter.Save: failed to write filter",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		err = zw.Close()
		if err != nil {
			bucket.mu.Unlock()
			slog.Info("bond: BloomFilter.Save: failed to close zstd writer",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		// Put the buffer back BEFORE potential error return from store.Set
		_buffPool.Put(writeBuff[:0])

		err = store.Set(
			buildKey(keyBuff[:0], b.keyPrefix, bucket.num),
			buff.Bytes(), // Use the compressed data from the buffer
			bond.Sync,
		)
		if err != nil {
			bucket.mu.Unlock()
			slog.Info("bond: BloomFilter.Save: failed to save bucket data",
				"bucket_num", bucket.num,
				"error", err)
			return err
		}

		bucket.hasChanges = false
		bucket.hasBeenWritten = true
		bucket.mu.Unlock()

		slog.Info("bond: BloomFilter.Save: bucket saved successfully",
			"bucket_num", bucket.num)
	}

	// Reset unsaved change count after successful save
	atomic.StoreUint64(&b.unsavedChangeCount, 0)

	slog.Info("bond: BloomFilter.Save: save complete",
		"buckets_saved", bucketsToSave,
		"buckets_skipped", bucketsSkipped,
		"total_buckets", b.numOfBuckets,
		"unsaved_changes_reset_to", 0)

	return nil
}

func (b *BloomFilter) Clear(_ context.Context, store bond.FilterStorer) error {
	currentUnsaved := atomic.LoadUint64(&b.unsavedChangeCount)

	slog.Info("bond: BloomFilter.Clear: starting clear",
		"key_prefix", fmt.Sprintf("%x", b.keyPrefix),
		"unsaved_changes", currentUnsaved)

	end := make([]byte, len(b.keyPrefix))
	copy(end, b.keyPrefix)
	end[len(end)-1]++

	err := store.DeleteRange(b.keyPrefix, end, bond.Sync)
	if err != nil {
		slog.Info("bond: BloomFilter.Clear: clear failed",
			"error", err)
		return err
	}

	// Reset unsaved change count after clear
	atomic.StoreUint64(&b.unsavedChangeCount, 0)

	slog.Info("bond: BloomFilter.Clear: clear successful",
		"unsaved_changes_reset_to", 0)
	return nil
}

func (b *BloomFilter) hash(key []byte) int {
	hasher := b.hasher.Get()
	v := int(HashBytes(key, int32(b.numOfBuckets), hasher))
	b.hasher.Put(hasher)

	slog.Debug("bond: BloomFilter.hash: computed hash",
		"key", fmt.Sprintf("%x", key),
		"key_length", len(key),
		"bucket", v,
		"num_buckets", b.numOfBuckets)

	return v
}

func buildKey(buff []byte, keyPrefix []byte, bucketNo int) []byte {
	buff = append(buff, keyPrefix...)
	buff = strconv.AppendInt(buff, int64(bucketNo), 10)
	return buff
}

func buildBucketNumKey(buff []byte, keyPrefix []byte) []byte {
	buff = append(buff, keyPrefix...)
	buff = append(buff, "bn"...)
	return buff
}

func HashBytes(key []byte, buckets int32, h jump.KeyHasher) int32 {
	h.Reset()
	_, err := h.Write(key)
	if err != nil {
		slog.Debug("bond: BloomFilter.HashBytes: hash write failed",
			"key", fmt.Sprintf("%x", key),
			"error", err)
		panic(err)
	}
	hash := jump.Hash(h.Sum64(), buckets)

	slog.Debug("bond: BloomFilter.HashBytes: hash computed",
		"key", fmt.Sprintf("%x", key),
		"hash", hash,
		"buckets", buckets)

	return hash
}
