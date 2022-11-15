package bloom

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/go-bond/bond"
	"github.com/klauspost/compress/zstd"
	"github.com/lithammer/go-jump-consistent-hash"
)

var _buffPool = sync.Pool{
	New: func() any {
		return make([]byte, 128<<10) // 128 KB
	},
}

type _bucket struct {
	num            int
	hasChanges     bool
	hasBeenWritten bool

	filter *bloom.BloomFilter

	mutex sync.RWMutex
}

type BloomFilter struct {
	hasher *sync.Pool

	keyPrefix    string
	numOfBuckets int
	buckets      []*_bucket

	mutex sync.RWMutex
}

func NewBloomFilter(n uint, fp float64, numOfBuckets int, keyPrefixes ...string) *BloomFilter {
	hasher := &sync.Pool{
		New: func() any {
			return jump.New(numOfBuckets, jump.NewCRC32())
		},
	}

	buckets := make([]*_bucket, 0, numOfBuckets)
	for i := 0; i < numOfBuckets; i++ {
		buckets = append(buckets, &_bucket{
			num:            i,
			hasChanges:     false,
			hasBeenWritten: false,
			filter:         bloom.NewWithEstimates(n, fp),
		})
	}

	keyPrefix := string(bond.KeyEncode(bond.Key{
		TableID:    0,
		IndexID:    0,
		IndexKey:   []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("bf_"),
	}))
	if len(keyPrefixes) > 0 {
		keyPrefix = keyPrefixes[0]
	}

	return &BloomFilter{
		hasher:       hasher,
		keyPrefix:    keyPrefix,
		numOfBuckets: numOfBuckets,
		buckets:      buckets,
		mutex:        sync.RWMutex{},
	}
}

func (b *BloomFilter) Add(_ context.Context, key []byte) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	bucket := b.buckets[b.hash(key)]
	bucket.hasChanges = !bucket.filter.TestOrAdd(key) || bucket.hasChanges
}

func (b *BloomFilter) MayContain(_ context.Context, key []byte) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.buckets[b.hash(key)].filter.Test(key)
}

func (b *BloomFilter) Load(_ context.Context, store bond.FilterStorer) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var keyBuff [1024]byte

	bucketNum, bucketNumCloser, err := store.Get(buildBucketNumKey(keyBuff[:0], b.keyPrefix))
	if err != nil || (err == nil && binary.BigEndian.Uint64(bucketNum) != uint64(b.numOfBuckets)) {
		if err == nil {
			_ = bucketNumCloser.Close()
		}
		return fmt.Errorf("configuration changed")
	}

	for _, bucket := range b.buckets {
		data, closer, err := store.Get(buildKey(keyBuff[:0], b.keyPrefix, bucket.num))
		if err != nil {
			return err
		}

		filter := bloom.New(0, 0)
		zr, err := zstd.NewReader(bytes.NewBuffer(data))
		if err != nil {
			return err
		}

		_, err = filter.ReadFrom(zr)
		if err != nil {
			return err
		}

		zr.Close()
		_ = closer.Close()

		err = bucket.filter.Merge(filter)
		if err != nil {
			return fmt.Errorf("configuration changed: %w", err)
		}

		bucket.hasBeenWritten = true
	}

	return nil
}

func (b *BloomFilter) Save(_ context.Context, store bond.FilterStorer) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var keyBuff [1024]byte
	dataBuff := _buffPool.Get().([]byte)
	defer _buffPool.Put(dataBuff)

	binary.BigEndian.PutUint64(dataBuff[:8], uint64(b.numOfBuckets))
	err := store.Set(buildBucketNumKey(keyBuff[:0], b.keyPrefix), dataBuff[:8], bond.Sync)
	if err != nil {
		return err
	}

	for _, bucket := range b.buckets {
		if !bucket.hasChanges && bucket.hasBeenWritten {
			continue
		}

		buff := bytes.NewBuffer(dataBuff[:0])
		zw, err := zstd.NewWriter(buff)
		if err != nil {
			return err
		}

		_, err = bucket.filter.WriteTo(zw)
		if err != nil {
			return err
		}

		err = zw.Close()
		if err != nil {
			return err
		}

		err = store.Set(
			buildKey(keyBuff[:0], b.keyPrefix, bucket.num),
			buff.Bytes(),
			bond.Sync,
		)
		if err != nil {
			return err
		}

		bucket.hasChanges = false
		bucket.hasBeenWritten = true
	}

	return nil
}

func (b *BloomFilter) Clear(_ context.Context, store bond.FilterStorer) error {
	end := []byte(b.keyPrefix)
	end[len(end)-1]++
	return store.DeleteRange([]byte(b.keyPrefix), end, bond.Sync)
}

func (b *BloomFilter) hash(key []byte) int {
	hasher := b.hasher.Get().(*jump.Hasher)
	defer b.hasher.Put(hasher)
	return hasher.Hash(string(key))
}

func buildKey(buff []byte, keyPrefix string, bucketNo int) []byte {
	buffer := bytes.NewBuffer(buff)
	_, _ = fmt.Fprintf(buffer, "%s%d", keyPrefix, bucketNo)
	return buffer.Bytes()
}

func buildBucketNumKey(buff []byte, keyPrefix string) []byte {
	buffer := bytes.NewBuffer(buff)
	_, _ = fmt.Fprintf(buffer, "%sbn", keyPrefix)
	return buffer.Bytes()
}
