package filters

import (
	"context"

	"github.com/bits-and-blooms/bloom"
)

type SingleBloomFilter struct {
	filter        *bloom.BloomFilter
	isInitialized bool
}

// NewSingleBloomFilter the bloom filter with capacity n and false-positive ration fp. The filter
// is initialized at creation time with the data from the database. The filter is in-memory only.
// It's not saved anywhere.
func NewSingleBloomFilter(n uint, fp float64) *SingleBloomFilter {
	return &SingleBloomFilter{
		filter:        bloom.NewWithEstimates(n, fp),
		isInitialized: false,
	}
}

func (s *SingleBloomFilter) Add(_ context.Context, key []byte) {
	if !s.isInitialized {
		panic("SingleBloomFilter not initialized")
	}

	s.filter.Add(key)
}

func (s *SingleBloomFilter) MayContain(_ context.Context, key []byte) bool {
	if !s.isInitialized {
		panic("SingleBloomFilter not initialized")
	}

	return s.filter.Test(key)
}

func (s *SingleBloomFilter) IsInitialized() bool {
	return s.isInitialized
}

func (s *SingleBloomFilter) SetInitialized(isInitialized bool) {
	s.isInitialized = isInitialized
}
