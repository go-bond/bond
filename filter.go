package bond

import (
	"context"
	"fmt"
	"sync/atomic"
)

type FilterStats struct {
	FalsePositives uint64
	HitCount       uint64
	MissCount      uint64
}

type FilterStorer interface {
	Getter
	Setter
	DeleterWithRange
}

type FilterWithStats interface {
	Filter
	Stats() FilterStats
	RecordFalsePositive()
}

type Filter interface {
	Add(ctx context.Context, key []byte)
	MayContain(ctx context.Context, key []byte) bool

	Stats() FilterStats
	RecordFalsePositive()

	Load(ctx context.Context, store FilterStorer) error
	Save(ctx context.Context, store FilterStorer) error
	Clear(ctx context.Context, store FilterStorer) error
}

type FilterInitializable struct {
	filter        Filter
	isInitialized uint64
}

var _ Filter = &FilterInitializable{}

func NewFilterInitializable(filter Filter) *FilterInitializable {
	return &FilterInitializable{
		filter:        filter,
		isInitialized: 0,
	}
}

func (f *FilterInitializable) IsInitialized() bool {
	return atomic.LoadUint64(&f.isInitialized) == 1
}

func (f *FilterInitializable) Add(ctx context.Context, key []byte) {
	f.filter.Add(ctx, key)
}

func (f *FilterInitializable) MayContain(ctx context.Context, key []byte) bool {
	if atomic.LoadUint64(&f.isInitialized) == 1 {
		return f.filter.MayContain(ctx, key)
	} else {
		return true
	}
}

func (f *FilterInitializable) Stats() FilterStats {
	if atomic.LoadUint64(&f.isInitialized) == 1 {
		return f.filter.Stats()
	} else {
		return FilterStats{}
	}
}

func (f *FilterInitializable) RecordFalsePositive() {
	if atomic.LoadUint64(&f.isInitialized) == 1 {
		f.filter.RecordFalsePositive()
	}
}

func (f *FilterInitializable) Initialize(ctx context.Context, filterStorer FilterStorer, scanners []TableScanner[any]) error {
	err := FilterInitialize(ctx, f.filter, filterStorer, scanners)
	if err != nil {
		return err
	}

	atomic.StoreUint64(&f.isInitialized, 1)
	return nil
}

func (f *FilterInitializable) Load(ctx context.Context, store FilterStorer) error {
	return f.filter.Load(ctx, store)
}

func (f *FilterInitializable) Save(ctx context.Context, store FilterStorer) error {
	if atomic.LoadUint64(&f.isInitialized) == 1 {
		return f.filter.Save(ctx, store)
	} else {
		return fmt.Errorf("filter not initialized")
	}
}

func (f *FilterInitializable) Clear(ctx context.Context, store FilterStorer) error {
	return f.filter.Clear(ctx, store)
}

func FilterInitialize(ctx context.Context, filter Filter, filterStorer FilterStorer, scanners []TableScanner[any]) error {
	err := filter.Load(ctx, filterStorer)
	if err != nil {
		/*err = filter.Clear(ctx, filterStorer)
		if err != nil {
			return fmt.Errorf("filter initialization failed: %w", err)
		}

		for _, scanner := range scanners {
			err = scanner.ScanForEach(ctx, func(keyBytes KeyBytes, lazy Lazy[any]) (bool, error) {
				filter.Add(ctx, keyBytes)
				return true, nil
			}, false)
			if err != nil {
				return fmt.Errorf("filter initialization failed: %w", err)
			}
		}*/
		return err
	}
	return nil
}
