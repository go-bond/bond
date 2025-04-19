package bond

import (
	"context"
	"fmt"
	"sync/atomic"
)

type FilterStorer interface {
	Getter
	Setter
	DeleterWithRange
}

type Filter interface {
	Add(ctx context.Context, key []byte)
	MayContain(ctx context.Context, key []byte) bool

	Load(ctx context.Context, store FilterStorer) error
	Save(ctx context.Context, store FilterStorer) error
	Clear(ctx context.Context, store FilterStorer) error
}

type FilterInitializable struct {
	Filter
	isInitialized uint64
}

func (f *FilterInitializable) MayContain(ctx context.Context, key []byte) bool {
	if atomic.LoadUint64(&f.isInitialized) == 1 {
		return f.Filter.MayContain(ctx, key)
	} else {
		return true
	}
}

func (f *FilterInitializable) Initialize(ctx context.Context, filterStorer FilterStorer, scanners []TableScanner[any]) error {
	err := FilterInitialize(ctx, f.Filter, filterStorer, scanners)
	if err != nil {
		return err
	}

	atomic.StoreUint64(&f.isInitialized, 1)
	return nil
}

func (f *FilterInitializable) Save(ctx context.Context, store FilterStorer) error {
	if atomic.LoadUint64(&f.isInitialized) == 1 {
		return f.Filter.Save(ctx, store)
	} else {
		return fmt.Errorf("filter not initialized")
	}
}

func FilterInitialize(ctx context.Context, filter Filter, filterStorer FilterStorer, scanners []TableScanner[any]) error {
	err := filter.Load(ctx, filterStorer)
	if err != nil {
		err = filter.Clear(ctx, filterStorer)
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
		}
	}
	return nil
}
