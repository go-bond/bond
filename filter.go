package bond

import (
	"context"
	"fmt"
	"log/slog"
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
	slog.Info("bond: FilterInitializable.Initialize: starting filter initialization",
		"num_scanners", len(scanners))

	err := FilterInitialize(ctx, f.filter, filterStorer, scanners)
	if err != nil {
		slog.Info("bond: FilterInitializable.Initialize: initialization failed",
			"error", err)
		return err
	}

	atomic.StoreUint64(&f.isInitialized, 1)
	slog.Info("bond: FilterInitializable.Initialize: filter successfully initialized",
		"isInitialized", true)
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
	slog.Info("bond: FilterInitialize: attempting to load existing filter")

	err := filter.Load(ctx, filterStorer)
	if err != nil {
		slog.Info("bond: FilterInitialize: failed to load filter, will rebuild",
			"error", err,
			"error_type", fmt.Sprintf("%T", err))

		slog.Info("bond: FilterInitialize: clearing existing filter data")
		err = filter.Clear(ctx, filterStorer)
		if err != nil {
			slog.Info("bond: FilterInitialize: failed to clear filter",
				"error", err)
			return fmt.Errorf("filter initialization failed: %w", err)
		}
		slog.Info("bond: FilterInitialize: filter cleared successfully")

		slog.Info("bond: FilterInitialize: rebuilding filter from scratch",
			"num_scanners", len(scanners))

		totalKeysAdded := 0
		for scannerIdx, scanner := range scanners {
			slog.Info("bond: FilterInitialize: scanning table",
				"scanner_index", scannerIdx,
				"total_scanners", len(scanners))

			keysAddedInThisScanner := 0
			err = scanner.ScanForEach(ctx, func(keyBytes KeyBytes, lazy Lazy[any]) (bool, error) {
				filter.Add(ctx, keyBytes)
				keysAddedInThisScanner++
				totalKeysAdded++

				if totalKeysAdded%1e6 == 0 {
					slog.Info("bond: FilterInitialize: scan progress",
						"scanner_index", scannerIdx,
						"keys_added_total", totalKeysAdded,
						"keys_in_current_scanner", keysAddedInThisScanner)
				}

				return true, nil
			}, false)

			if err != nil {
				slog.Info("bond: FilterInitialize: scanner failed",
					"scanner_index", scannerIdx,
					"keys_added_in_scanner", keysAddedInThisScanner,
					"total_keys_added", totalKeysAdded,
					"error", err)
				return fmt.Errorf("filter initialization failed: %w", err)
			}

			slog.Info("bond: FilterInitialize: scanner completed",
				"scanner_index", scannerIdx,
				"keys_added_in_scanner", keysAddedInThisScanner,
				"total_keys_added", totalKeysAdded)
		}

		slog.Info("bond: FilterInitialize: all scanners completed",
			"total_keys_added", totalKeysAdded)

		slog.Info("bond: FilterInitialize: saving rebuilt filter")
		err = filter.Save(ctx, filterStorer)
		if err != nil {
			slog.Info("bond: FilterInitialize: failed to save rebuilt filter",
				"error", err,
				"total_keys_added", totalKeysAdded)
			return fmt.Errorf("filter initialization failed to save: %w", err)
		}
		slog.Info("bond: FilterInitialize: rebuilt filter saved successfully",
			"total_keys_added", totalKeysAdded)
	} else {
		slog.Info("bond: FilterInitialize: filter loaded successfully from storage")
	}

	return nil
}
