package filters

import (
	"context"

	"github.com/go-bond/bond"
)

type InitializableFilter interface {
	bond.Filter

	IsInitialized() bool
	SetInitialized(isInitialized bool)
}

func InitializeFilter(ctx context.Context, filter InitializableFilter, scanners []bond.TableScanner[any]) error {
	if !filter.IsInitialized() {
		filter.SetInitialized(true)
		for _, scanner := range scanners {
			err := scanner.ScanForEach(ctx, func(keyBytes bond.KeyBytes, lazy bond.Lazy[any]) (bool, error) {
				filter.Add(ctx, keyBytes)
				return true, nil
			})
			if err != nil {
				filter.SetInitialized(false)
				return err
			}
		}
	}
	return nil
}

type _anyTableScanner struct {
	scan             func(ctx context.Context, tr *[]any, optBatch ...bond.Batch) error
	scanIndex        func(ctx context.Context, i *bond.Index[any], s any, tr *[]any, optBatch ...bond.Batch) error
	scanForEach      func(ctx context.Context, f func(keyBytes bond.KeyBytes, l bond.Lazy[any]) (bool, error), optBatch ...bond.Batch) error
	scanIndexForEach func(ctx context.Context, idx *bond.Index[any], s any, f func(keyBytes bond.KeyBytes, t bond.Lazy[any]) (bool, error), optBatch ...bond.Batch) error
}

func (a *_anyTableScanner) Scan(ctx context.Context, tr *[]any, optBatch ...bond.Batch) error {
	return a.scan(ctx, tr, optBatch...)
}

func (a *_anyTableScanner) ScanIndex(ctx context.Context, i *bond.Index[any], s any, tr *[]any, optBatch ...bond.Batch) error {
	return a.scanIndex(ctx, i, s, tr, optBatch...)
}

func (a *_anyTableScanner) ScanForEach(ctx context.Context, f func(keyBytes bond.KeyBytes, l bond.Lazy[any]) (bool, error), optBatch ...bond.Batch) error {
	return a.scanForEach(ctx, f, optBatch...)
}

func (a *_anyTableScanner) ScanIndexForEach(ctx context.Context, idx *bond.Index[any], s any, f func(keyBytes bond.KeyBytes, t bond.Lazy[any]) (bool, error), optBatch ...bond.Batch) error {
	return a.scanIndexForEach(ctx, idx, s, f, optBatch...)
}

func AnyTableScanner[T any](scanner bond.TableScanner[T]) bond.TableScanner[any] {
	return &_anyTableScanner{
		scan: func(ctx context.Context, tr *[]any, optBatch ...bond.Batch) error {
			var ttr []T
			err := scanner.Scan(ctx, &ttr, optBatch...)
			if err != nil {
				return err
			}

			for _, t := range ttr {
				*tr = append(*tr, t)
			}

			return nil
		},
		scanIndex: func(ctx context.Context, i *bond.Index[any], s any, tr *[]any, optBatch ...bond.Batch) error {
			var ttr []T

			iAny := bond.NewIndex[T](bond.IndexOptions[T]{
				IndexID:   i.IndexID,
				IndexName: i.IndexName,
				IndexKeyFunc: func(keyBuilder bond.KeyBuilder, t T) []byte {
					return i.IndexKeyFunction(keyBuilder, t)
				},
				IndexOrderFunc: func(order bond.IndexOrder, t T) bond.IndexOrder {
					if i.IndexOrderFunction == nil {
						return order
					}
					return i.IndexOrderFunction(order, t)
				},
				IndexFilterFunc: func(t T) bool {
					if i.IndexFilterFunction == nil {
						return true
					}
					return i.IndexFilterFunction(t)
				},
			})

			err := scanner.ScanIndex(ctx, iAny, s.(T), &ttr, optBatch...)
			if err != nil {
				return err
			}

			for _, t := range ttr {
				*tr = append(*tr, t)
			}

			return nil
		},
		scanForEach: func(ctx context.Context, f func(keyBytes bond.KeyBytes, l bond.Lazy[any]) (bool, error), optBatch ...bond.Batch) error {
			return scanner.ScanForEach(ctx, func(keyBytes bond.KeyBytes, l bond.Lazy[T]) (bool, error) {
				return f(keyBytes, bond.Lazy[any]{
					GetFunc: func() (any, error) {
						return l.Get()
					},
				})
			})
		},
		scanIndexForEach: func(ctx context.Context, i *bond.Index[any], s any, f func(keyBytes bond.KeyBytes, t bond.Lazy[any]) (bool, error), optBatch ...bond.Batch) error {
			iAny := bond.NewIndex[T](bond.IndexOptions[T]{
				IndexID:   i.IndexID,
				IndexName: i.IndexName,
				IndexKeyFunc: func(keyBuilder bond.KeyBuilder, t T) []byte {
					return i.IndexKeyFunction(keyBuilder, t)
				},
				IndexOrderFunc: func(order bond.IndexOrder, t T) bond.IndexOrder {
					if i.IndexOrderFunction == nil {
						return order
					}
					return i.IndexOrderFunction(order, t)
				},
				IndexFilterFunc: func(t T) bool {
					if i.IndexFilterFunction == nil {
						return true
					}
					return i.IndexFilterFunction(t)
				},
			})

			return scanner.ScanIndexForEach(ctx, iAny, s.(T), func(keyBytes bond.KeyBytes, l bond.Lazy[T]) (bool, error) {
				return f(keyBytes, bond.Lazy[any]{
					GetFunc: func() (any, error) {
						return l.Get()
					},
				})
			})
		},
	}
}
