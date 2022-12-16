package bond

import (
	"context"

	"github.com/go-bond/bond/utils"
)

type _tableAnyScanner struct {
	scan             func(ctx context.Context, tr *[]any, optBatch ...Batch) error
	scanIndex        func(ctx context.Context, i *Index[any], s any, tr *[]any, optBatch ...Batch) error
	scanForEach      func(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[any]) (bool, error), optBatch ...Batch) ([]any, error)
	scanIndexForEach func(ctx context.Context, idx *Index[any], s any, f func(keyBytes KeyBytes, t Lazy[any]) (bool, error), optBatch ...Batch) ([]any, error)
}

func (a *_tableAnyScanner) Scan(ctx context.Context, tr *[]any, optBatch ...Batch) error {
	return a.scan(ctx, tr, optBatch...)
}

func (a *_tableAnyScanner) ScanIndex(ctx context.Context, i *Index[any], s any, tr *[]any, optBatch ...Batch) error {
	return a.scanIndex(ctx, i, s, tr, optBatch...)
}

func (a *_tableAnyScanner) ScanForEach(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[any]) (bool, error), optBatch ...Batch) ([]any, error) {
	return a.scanForEach(ctx, f, optBatch...)
}

func (a *_tableAnyScanner) ScanIndexForEach(ctx context.Context, idx *Index[any], s any, f func(keyBytes KeyBytes, t Lazy[any]) (bool, error), optBatch ...Batch) ([]any, error) {
	return a.scanIndexForEach(ctx, idx, s, f, optBatch...)
}

func TableAnyScanner[T any](scanner TableScanner[T]) TableScanner[any] {
	return &_tableAnyScanner{
		scan: func(ctx context.Context, tr *[]any, optBatch ...Batch) error {
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
		scanIndex: func(ctx context.Context, i *Index[any], s any, tr *[]any, optBatch ...Batch) error {
			var ttr []T

			iAny := NewIndex[T](IndexOptions[T]{
				IndexID:   i.IndexID,
				IndexName: i.IndexName,
				IndexKeyFunc: func(keyBuilder KeyBuilder, t T) []byte {
					return i.IndexKeyFunction(keyBuilder, t)
				},
				IndexOrderFunc: func(order IndexOrder, t T) IndexOrder {
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
		scanForEach: func(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[any]) (bool, error), optBatch ...Batch) ([]any, error) {
			results, err := scanner.ScanForEach(ctx, func(keyBytes KeyBytes, l Lazy[T]) (bool, error) {
				return f(keyBytes, Lazy[any]{
					GetFunc: func() (any, error) {
						return l.Get()
					},
					BufferFunc: func() error {
						return l.BufferFunc()
					},
					EmitFunc: func() ([]any, error) {
						results, err := l.EmitFunc()
						return utils.ToSliceAny[T](results), err
					},
				})
			})
			return utils.ToSliceAny[T](results), err
		},
		scanIndexForEach: func(ctx context.Context, i *Index[any], s any, f func(keyBytes KeyBytes, t Lazy[any]) (bool, error), optBatch ...Batch) ([]any, error) {
			iAny := NewIndex[T](IndexOptions[T]{
				IndexID:   i.IndexID,
				IndexName: i.IndexName,
				IndexKeyFunc: func(keyBuilder KeyBuilder, t T) []byte {
					return i.IndexKeyFunction(keyBuilder, t)
				},
				IndexOrderFunc: func(order IndexOrder, t T) IndexOrder {
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

			results, err := scanner.ScanIndexForEach(ctx, iAny, s.(T), func(keyBytes KeyBytes, l Lazy[T]) (bool, error) {
				return f(keyBytes, Lazy[any]{
					GetFunc: func() (any, error) {
						return l.Get()
					},
					BufferFunc: func() error {
						return l.BufferFunc()
					},
					EmitFunc: func() ([]any, error) {
						results, err := l.EmitFunc()
						return utils.ToSliceAny[T](results), err
					},
				})
			})
			return utils.ToSliceAny[T](results), err
		},
	}
}
