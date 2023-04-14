package bond

import "context"

type _tableAnyScanner struct {
	scan             func(ctx context.Context, tr *[]any, reverse bool, optBatch ...Batch) error
	scanIndex        func(ctx context.Context, i *Index[any], s Selector[any], tr *[]any, reverse bool, optBatch ...Batch) error
	scanForEach      func(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[any]) (bool, error), reverse bool, optBatch ...Batch) error
	scanIndexForEach func(ctx context.Context, idx *Index[any], s Selector[any], f func(keyBytes KeyBytes, t Lazy[any]) (bool, error), reverse bool, optBatch ...Batch) error
}

func (a *_tableAnyScanner) Scan(ctx context.Context, tr *[]any, reverse bool, optBatch ...Batch) error {
	return a.scan(ctx, tr, reverse, optBatch...)
}

func (a *_tableAnyScanner) ScanIndex(ctx context.Context, i *Index[any], s Selector[any], tr *[]any, reverse bool, optBatch ...Batch) error {
	return a.scanIndex(ctx, i, s, tr, reverse, optBatch...)
}

func (a *_tableAnyScanner) ScanForEach(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[any]) (bool, error), reverse bool, optBatch ...Batch) error {
	return a.scanForEach(ctx, f, reverse, optBatch...)
}

func (a *_tableAnyScanner) ScanIndexForEach(ctx context.Context, idx *Index[any], s Selector[any], f func(keyBytes KeyBytes, t Lazy[any]) (bool, error), reverse bool, optBatch ...Batch) error {
	return a.scanIndexForEach(ctx, idx, s, f, reverse, optBatch...)
}

func TableAnyScanner[T any](scanner TableScanner[T]) TableScanner[any] {
	return &_tableAnyScanner{
		scan: func(ctx context.Context, tr *[]any, reverse bool, optBatch ...Batch) error {
			var ttr []T
			err := scanner.Scan(ctx, &ttr, reverse, optBatch...)
			if err != nil {
				return err
			}

			for _, t := range ttr {
				*tr = append(*tr, t)
			}

			return nil
		},
		scanIndex: func(ctx context.Context, i *Index[any], s Selector[any], tr *[]any, reverse bool, optBatch ...Batch) error {
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

			err := scanner.ScanIndex(ctx, iAny, s.(Selector[T]), &ttr, reverse, optBatch...)
			if err != nil {
				return err
			}

			for _, t := range ttr {
				*tr = append(*tr, t)
			}

			return nil
		},
		scanForEach: func(ctx context.Context, f func(keyBytes KeyBytes, l Lazy[any]) (bool, error), reverse bool, optBatch ...Batch) error {
			return scanner.ScanForEach(ctx, func(keyBytes KeyBytes, l Lazy[T]) (bool, error) {
				return f(keyBytes, Lazy[any]{
					GetFunc: func() (any, error) {
						return l.Get()
					},
				})
			}, reverse)
		},
		scanIndexForEach: func(ctx context.Context, i *Index[any], s Selector[any], f func(keyBytes KeyBytes, t Lazy[any]) (bool, error), reverse bool, optBatch ...Batch) error {
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

			return scanner.ScanIndexForEach(ctx, iAny, s.(Selector[T]), func(keyBytes KeyBytes, l Lazy[T]) (bool, error) {
				return f(keyBytes, Lazy[any]{
					GetFunc: func() (any, error) {
						return l.Get()
					},
				})
			}, reverse)
		},
	}
}
