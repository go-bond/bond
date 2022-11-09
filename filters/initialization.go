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

func InitializeFilter[T any](ctx context.Context, filter InitializableFilter, scanner bond.TableScanner[T]) error {
	if !filter.IsInitialized() {
		filter.SetInitialized(true)
		err := scanner.ScanForEach(ctx, func(keyBytes bond.KeyBytes, lazy bond.Lazy[T]) (bool, error) {
			filter.Add(ctx, keyBytes)
			return true, nil
		})
		if err != nil {
			filter.SetInitialized(false)
			return err
		}
	}
	return nil
}
