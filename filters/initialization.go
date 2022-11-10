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
