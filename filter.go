package bond

import (
	"context"
	"fmt"
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

func FilterInitialize(ctx context.Context, filter Filter, db DB, scanners []TableScanner[any]) error {
	err := filter.Load(ctx, db)
	if err != nil {
		for _, scanner := range scanners {
			err = scanner.ScanForEach(ctx, func(keyBytes KeyBytes, lazy Lazy[any]) (bool, error) {
				filter.Add(ctx, keyBytes)
				return true, nil
			})
			if err != nil {
				return fmt.Errorf("filter initialization failed: %w", err)
			}
		}

		batch := db.Batch()
		err = filter.Save(ctx, batch)
		if err != nil {
			return fmt.Errorf("filter initialization failed: %w", err)
		}

		err = batch.Commit(Sync)
		if err != nil {
			return fmt.Errorf("filter initialization failed: %w", err)
		}

		_ = batch.Close()
	}
	return nil
}
