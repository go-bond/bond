package bond

import "context"

type Filter interface {
	Add(ctx context.Context, key []byte)

	MayContain(ctx context.Context, key []byte) bool
}
