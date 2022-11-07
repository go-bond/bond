package bond

import "context"

type Filter interface {
	Add(ctx context.Context, key []byte)
	AddMany(ctx context.Context, keys [][]byte)

	MayContain(ctx context.Context, key []byte) bool
}
