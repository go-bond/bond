package bond

import "sync"

type KeyLocker struct {
	keys map[string]struct{}
	cond *sync.Cond
}
