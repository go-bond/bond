package bond

import (
	"sync"

	"github.com/go-bond/bond/utils"
)

type KeyLocker struct {
	readLock  map[string]int
	writeLock map[string]struct{}
	cond      *sync.Cond
}

func NewLocker() *KeyLocker {
	return &KeyLocker{
		readLock:  map[string]int{},
		writeLock: map[string]struct{}{},
		cond:      sync.NewCond(&sync.Mutex{}),
	}
}

func (k *KeyLocker) LockKey(key []byte) {
	k.cond.L.Lock()
	defer k.cond.L.Unlock()
	for {
		_, ok := k.readLock[utils.BytesToString(key)]
		if ok {
			k.cond.Wait()
			continue
		}
		break
	}
	k.writeLock[utils.BytesToString(key)] = struct{}{}
}

func (k *KeyLocker) UnlockKey(key []byte) {
	k.cond.L.Lock()
	defer k.cond.L.Unlock()
	delete(k.writeLock, utils.BytesToString(key))
	k.cond.Broadcast()
}

func (k *KeyLocker) RLockKey(key []byte) {
	k.cond.L.Lock()
	defer k.cond.L.Unlock()
	for {
		_, ok := k.writeLock[utils.BytesToString(key)]
		if ok {
			k.cond.Wait()
			continue
		}
		break
	}
	count, ok := k.readLock[utils.BytesToString(key)]
	if ok {
		count++
		k.readLock[utils.BytesToString(key)] = count
		return
	}
	k.readLock[utils.BytesToString(key)] = 1
}

func (k *KeyLocker) RUnlockKey(key []byte) {
	k.cond.L.Lock()
	defer k.cond.L.Unlock()
	count, ok := k.readLock[utils.BytesToString(key)]
	if !ok {
		panic("something went wrong")
	}
	count--
	if count == 0 {
		delete(k.readLock, utils.BytesToString(key))
		k.cond.Broadcast()
		return
	}
	k.readLock[utils.BytesToString(key)] = count
}
