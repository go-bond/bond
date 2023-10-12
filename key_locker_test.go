package bond

import "testing"

func TestKeyLocker(t *testing.T) {
	locker := NewLocker()
	key := []byte("hello")
	locker.RLockKey(key)
	locker.RUnlockKey(key)
}
