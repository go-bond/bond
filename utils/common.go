package utils

import "unsafe"

func Copy[T any](in []T) []T {
	out := make([]T, len(in))
	copy(out, in)
	return out
}

func IntToSlice(in int) []byte {
	return *(*[]byte)(unsafe.Pointer(&in))
}

func SliceToInt(in []byte) int {
	return *(*int)(unsafe.Pointer(&in))
}
