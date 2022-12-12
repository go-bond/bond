package utils

import "bytes"

func Copy(dst []byte, src []byte) []byte {
	if len(dst) >= len(src) {
		copy(dst, src)
		return dst[:len(src)]
	}
	dst = make([]byte, len(src))
	copy(dst, src)
	return dst
}

type SortShim struct {
	SwapFn func(i, j int)
	Length int
	LessFn func(i, j int) bool
}

func (s *SortShim) Len() int {
	return s.Length
}

func (s *SortShim) Swap(i, j int) {
	s.SwapFn(i, j)
}

func (s *SortShim) Less(i, j int) bool {
	return s.LessFn(i, j)
}

func Duplicate(arr [][]byte) (int, bool) {
	start := 0
	for start < len(arr) && start+1 < len(arr) {
		if bytes.Equal(arr[start], arr[start+1]) {
			return start, true
		}
		start += 2
	}
	return 0, false
}
