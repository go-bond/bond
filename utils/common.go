package utils

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

func ArrayN(n int) []int {
	arr := make([]int, 0, n)
	for i := 0; i < n; i++ {
		arr = append(arr, i)
	}
	return arr
}

func ClearBytes(in []byte, n int) []byte {
	for i := 0; i < n; i++ {
		in[i] = 0
	}
	return in
}
