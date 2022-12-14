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
