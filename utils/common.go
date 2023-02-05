package utils

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
	arr := make([]int, n)
	for i := 0; i < n; i++ {
		arr[i] = i
	}
	return arr
}
