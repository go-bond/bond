package utils

func Copy[T any](in []T) []T {
	out := make([]T, len(in))
	copy(out, in)
	return out
}
