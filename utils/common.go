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
