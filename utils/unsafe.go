package utils

import "unsafe"

// StringToBytes converts a string to a byte slice without copying.
// IMPORTANT: The returned byte slice must NOT be modified, as this will
// corrupt the original string. Only use this for READ-ONLY operations.
func StringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BytesToString converts a byte slice to a string without copying.
// IMPORTANT: The original byte slice should not be modified after this conversion.
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
