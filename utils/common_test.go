package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDuplicate(t *testing.T) {
	arr := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("c")}
	_, exist := Duplicate(arr)
	require.True(t, exist)
}
