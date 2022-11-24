package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferExtend(t *testing.T) {
	buf := NewBuffer([]byte{})
	buf.Extend(3)
	assert.Equal(t, 3, buf.Len())
	assert.Equal(t, 3, len(buf.Next(3)))
}
