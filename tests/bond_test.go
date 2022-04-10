package bond_test

import (
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	mrand.Seed(time.Now().UnixNano())
}

func TestPing(t *testing.T) {
	assert.True(t, true, true)
}
