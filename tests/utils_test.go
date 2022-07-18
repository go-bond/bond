package tests

import (
	"testing"

	"github.com/go-bond/bond"
)

func Benchmark_KeyBuilder(b *testing.B) {
	buffer := make([]byte, 0, 512)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bond.NewKeyBuilder(buffer[:0]).
			AddUint32Field(uint32(1)).
			AddBytesField([]byte("0xacd12312jasjjjasjdbasbdsabdab")).
			AddBytesField([]byte("0xacd32121jasjjjasjdbasbdsabdab")).
			Bytes()
	}
}
