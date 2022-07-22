package bond

import (
	"testing"
)

func Benchmark_KeyBuilder(b *testing.B) {
	buffer := make([]byte, 0, 512)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewKeyBuilder(buffer[:0]).
			AddUint32Field(uint32(1)).
			AddBytesField([]byte("0xacd12312jasjjjasjdbasbdsabdab")).
			AddBytesField([]byte("0xacd32121jasjjjasjdbasbdsabdab")).
			Bytes()
	}
}
