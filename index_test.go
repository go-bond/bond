package bond

import (
	"bytes"
	"math/big"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStructure struct {
	ID          int
	Name        string
	Description string
}

func TestIndexOrder_Single_Int64_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderInt64(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt64(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt64(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt64(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt64(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int32_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderInt32(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt32(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt32(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt32(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt32(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int16_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderInt16(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt16(-1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt16(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt16(-10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt16(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int64_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderInt64(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt64(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt64(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt64(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt64(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int32_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderInt32(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt32(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt32(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt32(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt32(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Int16_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderInt16(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderInt16(-1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderInt16(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderInt16(-10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderInt16(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint64_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderUint64(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint64(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint64(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint64(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint64(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint32_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderUint32(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint32(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint32(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint32(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint32(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint16_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderUint16(5, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint16(1, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint16(7, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint16(10, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint16(2, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint64_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderUint64(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint64(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint64(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint64(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint64(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint32_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderUint32(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint32(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint32(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint32(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint32(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_Uint16_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderUint16(5, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderUint16(1, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderUint16(7, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderUint16(10, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderUint16(2, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_BigInt256_ASC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderBigInt(big.NewInt(5), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderBigInt(big.NewInt(-1), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderBigInt(big.NewInt(7), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderBigInt(big.NewInt(-10), 256, IndexOrderTypeASC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderBigInt(big.NewInt(2), 256, IndexOrderTypeASC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey4,
		indexOrderKey2,
		indexOrderKey5,
		indexOrderKey1,
		indexOrderKey3,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Single_BigInt256_DESC(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.OrderBigInt(big.NewInt(5), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.OrderBigInt(big.NewInt(-1), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.OrderBigInt(big.NewInt(7), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.OrderBigInt(big.NewInt(-10), 256, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.OrderBigInt(big.NewInt(2), 256, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey3,
		indexOrderKey1,
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey4,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}

func TestIndexOrder_Multi(t *testing.T) {
	indexOrderKey1 := IndexOrder{}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(50, IndexOrderTypeDESC).Bytes()
	indexOrderKey2 := IndexOrder{}.
		OrderUint64(1, IndexOrderTypeASC).
		OrderUint64(100, IndexOrderTypeDESC).Bytes()
	indexOrderKey3 := IndexOrder{}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(100, IndexOrderTypeDESC).Bytes()
	indexOrderKey4 := IndexOrder{}.
		OrderUint64(2, IndexOrderTypeASC).
		OrderUint64(90, IndexOrderTypeDESC).Bytes()
	indexOrderKey5 := IndexOrder{}.
		OrderUint64(1, IndexOrderTypeASC).
		OrderUint64(100000000, IndexOrderTypeDESC).Bytes()

	keyList := [][]byte{
		indexOrderKey1,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey5,
	}

	expectedKeyList := [][]byte{
		indexOrderKey5,
		indexOrderKey2,
		indexOrderKey3,
		indexOrderKey4,
		indexOrderKey1,
	}

	sort.Slice(keyList, func(i, j int) bool {
		return bytes.Compare(keyList[i], keyList[j]) == -1
	})

	assert.Equal(t, expectedKeyList, keyList)
}
