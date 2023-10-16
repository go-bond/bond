package bloom

import (
	"context"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/go-bond/bond"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type filterStorer struct {
	data map[string][]byte
}

func (f *filterStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	keyData, ok := f.data[string(key)]
	if !ok {
		return nil, nil, bond.ErrNotFound
	}
	return keyData, io.NopCloser(nil), nil
}

func (f *filterStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	f.data[string(key)] = valueCopy
	return nil
}

func (f *filterStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	for key, _ := range f.data {
		if strings.HasPrefix(key, string(start)) {
			delete(f.data, key)
		}

		if strings.Compare(key, string(end)) == 0 {
			break
		}
	}
	return nil
}

func TestBloomFilter_Add_MayContain(t *testing.T) {
	bf := NewBloomFilter(10, 0.01, 1000)

	keys := KeyGenerate(200, 128)
	for _, key := range keys {
		bf.Add(context.Background(), key)
	}

	for _, key := range keys {
		assert.True(t, bf.MayContain(context.Background(), key))
	}
}

func TestBloomFilter_Save_Load_Clear(t *testing.T) {
	bf := NewBloomFilter(10, 0.01, 1000)

	keys := KeyGenerate(200, 128)
	for _, key := range keys {
		bf.Add(context.Background(), key)
	}

	store := filterStorer{
		data: make(map[string][]byte),
	}

	err := bf.Save(context.Background(), &store)
	require.NoError(t, err)

	bf2 := NewBloomFilter(10, 0.01, 1000)
	err = bf2.Load(context.Background(), &store)
	require.NoError(t, err)

	for _, key := range keys {
		assert.True(t, bf2.MayContain(context.Background(), key))
	}

	err = bf2.Clear(context.Background(), &store)
	require.NoError(t, err)

	err = bf2.Load(context.Background(), &store)
	require.Error(t, err)
}

func TestBloomFilter_Load_Config_Changed(t *testing.T) {
	bf := NewBloomFilter(10, 0.01, 1000)

	keys := KeyGenerate(200, 128)
	for _, key := range keys {
		bf.Add(context.Background(), key)
	}

	store := filterStorer{
		data: make(map[string][]byte),
	}

	err := bf.Save(context.Background(), &store)
	require.NoError(t, err)

	bf2 := NewBloomFilter(100, 0.01, 1000)
	err = bf2.Load(context.Background(), &store)
	require.Error(t, err)

	bf2 = NewBloomFilter(10, 0.02, 1000)
	err = bf2.Load(context.Background(), &store)
	require.Error(t, err)

	bf2 = NewBloomFilter(10, 0.01, 100)
	err = bf2.Load(context.Background(), &store)
	require.Error(t, err)
}

func KeyGenerate(n int, keySize int) (ret [][]byte) {
	for i := 0; i < n; i++ {
		ret = append(ret, []byte(RandomString(keySize)))
	}
	return
}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
