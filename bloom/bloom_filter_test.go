package bloom

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type filterStorer struct {
	data map[string][]byte
}

func (f *filterStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	keyData, ok := f.data[cleanKey(key)]
	if !ok {
		return nil, nil, bond.ErrNotFound
	}
	return keyData, io.NopCloser(nil), nil
}

func (f *filterStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	f.data[cleanKey(key)] = valueCopy
	return nil
}

func (f *filterStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	for key, _ := range f.data {
		if strings.HasPrefix(key, cleanKey(start)) {
			delete(f.data, key)
		}

		if strings.Compare(key, string(end)) == 0 {
			break
		}
	}
	return nil
}

func cleanKey(key []byte) string {
	cleanKey := make([]rune, 0, len(key))
	for _, r := range string(key) {
		if r >= 32 && r <= 126 { // Keep printable ASCII characters
			cleanKey = append(cleanKey, r)
		}
	}
	return string(cleanKey)
}

func TestBloomFilter(t *testing.T) {
	filePath := "./tests/test_bloom_filter/"
	defer os.RemoveAll(filePath)

	variants := []struct {
		Name       string
		FileStorer func() bond.FilterStorer
	}{
		{
			Name: "InMemoryFileStorer",
			FileStorer: func() bond.FilterStorer {
				return &filterStorer{
					data: make(map[string][]byte),
				}
			},
		},
		{
			Name: "FSFileStorer",
			FileStorer: func() bond.FilterStorer {
				return filters.NewFSFilterStorer(filePath)
			},
		},
	}

	for _, variant := range variants {
		t.Run(variant.Name+"_BloomFilter_Add_MayContain", func(t *testing.T) {
			bf := NewBloomFilter(10, 0.01, 1000)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			for _, key := range keys {
				assert.True(t, bf.MayContain(context.Background(), key))
			}
		})

		t.Run(variant.Name+"_BloomFilter_Save_Load_Clear", func(t *testing.T) {
			bf := NewBloomFilter(10, 0.01, 1000)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			store := variant.FileStorer()

			// Save the filter to the store
			err := bf.Save(context.Background(), store)
			require.NoError(t, err)

			// Load the filter from the store
			bf2 := NewBloomFilter(10, 0.01, 1000)
			err = bf2.Load(context.Background(), store)
			require.NoError(t, err)

			// Check if the filter contains the keys
			for _, key := range keys {
				assert.True(t, bf2.MayContain(context.Background(), key))
			}

			// Clear the filter
			err = bf2.Clear(context.Background(), store)
			require.NoError(t, err)

			// Try to load the filter again, should fail because the filter is cleared
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)

			// Build a new filter
			bf3 := NewBloomFilter(10, 0.01, 1000)

			keys = KeyGenerate(200, 128)
			for _, key := range keys {
				bf3.Add(context.Background(), key)
			}

			// Try to save a new filter
			err = bf3.Save(context.Background(), store)
			require.NoError(t, err)

			// Try to load the new filter
			err = bf3.Load(context.Background(), store)
			require.NoError(t, err)

			// Check if the filter contains the keys
			for _, key := range keys {
				assert.True(t, bf3.MayContain(context.Background(), key))
			}
		})

		t.Run(variant.Name+"_BloomFilter_Load_Config_Changed", func(t *testing.T) {
			bf := NewBloomFilter(10, 0.01, 1000)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			store := variant.FileStorer()

			err := bf.Save(context.Background(), store)
			require.NoError(t, err)

			bf2 := NewBloomFilter(100, 0.01, 1000)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)

			bf2 = NewBloomFilter(10, 0.02, 1000)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)

			bf2 = NewBloomFilter(10, 0.01, 100)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)
		})

		t.Run(variant.Name+"_BloomFilter_Load_FileCorrupted", func(t *testing.T) {
			bf := NewBloomFilter(100, 0.01, 5)

			keys := KeyGenerate(200, 128)
			for _, key := range keys {
				bf.Add(context.Background(), key)
			}

			store := variant.FileStorer()

			err := bf.Save(context.Background(), store)
			require.NoError(t, err)

			// Corrupt the file
			switch variant.Name {
			case "InMemoryFileStorer":
				store.(*filterStorer).data["bf_0"] = []byte("corrupted")
			case "FSFileStorer":
				err = os.WriteFile(path.Join(filePath, "bf_0"), []byte("corrupted"), 0660)
				require.NoError(t, err)
			}

			bf2 := NewBloomFilter(100, 0.01, 5)
			err = bf2.Load(context.Background(), store)
			require.Error(t, err)
		})
	}
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
