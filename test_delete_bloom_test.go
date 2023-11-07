package bond

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

type BlockIndexValueCollector struct {
	BlockCollector     *bloom.BloomFilter
	IndexCollector     *bloom.BloomFilter
	PrevBlockCollector *bloom.BloomFilter
	TableCollector     *bloom.BloomFilter
}

func (b *BlockIndexValueCollector) Name() string {
	return "block_index_value_collector"
}

func (b *BlockIndexValueCollector) Add(key sstable.InternalKey, value []byte) error {
	if b.BlockCollector == nil {
		b.BlockCollector = bloom.NewWithEstimates(1000, 0.001)
	}
	if b.TableCollector == nil {
		b.TableCollector = bloom.NewWithEstimates(1000, 0.001)
	}
	b.BlockCollector.Add(value)
	b.TableCollector.Add(value)
	return nil
}

func (b *BlockIndexValueCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(buf)
	_, err := b.BlockCollector.WriteTo(buffer)
	if err != nil {
		return nil, err
	}
	b.PrevBlockCollector = b.BlockCollector
	b.BlockCollector = bloom.NewWithEstimates(1000, 0.001)
	return buffer.Bytes(), nil
}

func (b *BlockIndexValueCollector) AddPrevDataBlockToIndexBlock() {
	if b.IndexCollector == nil {
		b.IndexCollector = bloom.NewWithEstimates(1000, 0.001)
	}
	if b.PrevBlockCollector != nil {
		b.IndexCollector.Merge(b.PrevBlockCollector)
	}
}

func (b *BlockIndexValueCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(buf)
	_, err := b.IndexCollector.WriteTo(buffer)
	if err != nil {
		return nil, err
	}
	b.IndexCollector = bloom.NewWithEstimates(1000, 0.001)
	return buffer.Bytes(), nil
}

func (b *BlockIndexValueCollector) FinishTable(buf []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(buf)
	_, err := b.TableCollector.WriteTo(buffer)
	if err != nil {
		return nil, err
	}
	b.TableCollector = bloom.NewWithEstimates(1000, 0.001)
	return buffer.Bytes(), nil
}

type BlockIndexValueFilter struct {
	Values []byte
}

func (b BlockIndexValueFilter) Name() string {
	return "block_index_value_collector"
}

func (b BlockIndexValueFilter) Intersects(props []byte) (bool, error) {
	// if len(props) == 0 {
	// 	return true, nil
	// }
	bf := &bloom.BloomFilter{}
	_, err := bf.ReadFrom(bytes.NewBuffer(props))
	if err != nil {
		return false, err
	}
	exist := bf.Test(b.Values)
	return exist, nil
}

func TestDeleteSurface(t *testing.T) {
	opt := DefaultOptions()
	opt.PebbleOptions.Comparer = pebble.DefaultComparer
	opt.PebbleOptions.DisableAutomaticCompactions = true
	opt.PebbleOptions.BlockPropertyCollectors = []func() pebble.BlockPropertyCollector{func() pebble.BlockPropertyCollector {
		return &BlockIndexValueCollector{}
	}}
	opt.PebbleOptions.FormatMajorVersion = pebble.FormatBlockPropertyCollector
	db, err := Open(".db", opt)
	require.NoError(t, err)
	pdb := db.Backend()
	for i := 0; i < 100; i++ {
		pdb.Set([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)), pebble.Sync)
	}
	pdb.Flush()
	itr, err := pdb.NewIter(&pebble.IterOptions{
		PointKeyFilters: []pebble.BlockPropertyFilter{
			&BlockIndexValueFilter{Values: []byte("50")},
		},
	})
	require.NoError(t, err)
	exist := false
	for itr.First(); itr.Valid(); itr.Next() {
		if string(itr.Value()) == "50" {
			exist = true
		}
	}
	require.True(t, exist, "50 supposed to be exist")

	// delete 50 entries.
	for i := 50; i < 100; i++ {
		pdb.Delete([]byte(fmt.Sprintf("%d", i)), pebble.Sync)
	}
	pdb.Flush()
	itr, err = pdb.NewIter(&pebble.IterOptions{
		PointKeyFilters: []pebble.BlockPropertyFilter{
			&BlockIndexValueFilter{Values: []byte("50")},
		},
	})
	require.NoError(t, err)
	exist = false
	for itr.First(); itr.Valid(); itr.Next() {
		if string(itr.Value()) == "50" {
			exist = true
		}
	}
	if exist {
		fmt.Println("50 supposed to be deleted")
	}
	db.Close()
	os.RemoveAll(".db")
}
