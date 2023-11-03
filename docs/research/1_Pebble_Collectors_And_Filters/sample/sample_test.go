package sample

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
)

type BlockIndexCollector struct {
	TableKeys     *bloom.BloomFilter
	IndexKeys     *bloom.BloomFilter
	BlockKeys     *bloom.BloomFilter
	PrevBlockKeys *bloom.BloomFilter
}

func (b *BlockIndexCollector) Name() string {
	return "index_bloom"
}

func (b *BlockIndexCollector) Add(key sstable.InternalKey, value []byte) error {
	if b.BlockKeys == nil {
		b.BlockKeys = bloom.NewWithEstimates(1000, 0.001)
	}
	if b.TableKeys == nil {
		b.TableKeys = bloom.NewWithEstimates(1000, 0.001)
	}

	parts := bytes.Split(key.UserKey, []byte("."))
	if length := len(parts); length > 1 {
		b.BlockKeys.Add(parts[length-1])
		b.TableKeys.Add(parts[length-1])
	}
	return nil
}

func (b *BlockIndexCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	bytesBuff := bytes.NewBuffer(buf)
	_, err := b.BlockKeys.WriteTo(bytesBuff)
	if err != nil {
		return nil, err
	}

	b.PrevBlockKeys = b.BlockKeys
	b.BlockKeys = bloom.NewWithEstimates(1000, 0.001)

	return bytesBuff.Bytes(), nil
}

func (b *BlockIndexCollector) AddPrevDataBlockToIndexBlock() {
	if b.IndexKeys == nil {
		b.IndexKeys = bloom.NewWithEstimates(1000, 0.001)
	}
	if b.PrevBlockKeys != nil {
		_ = b.IndexKeys.Merge(b.PrevBlockKeys)
	}
}

func (b *BlockIndexCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	bytesBuff := bytes.NewBuffer(buf)
	_, err := b.IndexKeys.WriteTo(bytesBuff)
	if err != nil {
		return nil, err
	}

	b.IndexKeys = bloom.NewWithEstimates(1000, 0.001)

	return bytesBuff.Bytes(), nil
}

func (b *BlockIndexCollector) FinishTable(buf []byte) ([]byte, error) {
	bytesBuff := bytes.NewBuffer(buf)
	_, err := b.TableKeys.WriteTo(bytesBuff)
	if err != nil {
		return nil, err
	}

	b.TableKeys = bloom.NewWithEstimates(1000, 0.001)

	return bytesBuff.Bytes(), nil
}

type BlockIndexFilter struct {
	Value []byte
}

func (b BlockIndexFilter) Name() string {
	return "index_bloom"
}

func (b BlockIndexFilter) Intersects(prop []byte) (bool, error) {
	bf := &bloom.BloomFilter{}
	_, err := bf.ReadFrom(bytes.NewBuffer(prop))
	if err != nil {
		return false, err
	}

	tr := bf.Test(b.Value)
	if tr {
		//fmt.Println("includes")
	} else {
		//fmt.Println("not includes")
	}

	return tr, nil
}

func TestBlockFilter_EqualDist(t *testing.T) {
	opt := bond.DefaultPebbleOptions()

	opt.FormatMajorVersion = pebble.FormatNewest
	opt.Comparer = pebble.DefaultComparer

	opt.BlockPropertyCollectors = []func() pebble.BlockPropertyCollector{
		func() pebble.BlockPropertyCollector {
			return &BlockIndexCollector{}
		},
	}

	db, err := pebble.Open(".db", opt)
	require.NoError(t, err)

	dummyData := []byte("dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, ")

	fmt.Println("equal distribution:")
	names := []string{"one", "two", "three", "four", "five", "not_exist_in_db"}
	for i := 0; i < 1000000; i++ {
		err = db.Set([]byte(fmt.Sprintf("R.%.7d.name.%s", i, names[i%5])), dummyData, pebble.NoSync)
		require.NoError(t, err)
	}

	t1 := time.Now()
	it, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("R.0"),
		UpperBound: []byte("R.9"),
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
	})
	require.NoError(t, err)

	count := 0
	for it.First(); it.Valid(); it.Next() {
		hasPoint, hasRange := it.HasPointAndRange()
		if hasPoint {
			//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
		}
		if hasRange {
			s, e := it.RangeBounds()
			kd := it.RangeKeys()
			t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
		}

		count++
	}
	fmt.Printf("iterate no filter, took %s items %d\n", time.Since(t1).String(), count)
	it.Close()

	for _, name := range names {
		t1 := time.Now()
		it, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte("R.0"),
			UpperBound: []byte("R.9"),
			KeyTypes:   pebble.IterKeyTypePointsAndRanges,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&BlockIndexFilter{Value: []byte(name)},
			},
		})
		require.NoError(t, err)

		count := 0
		for it.First(); it.Valid(); it.Next() {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasPoint {
				//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
			}
			if hasRange {
				s, e := it.RangeBounds()
				kd := it.RangeKeys()
				t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
			}

			count++
		}
		fmt.Printf("iterate filter name: %s, took %s items %d\n", name, time.Since(t1).String(), count)
		it.Close()
	}

	fmt.Println("compact db:")
	_ = db.Compact([]byte("R.0"), []byte("R.9"), true)

	t1 = time.Now()
	it, err = db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("R.0"),
		UpperBound: []byte("R.9"),
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
	})
	require.NoError(t, err)

	count = 0
	for it.First(); it.Valid(); it.Next() {
		hasPoint, hasRange := it.HasPointAndRange()
		if hasPoint {
			//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
		}
		if hasRange {
			s, e := it.RangeBounds()
			kd := it.RangeKeys()
			t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
		}

		count++
	}
	fmt.Printf("iterate no filter, took %s items %d\n", time.Since(t1).String(), count)
	it.Close()

	for _, name := range names {
		t1 := time.Now()
		it, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte("R.0"),
			UpperBound: []byte("R.9"),
			KeyTypes:   pebble.IterKeyTypePointsAndRanges,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&BlockIndexFilter{Value: []byte(name)},
			},
		})
		require.NoError(t, err)

		count := 0
		for it.First(); it.Valid(); it.Next() {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasPoint {
				//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
			}
			if hasRange {
				s, e := it.RangeBounds()
				kd := it.RangeKeys()
				t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
			}

			count++
		}
		fmt.Printf("iterate filter name: %s, took %s items %d\n", name, time.Since(t1).String(), count)
		it.Close()
	}

	db.Close()

	os.RemoveAll(".db")
}

func TestBlockFilter_SequDist(t *testing.T) {
	opt := bond.DefaultPebbleOptions()

	opt.FormatMajorVersion = pebble.FormatNewest
	opt.Comparer = pebble.DefaultComparer

	opt.BlockPropertyCollectors = []func() pebble.BlockPropertyCollector{
		func() pebble.BlockPropertyCollector {
			return &BlockIndexCollector{}
		},
	}

	db, err := pebble.Open(".db", opt)
	require.NoError(t, err)

	dummyData := []byte("dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, dummy data, ")

	fmt.Println("sequential distribution:")
	names := []string{"one", "two", "three", "four", "five", "not_exist_in_db"}

	for i := 0; i < 200000; i++ {
		err = db.Set([]byte(fmt.Sprintf("R.%.7d.name.%s", i, names[0])), dummyData, pebble.NoSync)
		require.NoError(t, err)
	}

	for i := 200000; i < 400000; i++ {
		err = db.Set([]byte(fmt.Sprintf("R.%.7d.name.%s", i, names[1])), dummyData, pebble.NoSync)
		require.NoError(t, err)
	}

	for i := 400000; i < 600000; i++ {
		err = db.Set([]byte(fmt.Sprintf("R.%.7d.name.%s", i, names[2])), dummyData, pebble.NoSync)
		require.NoError(t, err)
	}

	for i := 600000; i < 800000; i++ {
		err = db.Set([]byte(fmt.Sprintf("R.%.7d.name.%s", i, names[3])), dummyData, pebble.NoSync)
		require.NoError(t, err)
	}

	for i := 800000; i < 1000000; i++ {
		err = db.Set([]byte(fmt.Sprintf("R.%.7d.name.%s", i, names[4])), dummyData, pebble.NoSync)
		require.NoError(t, err)
	}

	t1 := time.Now()
	it, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("R.0"),
		UpperBound: []byte("R.9"),
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
	})
	require.NoError(t, err)

	count := 0
	for it.First(); it.Valid(); it.Next() {
		hasPoint, hasRange := it.HasPointAndRange()
		if hasPoint {
			//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
		}
		if hasRange {
			s, e := it.RangeBounds()
			kd := it.RangeKeys()
			t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
		}

		count++
	}
	fmt.Printf("iterate no filter, took %s items %d\n", time.Since(t1).String(), count)
	it.Close()

	for _, name := range names {
		t1 := time.Now()

		it, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte("R.0"),
			UpperBound: []byte("R.9"),
			KeyTypes:   pebble.IterKeyTypePointsAndRanges,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&BlockIndexFilter{Value: []byte(name)},
			},
		})
		require.NoError(t, err)

		count := 0
		for it.First(); it.Valid(); it.Next() {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasPoint {
				//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
			}
			if hasRange {
				s, e := it.RangeBounds()
				kd := it.RangeKeys()
				t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
			}

			count++
		}
		fmt.Printf("iterate filter name: %s, took %s items %d\n", name, time.Since(t1).String(), count)
		it.Close()
	}

	fmt.Println("compact db:")
	_ = db.Compact([]byte("R.0"), []byte("R.9"), true)

	t1 = time.Now()
	it, err = db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("R.0"),
		UpperBound: []byte("R.9"),
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
	})
	require.NoError(t, err)

	count = 0
	for it.First(); it.Valid(); it.Next() {
		hasPoint, hasRange := it.HasPointAndRange()
		if hasPoint {
			//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
		}
		if hasRange {
			s, e := it.RangeBounds()
			kd := it.RangeKeys()
			t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
		}

		count++
	}
	fmt.Printf("iterate no filter, took %s items %d\n", time.Since(t1).String(), count)
	it.Close()

	for _, name := range names {
		t1 := time.Now()

		it, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte("R.0"),
			UpperBound: []byte("R.9"),
			KeyTypes:   pebble.IterKeyTypePointsAndRanges,
			PointKeyFilters: []pebble.BlockPropertyFilter{
				&BlockIndexFilter{Value: []byte(name)},
			},
		})
		require.NoError(t, err)

		count := 0
		for it.First(); it.Valid(); it.Next() {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasPoint {
				//t.Logf("point %s: %s", string(it.Key()), string(it.Value()))
			}
			if hasRange {
				s, e := it.RangeBounds()
				kd := it.RangeKeys()
				t.Logf("range(%s,%s): %v", string(s), string(e), string(kd[0].Value))
			}

			count++
		}
		fmt.Printf("iterate filter name: %s, took %s items %d\n", name, time.Since(t1).String(), count)
		it.Close()
	}

	db.Close()

	os.RemoveAll(".db")
}
