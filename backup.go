package bond

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sync/errgroup"
)

// write all the key/value of iterator to the SST file.
func IteratorToSST(itr Iterator, path string) error {
	defer itr.Close()
	// sst reader
	currentFileID := 1
	file, err := vfs.Default.Create(filepath.Join(path, fmt.Sprintf("%d.sst", currentFileID)))
	if err != nil {
		return err
	}
	opts := sstable.WriterOptions{
		TableFormat: sstable.TableFormatRocksDBv2, Parallelism: true, Comparer: DefaultKeyComparer(),
	}
	writer := sstable.NewWriter(objstorageprovider.NewFileWritable(file), opts)

	for itr.First(); itr.Valid(); itr.Next() {
		if err := writer.Set(itr.Key(), itr.Value()); err != nil {
			return err
		}

		// Replace the old writer with new writer after the old writer reaches it's capacity.
		if writer.EstimatedSize() > exportFileSize {
			if err := writer.Close(); err != nil {
				return err
			}
			currentFileID++
			file, err = vfs.Default.Create(filepath.Join(path, fmt.Sprintf("%d.sst", currentFileID)))
			if err != nil {
				return err
			}
			writer = sstable.NewWriter(objstorageprovider.NewFileWritable(file), opts)
		}
	}
	return writer.Close()
}

func listSST(dir string) []string {
	sst := []string{}
	filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if filepath.Ext(path) == ".sst" {
			sst = append(sst, path)
		}
		return nil
	})
	return sst
}

func (t *_table[T]) exportDir() string {
	return fmt.Sprintf("%s_%d", t.Name(), t.ID())
}

func (t *_table[T]) indexExportDir(idx IndexInfo) string {
	return fmt.Sprintf("%s_%d_%s_%d", t.Name(), t.ID(), idx.Name(), idx.ID())
}

func (t *_table[T]) backup(ctx context.Context, path string, index bool) error {
	grp := new(errgroup.Group)
	tableDir := filepath.Join(path, t.exportDir())
	if err := os.Mkdir(tableDir, 0755); err != nil {
		return err
	}
	itr := t.db.Iter(&IterOptions{
		IterOptions: pebble.IterOptions{
			LowerBound: t.dataKeySpaceStart,
			UpperBound: t.dataKeySpaceEnd,
		},
	})
	grp.Go(func() error {
		return IteratorToSST(itr, tableDir)
	})
	if !index {
		return grp.Wait()
	}
	// export all the indexes if it is explicitly requested.
	indexes := t.Indexes()
	for _, index := range indexes {
		indexDir := filepath.Join(path, t.indexExportDir(index))
		lowebound := []byte{byte(t.id), byte(index.ID()), 0, 0, 0, 0}
		upperbound := []byte{byte(t.id), byte(index.ID()), 255, 255, 255, 255}
		itr := t.db.Iter(&IterOptions{
			IterOptions: pebble.IterOptions{
				LowerBound: lowebound,
				UpperBound: upperbound,
			},
		})
		if err := os.MkdirAll(indexDir, 0755); err != nil {
			return err
		}
		grp.Go(func() error {
			return IteratorToSST(itr, indexDir)
		})
	}
	return grp.Wait()
}

func (t *_table[T]) restore(ctx context.Context, path string, index bool, strategy restoreStrategy) error {
	tablePath := filepath.Join(path, t.exportDir())
	tableSST := listSST(tablePath)

	switch strategy {
	case ingestSST:
		if err := t.db.Backend().Ingest(tableSST); err != nil {
			return err
		}
		if !index {
			return t.reindex(t.SecondaryIndexes())
		}

		indexes := t.Indexes()
		for _, index := range indexes {
			indexPath := filepath.Join(path, t.indexExportDir(index))
			indexSST := listSST(indexPath)
			if err := t.db.Backend().Ingest(indexSST); err != nil {
				return err
			}
		}
		return nil
	case batchedInsert:
		grp := new(errgroup.Group)
		for _, sst := range tableSST {
			grp.Go(func(sst string) func() error {
				return func() error {
					return t.insertSST(ctx, sst)
				}
			}(sst))
		}
		return grp.Wait()
	}
	return fmt.Errorf("invalid restore strategy")
}

func (t *_table[T]) insertSST(ctx context.Context, path string) error {
	file, err := vfs.Default.Open(path)
	if err != nil {
		return err
	}
	readable, err := sstable.NewSimpleReadable(file)
	if err != nil {
		return err
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{
		Comparer: DefaultKeyComparer(),
	})
	if err != nil {
		return err
	}
	itr, err := reader.NewIter(nil, nil)
	if err != nil {
		return err
	}

	var entries []T
	value := t.db.getValueBufferPool().Get()[:0]
	defer t.db.getValueBufferPool().Put(value[:0])
	batch := t.db.Batch()

	flushEntries := func() error {
		if err := t.Insert(ctx, entries, batch); err != nil {
			return err
		}
		if err := batch.Commit(Sync); err != nil {
			return err
		}
		return nil
	}

	for key, val := itr.First(); key != nil; key, val = itr.Next() {
		var entry T
		buf, _, err := val.Value(value[:0])
		if err != nil {
			return err
		}
		if err := t.serializer.Deserialize(buf, &entry); err != nil {
			return err
		}
		entries = append(entries, entry)
		if len(entries) > 200 {
			if err := flushEntries(); err != nil {
				return err
			}
			batch.ResetRetained()
			entries = entries[:0]
		}
	}

	if len(entries) > 0 {
		if err := flushEntries(); err != nil {
			return err
		}
	}
	return nil
}
