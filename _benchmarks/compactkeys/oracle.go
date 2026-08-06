package compactkeys

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

type oracleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
}

type Oracle struct {
	Entries  []Entry
	MissKeys [][]byte
}

func NewOracle(entries []Entry, missKeys ...[][]byte) Oracle {
	cloned := make([]Entry, len(entries))
	for i := range entries {
		cloned[i] = cloneEntry(entries[i])
	}
	var clonedMisses [][]byte
	if len(missKeys) > 0 {
		clonedMisses = make([][]byte, len(missKeys[0]))
		for i := range missKeys[0] {
			clonedMisses[i] = bytes.Clone(missKeys[0][i])
		}
	}
	return Oracle{Entries: cloned, MissKeys: clonedMisses}
}

func (o Oracle) Verify(reader oracleReader) error {
	if err := o.verifyForward(reader); err != nil {
		return err
	}
	if err := o.verifyReverse(reader); err != nil {
		return err
	}
	if err := o.verifyBounded(reader); err != nil {
		return err
	}
	if err := o.verifyPoints(reader); err != nil {
		return err
	}
	return o.verifyPointMisses(reader)
}

func (o Oracle) verifyForward(reader oracleReader) error {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return fmt.Errorf("create forward iterator: %w", err)
	}
	defer iter.Close()

	position := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		if position >= len(o.Entries) {
			return fmt.Errorf("forward scan has unexpected key %x at position %d", iter.Key(), position)
		}
		if err := compareEntry(o.Entries[position], iter.Key(), iter.Value(), position); err != nil {
			return fmt.Errorf("forward scan: %w", err)
		}
		position++
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("forward scan: %w", err)
	}
	if position != len(o.Entries) {
		return fmt.Errorf("forward scan returned %d entries, expected %d", position, len(o.Entries))
	}
	return nil
}

func (o Oracle) verifyReverse(reader oracleReader) error {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return fmt.Errorf("create reverse iterator: %w", err)
	}
	defer iter.Close()

	position := len(o.Entries) - 1
	for valid := iter.Last(); valid; valid = iter.Prev() {
		if position < 0 {
			return fmt.Errorf("reverse scan has unexpected key %x", iter.Key())
		}
		if err := compareEntry(o.Entries[position], iter.Key(), iter.Value(), position); err != nil {
			return fmt.Errorf("reverse scan: %w", err)
		}
		position--
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("reverse scan: %w", err)
	}
	if position != -1 {
		return fmt.Errorf("reverse scan stopped with %d entries missing", position+1)
	}
	return nil
}

func (o Oracle) verifyBounded(reader oracleReader) error {
	if len(o.Entries) < 4 {
		return nil
	}
	start := len(o.Entries) / 4
	end := len(o.Entries) * 3 / 4
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: o.Entries[start].Key,
		UpperBound: o.Entries[end].Key,
	})
	if err != nil {
		return fmt.Errorf("create bounded iterator: %w", err)
	}
	defer iter.Close()

	position := start
	for valid := iter.First(); valid; valid = iter.Next() {
		if position >= end {
			return fmt.Errorf("bounded scan escaped upper bound at %x", iter.Key())
		}
		if err := compareEntry(o.Entries[position], iter.Key(), iter.Value(), position); err != nil {
			return fmt.Errorf("bounded scan: %w", err)
		}
		position++
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("bounded scan: %w", err)
	}
	if position != end {
		return fmt.Errorf("bounded scan returned through position %d, expected %d", position, end)
	}
	return nil
}

func (o Oracle) verifyPoints(reader oracleReader) error {
	if len(o.Entries) == 0 {
		return nil
	}
	stride := max(1, len(o.Entries)/97)
	for i := 0; i < len(o.Entries); i += stride {
		value, closer, err := reader.Get(o.Entries[i].Key)
		if err != nil {
			return fmt.Errorf("point lookup %d: %w", i, err)
		}
		if !bytes.Equal(value, o.Entries[i].Value) {
			closer.Close()
			return fmt.Errorf("point lookup %d value mismatch", i)
		}
		if err := closer.Close(); err != nil {
			return fmt.Errorf("point lookup %d close: %w", i, err)
		}
	}
	return nil
}

func (o Oracle) verifyPointMisses(reader oracleReader) error {
	for i, key := range o.MissKeys {
		value, closer, err := reader.Get(key)
		if err == nil {
			if closer != nil {
				_ = closer.Close()
			}
			return fmt.Errorf("point miss %d unexpectedly returned value %x", i, value)
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("point miss %d: %w", i, err)
		}
	}
	return nil
}

func compareEntry(expected Entry, key, value []byte, position int) error {
	if !bytes.Equal(expected.Key, key) {
		return fmt.Errorf("key mismatch at position %d: got %x, expected %x", position, key, expected.Key)
	}
	if !bytes.Equal(expected.Value, value) {
		return fmt.Errorf("value mismatch at position %d for key %x", position, key)
	}
	return nil
}
