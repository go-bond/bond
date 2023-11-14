# Pebble Collectors And Filters

## Introduction

This document describes the pebble collectors and filters. The pebble collectors can be used to collect
some information about the contents of the block / index / sstable files that later can be used to filter
out the files that do not meet the criteria of the filtering.

## Pebble Collectors

The collector interface looks like this:

```go
// Block properties are an optional user-facing feature that can be used to
// filter data blocks (and whole sstables) from an Iterator before they are
// loaded. They do not apply to range delete blocks. These are expected to
// very concisely represent a set of some attribute value contained within the
// key or value, such that the set includes all the attribute values in the
// block. This has some similarities with OLAP pruning approaches that
// maintain min-max attribute values for some column (which concisely
// represent a set), that is then used to prune at query time. In Pebble's
// case, data blocks are small, typically 25-50KB, so these properties should
// reduce their precision in order to be concise -- a good rule of thumb is to
// not consume more than 50-100 bytes across all properties maintained for a
// block, i.e., a 500x reduction compared to loading the data block.
//
// A block property must be assigned a unique name, which is encoded and
// stored in the sstable. This name must be unique among all user-properties
// encoded in an sstable.
//
// A property is represented as a []byte. A nil value or empty byte slice are
// considered semantically identical. The caller is free to choose the
// semantics of an empty byte slice e.g. they could use it to represent the
// empty set or the universal set, whichever they think is more common and
// therefore better to encode more concisely. The serialization of the
// property for the various Finish*() calls in a BlockPropertyCollector
// implementation should be identical, since the corresponding
// BlockPropertyFilter implementation is not told the context in which it is
// deserializing the property.
//
// Block properties are more general than table properties and should be
// preferred over using table properties. A BlockPropertyCollector can achieve
// identical behavior to table properties by returning the nil slice from
// FinishDataBlock and FinishIndexBlock, and interpret them as the universal
// set in BlockPropertyFilter, and return a non-universal set in FinishTable.
//
// Block property filtering is nondeterministic because the separation of keys
// into blocks is nondeterministic. Clients use block-property filters to
// implement efficient application of a filter F that applies to key-value pairs
// (abbreviated as kv-filter). Consider correctness defined as surfacing exactly
// the same key-value pairs that would be surfaced if one applied the filter F
// above normal iteration. With this correctness definition, block property
// filtering may introduce two kinds of errors:
//
//   a) Block property filtering that uses a kv-filter may produce additional
//      key-value pairs that don't satisfy the filter because of the separation
//      of keys into blocks. Clients may remove these extra key-value pairs by
//      re-applying the kv filter while reading results back from Pebble.
//
//   b) Block property filtering may surface deleted key-value pairs if the
//      kv filter is not a strict function of the key's user key. A block
//      containing k.DEL may be filtered, while a block containing the deleted
//      key k.SET may not be filtered, if the kv filter applies to one but not
//      the other.
//
//      This error may be avoided trivially by using a kv filter that is a pure
//      function of the user key. A filter that examines values or key kinds
//      requires care to ensure F(k.SET, <value>) = F(k.DEL) = F(k.SINGLEDEL).
//
// The combination of range deletions and filtering by table-level properties
// add another opportunity for deleted point keys to be surfaced. The pebble
// Iterator stack takes care to correctly apply filtered tables' range deletions
// to lower tables, preventing this form of nondeterministic error.
//
// In addition to the non-determinism discussed in (b), which limits the use
// of properties over values, we now have support for values that are not
// stored together with the key, and may not even be retrieved during
// compactions. If Pebble is configured with such value separation, block
// properties must only apply to the key, and will be provided a nil value.

// BlockPropertyCollector is used when writing a sstable.
//
//   - All calls to Add are included in the next FinishDataBlock, after which
//     the next data block is expected to start.
//
//   - The index entry generated for the data block, which contains the return
//     value from FinishDataBlock, is not immediately included in the current
//     index block. It is included when AddPrevDataBlockToIndexBlock is called.
//     An alternative would be to return an opaque handle from FinishDataBlock
//     and pass it to a new AddToIndexBlock method, which requires more
//     plumbing, and passing of an interface{} results in a undesirable heap
//     allocation. AddPrevDataBlockToIndexBlock must be called before keys are
//     added to the new data block.
type BlockPropertyCollector interface {
	// Name returns the name of the block property collector.
	Name() string
	// Add is called with each new entry added to a data block in the sstable.
	// The callee can assume that these are in sorted order.
	Add(key InternalKey, value []byte) error
	// FinishDataBlock is called when all the entries have been added to a
	// data block. Subsequent Add calls will be for the next data block. It
	// returns the property value for the finished block.
	FinishDataBlock(buf []byte) ([]byte, error)
	// AddPrevDataBlockToIndexBlock adds the entry corresponding to the
	// previous FinishDataBlock to the current index block.
	AddPrevDataBlockToIndexBlock()
	// FinishIndexBlock is called when an index block, containing all the
	// key-value pairs since the last FinishIndexBlock, will no longer see new
	// entries. It returns the property value for the index block.
	FinishIndexBlock(buf []byte) ([]byte, error)
	// FinishTable is called when the sstable is finished, and returns the
	// property value for the sstable.
	FinishTable(buf []byte) ([]byte, error)
}
```

The collector is called for each entry in the block, and then for each block in the sstable. The *Add* method 
is called for each entry in the block, and the *FinishDataBlock* method is called when the block is finished.
The method is called for three types of the keys - *Delete*, *Set* and *Merge*. The *Delete* key is used to
mark the key as deleted, the *Set* key is used to set the value for the key, and the *Merge* key is used to
merge the value with the existing value for the key. The *Delete* key does not have the value. This means that
for collectors that are interested in values some of the keys may not be surfaced when filtering the sstable.

## Pebble Filters

The filter interface looks like this:

```go
// BlockPropertyFilter is used in an Iterator to filter sstables and blocks
// within the sstable. It should not maintain any per-sstable state, and must
// be thread-safe.
type BlockPropertyFilter interface {
	// Name returns the name of the block property collector.
	Name() string
	// Intersects returns true if the set represented by prop intersects with
	// the set in the filter.
	Intersects(prop []byte) (bool, error)
}
```

The filter is called for each block / index / sstable. The *Intersects* method checks the collector property and 
returns *true* if the block / index / sstable should be included in the result.

## Sample Implementation

The sample implementation of the collector and the filter is in the *sample* folder. There are two tests that check
use of the filter where row property is evenly distributed and where row property is sequentially distributed.

Output: evenly distributed

```bash
equal distribution:
iterate no filter, took 141.736093ms items 1000000
iterate filter name: one, took 138.184733ms items 1000000
iterate filter name: two, took 197.630284ms items 1000000
iterate filter name: three, took 189.57198ms items 1000000
iterate filter name: four, took 182.271383ms items 1000000
iterate filter name: five, took 181.391098ms items 1000000
iterate filter name: not_exist_in_db, took 19.767209ms items 251926
compact db:
iterate no filter, took 175.061389ms items 1000000
iterate filter name: one, took 217.751658ms items 1000000
iterate filter name: two, took 215.211524ms items 1000000
iterate filter name: three, took 244.427502ms items 1000000
iterate filter name: four, took 214.183289ms items 1000000
iterate filter name: five, took 208.370026ms items 1000000
iterate filter name: not_exist_in_db, took 45.197µs items 0
```

Output: sequentially distributed

```bash
sequential distribution:
iterate no filter, took 123.359613ms items 1000000
iterate filter name: one, took 140.459008ms items 825542
iterate filter name: two, took 98.458456ms items 800020
iterate filter name: three, took 66.975697ms items 451821
iterate filter name: four, took 56.781109ms items 400023
iterate filter name: five, took 19.914769ms items 251735
iterate filter name: not_exist_in_db, took 21.566947ms items 251735
compact db:
iterate no filter, took 168.639998ms items 1000000
iterate filter name: one, took 43.356478ms items 200081
iterate filter name: two, took 52.399397ms items 200111
iterate filter name: three, took 52.385891ms items 200090
iterate filter name: four, took 42.432561ms items 200084
iterate filter name: five, took 50.254286ms items 200035
iterate filter name: not_exist_in_db, took 115.537µs items 0
```

## Value based block filters

Bond DB will create additional keys while indexing the user data and occupy extra disk space. 

The below data will show the size distribution of index key of the sequence's indexer database. It shows that index key occupies upto 50% of the indexer database. 

```
======> Size of Database: 5.9 GB
size of index in db:  2.4 GB
size of index key part in db:  542 MB
size of primary key part in db:  1.9 GB
```

The block filter based on value will remove the extra index key and give us additional query performance. But the pebble storage surface will surface deleted key on value based block filters, which leads us to incorrect data. 



## Conclusions

1. The filtering is the more beneficial the bigger database is.
2. The more evenly distributed property through out the database the less beneficial the filtering is. 
This is mainly due to a fact that even distribution means that each block will have at least one of each
property therefore no blocks are filtered out.
3. The more sequentially distributed property through out the database the more beneficial the filtering is.
This is mainly due to a fact that sequential distribution means that each block will have only one property
therefore all other blocks are filtered out.
4. The filtering is more beneficial on compacted database. This is mainly due to the fact that first layer of the 
sstable in pebble can contain duplicated ranges within it's sstables. So there is greater chance that each of them
will contain at least one of each property. Once database is compacted there is no duplicated ranges within the 
sstable therefore filtering is more beneficial.


