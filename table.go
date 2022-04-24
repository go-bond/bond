package bond

// Maybe 'Table' .. maybe 'Store' .. as interface type

// structure is:
// key = [tableID (1 byte)][entry type (1 byte)][...]
// for entry type, we could have > 1 means its one of the indexs.. it works..
//
// for table, [...]:
// key = [column (1 byte)][record id]
// value = binaryEncode([record])
//
// for index, [...]:
// key = [column id (1 byte)][column value]/[record id]
// value = nil

type TableID uint8

type EntryType uint8

const (
	TableEntryType EntryType = iota
	IndexEntryType
)

type Index struct { // interesting, this structure, might work for all of our tables and indexes..? maybe. doesn't have to though
	Table     TableID
	EntryType EntryType
	Column    []byte // TODO: can change later to just byte or uint8 (same thing)
	Value     []byte
	ID        []byte

	// TODO: what we're missing is the key!
	// we know the table, column, cool.. we need the ID

	ComputedKey []byte
}

func (i *Index) Key() []byte {
	if len(i.ComputedKey) > 0 {
		return i.ComputedKey
	}
	b := []byte{byte(i.Table), byte(i.EntryType)}
	b = append(b, i.Column...)
	// b = append(b, '/')
	b = append(b, i.Value...)
	b = append(b, '/')
	b = append(b, i.ID...)
	i.ComputedKey = b
	return b
}
