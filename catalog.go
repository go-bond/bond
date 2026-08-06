package bond

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const catalogManifestVersion = 1

var (
	// ErrCatalogFrozen is returned when a definition is added after successful
	// validation. A validated catalog is immutable because its fingerprint may
	// already be durable.
	ErrCatalogFrozen = errors.New("bond: catalog is frozen")

	// ErrCatalogRequired is returned when a database with durable catalog
	// metadata is opened without supplying its catalog.
	ErrCatalogRequired = errors.New("bond: database requires a catalog")
)

// Descriptor gives runtime behavior an explicit durable identity. Name and
// Version, rather than an opaque Go callback, participate in catalog storage
// compatibility.
type Descriptor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type CodecDescriptor = Descriptor
type LayoutDescriptor = Descriptor
type OrderDescriptor = Descriptor
type PredicateDescriptor = Descriptor

// BondTableLayoutV1 and BondIndexLayoutV1 identify the unchanged logical key
// layouts implemented by the current Table and Index code. New logical layouts
// require new implementations and durable versions rather than arbitrary names.
func BondTableLayoutV1() LayoutDescriptor {
	return LayoutDescriptor{Name: "bond/table", Version: "v1"}
}

func BondIndexLayoutV1() LayoutDescriptor {
	return LayoutDescriptor{Name: "bond/index", Version: "v1"}
}

// LogicalKeyKind describes the unambiguous shape produced by a logical key
// callback. It does not serialize or inspect the callback itself.
type LogicalKeyKind string

const (
	LogicalKeyNone   LogicalKeyKind = "none"
	LogicalKeyUint64 LogicalKeyKind = "uint64"
	LogicalKeyUint32 LogicalKeyKind = "uint32"
	LogicalKeyBytes  LogicalKeyKind = "bytes"
	LogicalKeyTuple  LogicalKeyKind = "tuple"
	LogicalKeyOpaque LogicalKeyKind = "opaque"
)

// KeyDescriptor pairs a developer-owned durable identity with the logical
// shape that a runtime extractor promises to produce.
type KeyDescriptor struct {
	Descriptor
	Kind LogicalKeyKind `json:"kind"`
}

// SchemaFamilyDescriptor is a future physical storage assignment. Phase 5
// validates and fingerprints this descriptor but deliberately does not install
// it as a Pebble reader or writer.
type SchemaFamilyDescriptor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func LegacySchemaFamily() SchemaFamilyDescriptor {
	return SchemaFamilyDescriptor{Name: "pebble/default", Version: "v1"}
}

func PKUint64SchemaFamily() SchemaFamilyDescriptor {
	return SchemaFamilyDescriptor{Name: "bond/pk-u64", Version: "v1"}
}

func PKUint32SchemaFamily() SchemaFamilyDescriptor {
	return SchemaFamilyDescriptor{Name: "bond/pk-u32", Version: "v1"}
}

func PKBytesSchemaFamily() SchemaFamilyDescriptor {
	return SchemaFamilyDescriptor{Name: "bond/pk-bytes", Version: "v1"}
}

func PKOpaqueSchemaFamily() SchemaFamilyDescriptor {
	return SchemaFamilyDescriptor{Name: "bond/pk-opaque", Version: "v1"}
}

func (d SchemaFamilyDescriptor) String() string {
	if d.Name == "" {
		return d.Version
	}
	if d.Version == "" {
		return d.Name
	}
	return d.Name + "/" + d.Version
}

// TableSchema separates durable storage descriptors from runtime serializer
// and primary-key behavior. Serializer and PrimaryKeyFunc are required for a
// catalog-bound table but are intentionally excluded from the fingerprint.
type TableSchema[T any] struct {
	Name           string
	TableID        TableID
	LogicalLayout  LayoutDescriptor
	Codec          CodecDescriptor
	Serializer     Serializer[any]
	PrimaryKey     KeyDescriptor
	PrimaryKeyFunc TablePrimaryKeyFunc[T]
	PhysicalSchema SchemaFamilyDescriptor

	Filter           FilterWithStats
	ScanPrefetchSize int
}

// IndexSchema describes the durable and runtime portions of one secondary
// index. Unique records the application's durable uniqueness contract; Phase 5
// retains Bond's existing logical index encoding and does not introduce the
// separate unique-index-in-value layout discussed in the project plan.
type IndexSchema[T any] struct {
	Name          string
	IndexID       IndexID
	LogicalLayout LayoutDescriptor
	Unique        bool
	MultiKey      bool
	Partial       bool
	Key           KeyDescriptor
	Order         KeyDescriptor
	Predicate     PredicateDescriptor

	IndexKeyFunc      IndexKeyFunction[T]
	IndexMultiKeyFunc IndexMultiKeyFunction[T]
	IndexOrderFunc    IndexOrderFunction[T]
	IndexFilterFunc   IndexFilterFunction[T]
}

// CatalogManifest is the complete serializable storage definition. It contains
// no callbacks, process addresses, or reflected function names.
type CatalogManifest struct {
	ManifestVersion int                    `json:"manifest_version"`
	Name            string                 `json:"name"`
	Version         string                 `json:"version"`
	Tables          []CatalogTableManifest `json:"tables"`
}

type CatalogTableManifest struct {
	ID             TableID                `json:"id"`
	Name           string                 `json:"name"`
	LogicalLayout  LayoutDescriptor       `json:"logical_layout"`
	Codec          CodecDescriptor        `json:"codec"`
	PrimaryKey     KeyDescriptor          `json:"primary_key"`
	PhysicalSchema SchemaFamilyDescriptor `json:"physical_schema"`
	Indexes        []CatalogIndexManifest `json:"indexes"`
}

type CatalogIndexManifest struct {
	ID            IndexID             `json:"id"`
	Name          string              `json:"name"`
	LogicalLayout LayoutDescriptor    `json:"logical_layout"`
	Unique        bool                `json:"unique"`
	MultiKey      bool                `json:"multi_key"`
	Partial       bool                `json:"partial"`
	Key           KeyDescriptor       `json:"key"`
	Order         KeyDescriptor       `json:"order"`
	Predicate     PredicateDescriptor `json:"predicate"`
}

// CatalogChange is one deterministic, actionable storage-definition change.
type CatalogChange struct {
	Path      string `json:"path"`
	Stored    string `json:"stored"`
	Requested string `json:"requested"`
}

// CatalogCompatibilityError reports an open-time fingerprint mismatch with a
// field-level diff of the stored and requested manifests.
type CatalogCompatibilityError struct {
	StoredFingerprint    string          `json:"stored_fingerprint"`
	RequestedFingerprint string          `json:"requested_fingerprint"`
	Changes              []CatalogChange `json:"changes"`
}

func (e *CatalogCompatibilityError) Error() string {
	if len(e.Changes) == 0 {
		return fmt.Sprintf(
			"bond: catalog fingerprint drift: stored %s, requested %s",
			e.StoredFingerprint,
			e.RequestedFingerprint,
		)
	}
	parts := make([]string, len(e.Changes))
	for i, change := range e.Changes {
		parts[i] = fmt.Sprintf("%s: %q -> %q", change.Path, change.Stored, change.Requested)
	}
	return fmt.Sprintf(
		"bond: catalog fingerprint drift: stored %s, requested %s (%s)",
		e.StoredFingerprint,
		e.RequestedFingerprint,
		strings.Join(parts, "; "),
	)
}

type catalogTableDefinition interface {
	id() TableID
	name() string
	validate() error
	manifest() CatalogTableManifest
}

// Catalog is mutable only while definitions are being registered. Successful
// validation freezes it and caches its canonical manifest and fingerprint.
type Catalog struct {
	name    string
	version string

	mu          sync.RWMutex
	tables      []catalogTableDefinition
	frozen      bool
	manifestV   CatalogManifest
	fingerprint string
}

func NewCatalog(name, version string) *Catalog {
	return &Catalog{name: name, version: version}
}

func (c *Catalog) Name() string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.name
}

func (c *Catalog) Version() string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

func (c *Catalog) Validate() error {
	if c == nil {
		return errors.New("bond: catalog is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return nil
	}
	if err := validateNameVersion("catalog", Descriptor{Name: c.name, Version: c.version}); err != nil {
		return err
	}

	for _, table := range c.tables {
		if err := table.validate(); err != nil {
			return err
		}
	}
	manifest := CatalogManifest{
		ManifestVersion: catalogManifestVersion,
		Name:            c.name,
		Version:         c.version,
		Tables:          make([]CatalogTableManifest, 0, len(c.tables)),
	}
	for _, table := range c.tables {
		manifest.Tables = append(manifest.Tables, table.manifest())
	}
	sort.Slice(manifest.Tables, func(i, j int) bool {
		return manifest.Tables[i].ID < manifest.Tables[j].ID
	})
	fingerprint, err := fingerprintCatalogManifest(manifest)
	if err != nil {
		return err
	}
	c.manifestV = manifest
	c.fingerprint = fingerprint
	c.frozen = true
	return nil
}

func (c *Catalog) Fingerprint() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fingerprint, nil
}

func (c *Catalog) Manifest() (CatalogManifest, error) {
	if err := c.Validate(); err != nil {
		return CatalogManifest{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneCatalogManifest(c.manifestV), nil
}

func (c *Catalog) addTable(table catalogTableDefinition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return ErrCatalogFrozen
	}
	if table.id() == BOND_DB_DATA_TABLE_ID {
		return fmt.Errorf("bond: catalog table ID %d is reserved for Bond metadata", table.id())
	}
	if strings.TrimSpace(table.name()) == "" {
		return errors.New("bond: catalog table name is required")
	}
	for _, existing := range c.tables {
		if existing.id() == table.id() {
			return fmt.Errorf("bond: duplicate catalog table ID %d", table.id())
		}
		if existing.name() == table.name() {
			return fmt.Errorf("bond: duplicate catalog table name %q", table.name())
		}
	}
	c.tables = append(c.tables, table)
	return nil
}

func (c *Catalog) ownsTableID(id TableID) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, table := range c.tables {
		if table.id() == id {
			return true
		}
	}
	return false
}

// TableDefinition is an immutable, unbound generic table definition owned by
// exactly one Catalog.
type TableDefinition[T any] struct {
	catalog *Catalog
	schema  TableSchema[T]
	indexes []*IndexDefinition[T]
}

func DefineTable[T any](catalog *Catalog, schema TableSchema[T]) (*TableDefinition[T], error) {
	if catalog == nil {
		return nil, errors.New("bond: define table with nil catalog")
	}
	definition := &TableDefinition[T]{catalog: catalog, schema: schema}
	if err := catalog.addTable(definition); err != nil {
		return nil, err
	}
	return definition, nil
}

func (d *TableDefinition[T]) ID() TableID       { return d.schema.TableID }
func (d *TableDefinition[T]) Name() string      { return d.schema.Name }
func (d *TableDefinition[T]) Catalog() *Catalog { return d.catalog }

func (d *TableDefinition[T]) id() TableID  { return d.schema.TableID }
func (d *TableDefinition[T]) name() string { return d.schema.Name }
func (d *TableDefinition[T]) manifest() CatalogTableManifest {
	indexes := make([]CatalogIndexManifest, 0, len(d.indexes))
	for _, index := range d.indexes {
		indexes = append(indexes, index.manifest())
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].ID < indexes[j].ID })
	return CatalogTableManifest{
		ID:             d.schema.TableID,
		Name:           d.schema.Name,
		LogicalLayout:  d.schema.LogicalLayout,
		Codec:          d.schema.Codec,
		PrimaryKey:     d.schema.PrimaryKey,
		PhysicalSchema: d.schema.PhysicalSchema,
		Indexes:        indexes,
	}
}

func (d *TableDefinition[T]) validate() error {
	prefix := fmt.Sprintf("catalog table %q (ID %d)", d.schema.Name, d.schema.TableID)
	if d.schema.TableID == BOND_DB_DATA_TABLE_ID {
		return fmt.Errorf("bond: %s uses reserved table ID", prefix)
	}
	if strings.TrimSpace(d.schema.Name) == "" {
		return fmt.Errorf("bond: %s has an empty name", prefix)
	}
	if err := validateNameVersion(prefix+" logical layout", d.schema.LogicalLayout); err != nil {
		return err
	}
	if d.schema.LogicalLayout != BondTableLayoutV1() {
		return fmt.Errorf("bond: %s uses unsupported logical layout %s/%s", prefix, d.schema.LogicalLayout.Name, d.schema.LogicalLayout.Version)
	}
	if err := validateNameVersion(prefix+" codec", d.schema.Codec); err != nil {
		return err
	}
	if d.schema.Serializer == nil {
		return fmt.Errorf("bond: %s serializer is required and must match codec descriptor %s/%s", prefix, d.schema.Codec.Name, d.schema.Codec.Version)
	}
	if err := validateKeyDescriptor(prefix+" primary key", d.schema.PrimaryKey, false); err != nil {
		return err
	}
	if d.schema.PrimaryKeyFunc == nil {
		return fmt.Errorf("bond: %s primary-key callback is required", prefix)
	}
	if err := validatePhysicalFamily(prefix, d.schema.PhysicalSchema, d.schema.PrimaryKey.Kind); err != nil {
		return err
	}
	for _, index := range d.indexes {
		if err := index.validate(); err != nil {
			return err
		}
	}
	return nil
}

// IndexDefinition is an immutable, unbound generic index definition owned by
// one TableDefinition.
type IndexDefinition[T any] struct {
	table  *TableDefinition[T]
	schema IndexSchema[T]
}

func DefineIndex[T any](table *TableDefinition[T], schema IndexSchema[T]) (*IndexDefinition[T], error) {
	if table == nil || table.catalog == nil {
		return nil, errors.New("bond: define index with nil table definition")
	}
	catalog := table.catalog
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if catalog.frozen {
		return nil, ErrCatalogFrozen
	}
	if schema.IndexID == PrimaryIndexID {
		return nil, fmt.Errorf("bond: catalog index ID %d is reserved for the primary index", schema.IndexID)
	}
	if schema.IndexID == BOND_DB_DATA_USER_SPACE_INDEX_ID {
		return nil, fmt.Errorf("bond: catalog index ID %d is reserved", schema.IndexID)
	}
	if strings.TrimSpace(schema.Name) == "" {
		return nil, errors.New("bond: catalog index name is required")
	}
	for _, existing := range table.indexes {
		if existing.schema.IndexID == schema.IndexID {
			return nil, fmt.Errorf("bond: duplicate catalog index ID %d in table %q", schema.IndexID, table.schema.Name)
		}
		if existing.schema.Name == schema.Name {
			return nil, fmt.Errorf("bond: duplicate catalog index name %q in table %q", schema.Name, table.schema.Name)
		}
	}
	definition := &IndexDefinition[T]{table: table, schema: schema}
	table.indexes = append(table.indexes, definition)
	return definition, nil
}

func (d *IndexDefinition[T]) ID() IndexID                { return d.schema.IndexID }
func (d *IndexDefinition[T]) Name() string               { return d.schema.Name }
func (d *IndexDefinition[T]) Table() *TableDefinition[T] { return d.table }

func (d *IndexDefinition[T]) manifest() CatalogIndexManifest {
	return CatalogIndexManifest{
		ID:            d.schema.IndexID,
		Name:          d.schema.Name,
		LogicalLayout: d.schema.LogicalLayout,
		Unique:        d.schema.Unique,
		MultiKey:      d.schema.MultiKey,
		Partial:       d.schema.Partial,
		Key:           d.schema.Key,
		Order:         d.schema.Order,
		Predicate:     d.schema.Predicate,
	}
}

func (d *IndexDefinition[T]) validate() error {
	prefix := fmt.Sprintf(
		"catalog table %q index %q (ID %d)",
		d.table.schema.Name,
		d.schema.Name,
		d.schema.IndexID,
	)
	if d.schema.IndexID == PrimaryIndexID || d.schema.IndexID == BOND_DB_DATA_USER_SPACE_INDEX_ID {
		return fmt.Errorf("bond: %s uses a reserved index ID", prefix)
	}
	if strings.TrimSpace(d.schema.Name) == "" {
		return fmt.Errorf("bond: %s has an empty name", prefix)
	}
	if err := validateNameVersion(prefix+" logical layout", d.schema.LogicalLayout); err != nil {
		return err
	}
	if d.schema.LogicalLayout != BondIndexLayoutV1() {
		return fmt.Errorf("bond: %s uses unsupported logical layout %s/%s", prefix, d.schema.LogicalLayout.Name, d.schema.LogicalLayout.Version)
	}
	if err := validateKeyDescriptor(prefix+" key", d.schema.Key, false); err != nil {
		return err
	}
	if d.schema.IndexKeyFunc == nil {
		return fmt.Errorf("bond: %s query key callback is required", prefix)
	}
	if err := validateKeyDescriptor(prefix+" order", d.schema.Order, true); err != nil {
		return err
	}
	if d.schema.Order.Kind != LogicalKeyNone && d.schema.IndexOrderFunc == nil {
		return fmt.Errorf("bond: %s order callback is required for %q order", prefix, d.schema.Order.Kind)
	}
	if d.schema.Order.Kind == LogicalKeyNone && d.schema.IndexOrderFunc != nil {
		return fmt.Errorf("bond: %s declares no order but supplies an order callback", prefix)
	}
	if err := validateNameVersion(prefix+" predicate", d.schema.Predicate); err != nil {
		return err
	}
	if d.schema.MultiKey != (d.schema.IndexMultiKeyFunc != nil) {
		return fmt.Errorf("bond: %s MultiKey=%t does not match presence of multi-key callback", prefix, d.schema.MultiKey)
	}
	if d.schema.Partial != (d.schema.IndexFilterFunc != nil) {
		return fmt.Errorf("bond: %s Partial=%t does not match presence of predicate callback", prefix, d.schema.Partial)
	}
	return nil
}

func validateNameVersion(subject string, descriptor Descriptor) error {
	if strings.TrimSpace(descriptor.Name) == "" {
		return fmt.Errorf("bond: %s descriptor name is required", subject)
	}
	if strings.TrimSpace(descriptor.Version) == "" {
		return fmt.Errorf("bond: %s descriptor version is required", subject)
	}
	return nil
}

func validateKeyDescriptor(subject string, descriptor KeyDescriptor, allowNone bool) error {
	if err := validateNameVersion(subject, descriptor.Descriptor); err != nil {
		return err
	}
	switch descriptor.Kind {
	case LogicalKeyUint64, LogicalKeyUint32, LogicalKeyBytes, LogicalKeyTuple, LogicalKeyOpaque:
		return nil
	case LogicalKeyNone:
		if allowNone {
			return nil
		}
	}
	return fmt.Errorf("bond: %s has unsupported logical key kind %q", subject, descriptor.Kind)
}

func validatePhysicalFamily(subject string, family SchemaFamilyDescriptor, keyKind LogicalKeyKind) error {
	if err := validateNameVersion(subject+" physical schema family", Descriptor(family)); err != nil {
		return err
	}
	switch family {
	case LegacySchemaFamily(), PKOpaqueSchemaFamily():
		return nil
	case PKUint64SchemaFamily():
		if keyKind != LogicalKeyUint64 {
			return fmt.Errorf("bond: %s physical schema family %q requires primary key kind %q, got %q", subject, family.String(), LogicalKeyUint64, keyKind)
		}
		return nil
	case PKUint32SchemaFamily():
		if keyKind != LogicalKeyUint32 {
			return fmt.Errorf("bond: %s physical schema family %q requires primary key kind %q, got %q", subject, family.String(), LogicalKeyUint32, keyKind)
		}
		return nil
	case PKBytesSchemaFamily():
		if keyKind != LogicalKeyBytes {
			return fmt.Errorf("bond: %s physical schema family %q requires primary key kind %q, got %q", subject, family.String(), LogicalKeyBytes, keyKind)
		}
		return nil
	default:
		return fmt.Errorf("bond: %s uses unknown physical schema family %q", subject, family.String())
	}
}

func fingerprintCatalogManifest(manifest CatalogManifest) (string, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("bond: encode catalog manifest: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneCatalogManifest(manifest CatalogManifest) CatalogManifest {
	clone := manifest
	clone.Tables = make([]CatalogTableManifest, len(manifest.Tables))
	for i, table := range manifest.Tables {
		clone.Tables[i] = table
		clone.Tables[i].Indexes = append([]CatalogIndexManifest(nil), table.Indexes...)
		if clone.Tables[i].Indexes == nil {
			clone.Tables[i].Indexes = []CatalogIndexManifest{}
		}
	}
	if clone.Tables == nil {
		clone.Tables = []CatalogTableManifest{}
	}
	return clone
}

// DiffCatalogManifests compares canonical storage definitions by stable IDs.
// Its result is sorted by field path and is safe to surface directly in open
// diagnostics or migration tooling.
func DiffCatalogManifests(stored, requested CatalogManifest) []CatalogChange {
	changes := make([]CatalogChange, 0)
	add := func(path string, oldValue, newValue any) {
		oldText := fmt.Sprint(oldValue)
		newText := fmt.Sprint(newValue)
		if oldText != newText {
			changes = append(changes, CatalogChange{Path: path, Stored: oldText, Requested: newText})
		}
	}
	add("manifest_version", stored.ManifestVersion, requested.ManifestVersion)
	add("catalog.name", stored.Name, requested.Name)
	add("catalog.version", stored.Version, requested.Version)

	storedTables := make(map[TableID]CatalogTableManifest, len(stored.Tables))
	requestedTables := make(map[TableID]CatalogTableManifest, len(requested.Tables))
	ids := make(map[TableID]struct{}, len(stored.Tables)+len(requested.Tables))
	for _, table := range stored.Tables {
		storedTables[table.ID] = table
		ids[table.ID] = struct{}{}
	}
	for _, table := range requested.Tables {
		requestedTables[table.ID] = table
		ids[table.ID] = struct{}{}
	}
	for _, id := range sortedTableIDs(ids) {
		oldTable, oldOK := storedTables[id]
		newTable, newOK := requestedTables[id]
		path := fmt.Sprintf("tables[id=%d]", id)
		if !oldOK {
			add(path, "<missing>", newTable.Name)
			continue
		}
		if !newOK {
			add(path, oldTable.Name, "<missing>")
			continue
		}
		add(path+".name", oldTable.Name, newTable.Name)
		diffDescriptor(add, path+".logical_layout", oldTable.LogicalLayout, newTable.LogicalLayout)
		diffDescriptor(add, path+".codec", oldTable.Codec, newTable.Codec)
		diffKeyDescriptor(add, path+".primary_key", oldTable.PrimaryKey, newTable.PrimaryKey)
		diffDescriptor(add, path+".physical_schema", Descriptor(oldTable.PhysicalSchema), Descriptor(newTable.PhysicalSchema))
		diffIndexManifests(add, path, oldTable.Indexes, newTable.Indexes)
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return changes
}

func diffIndexManifests(
	add func(string, any, any),
	tablePath string,
	stored, requested []CatalogIndexManifest,
) {
	storedIndexes := make(map[IndexID]CatalogIndexManifest, len(stored))
	requestedIndexes := make(map[IndexID]CatalogIndexManifest, len(requested))
	ids := make(map[IndexID]struct{}, len(stored)+len(requested))
	for _, index := range stored {
		storedIndexes[index.ID] = index
		ids[index.ID] = struct{}{}
	}
	for _, index := range requested {
		requestedIndexes[index.ID] = index
		ids[index.ID] = struct{}{}
	}
	orderedIDs := make([]int, 0, len(ids))
	for id := range ids {
		orderedIDs = append(orderedIDs, int(id))
	}
	sort.Ints(orderedIDs)
	for _, numericID := range orderedIDs {
		id := IndexID(numericID)
		oldIndex, oldOK := storedIndexes[id]
		newIndex, newOK := requestedIndexes[id]
		path := fmt.Sprintf("%s.indexes[id=%d]", tablePath, id)
		if !oldOK {
			add(path, "<missing>", newIndex.Name)
			continue
		}
		if !newOK {
			add(path, oldIndex.Name, "<missing>")
			continue
		}
		add(path+".name", oldIndex.Name, newIndex.Name)
		diffDescriptor(add, path+".logical_layout", oldIndex.LogicalLayout, newIndex.LogicalLayout)
		add(path+".unique", oldIndex.Unique, newIndex.Unique)
		add(path+".multi_key", oldIndex.MultiKey, newIndex.MultiKey)
		add(path+".partial", oldIndex.Partial, newIndex.Partial)
		diffKeyDescriptor(add, path+".key", oldIndex.Key, newIndex.Key)
		diffKeyDescriptor(add, path+".order", oldIndex.Order, newIndex.Order)
		diffDescriptor(add, path+".predicate", oldIndex.Predicate, newIndex.Predicate)
	}
}

func diffDescriptor(add func(string, any, any), path string, stored, requested Descriptor) {
	add(path+".name", stored.Name, requested.Name)
	add(path+".version", stored.Version, requested.Version)
}

func diffKeyDescriptor(add func(string, any, any), path string, stored, requested KeyDescriptor) {
	diffDescriptor(add, path, stored.Descriptor, requested.Descriptor)
	add(path+".kind", stored.Kind, requested.Kind)
}

func sortedTableIDs(set map[TableID]struct{}) []TableID {
	ids := make([]int, 0, len(set))
	for id := range set {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	result := make([]TableID, len(ids))
	for i, id := range ids {
		result[i] = TableID(id)
	}
	return result
}
