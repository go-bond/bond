package bond

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond/serializers"
	"github.com/stretchr/testify/require"
)

type catalogTestRecord struct {
	ID    uint64
	Group string
}

func catalogTestTableSchema(id TableID, name string) TableSchema[*catalogTestRecord] {
	return TableSchema[*catalogTestRecord]{
		Name:          name,
		TableID:       id,
		LogicalLayout: BondTableLayoutV1(),
		Codec:         Descriptor{Name: "json", Version: "v1"},
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    KeyDescriptor{Descriptor: Descriptor{Name: "record-id", Version: "v1"}, Kind: LogicalKeyUint64},
		PrimaryKeyFunc: func(builder KeyBuilder, record *catalogTestRecord) []byte {
			return builder.AddUint64Field(record.ID).Bytes()
		},
		PhysicalSchema: PKUint64SchemaFamily(),
	}
}

func catalogTestIndexSchema(id IndexID, name string) IndexSchema[*catalogTestRecord] {
	return IndexSchema[*catalogTestRecord]{
		Name:          name,
		IndexID:       id,
		LogicalLayout: BondIndexLayoutV1(),
		Key:           KeyDescriptor{Descriptor: Descriptor{Name: "record-group", Version: "v1"}, Kind: LogicalKeyBytes},
		Order:         KeyDescriptor{Descriptor: Descriptor{Name: "no-order", Version: "v1"}, Kind: LogicalKeyNone},
		Predicate:     Descriptor{Name: "all-records", Version: "v1"},
		IndexKeyFunc: func(builder KeyBuilder, record *catalogTestRecord) []byte {
			return builder.AddStringField(record.Group).Bytes()
		},
	}
}

func TestCatalogRejectsDuplicateAndReservedIDs(t *testing.T) {
	catalog := NewCatalog("test", "v1")
	_, err := DefineTable(catalog, catalogTestTableSchema(0, "reserved"))
	require.ErrorContains(t, err, "reserved")

	table, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	_, err = DefineTable(catalog, catalogTestTableSchema(1, "other"))
	require.ErrorContains(t, err, "duplicate catalog table ID 1")

	_, err = DefineIndex(table, catalogTestIndexSchema(PrimaryIndexID, "reserved-primary"))
	require.ErrorContains(t, err, "reserved")
	_, err = DefineIndex(table, catalogTestIndexSchema(BOND_DB_DATA_USER_SPACE_INDEX_ID, "reserved-user-space"))
	require.ErrorContains(t, err, "reserved")
	_, err = DefineIndex(table, catalogTestIndexSchema(1, "by-group"))
	require.NoError(t, err)
	_, err = DefineIndex(table, catalogTestIndexSchema(1, "by-other"))
	require.ErrorContains(t, err, "duplicate catalog index ID 1")
}

func TestCatalogRejectsDuplicateNames(t *testing.T) {
	catalog := NewCatalog("test", "v1")
	table, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	_, err = DefineTable(catalog, catalogTestTableSchema(2, "records"))
	require.ErrorContains(t, err, "duplicate catalog table name")

	_, err = DefineIndex(table, catalogTestIndexSchema(1, "by-group"))
	require.NoError(t, err)
	_, err = DefineIndex(table, catalogTestIndexSchema(2, "by-group"))
	require.ErrorContains(t, err, "duplicate catalog index name")
	require.NoError(t, catalog.Validate())
}

func TestCatalogDefinitionsFreezeAfterValidation(t *testing.T) {
	catalog := NewCatalog("test", "v1")
	table, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	require.NoError(t, catalog.Validate())

	_, err = DefineTable(catalog, catalogTestTableSchema(2, "more-records"))
	require.ErrorIs(t, err, ErrCatalogFrozen)
	_, err = DefineIndex(table, catalogTestIndexSchema(1, "by-group"))
	require.ErrorIs(t, err, ErrCatalogFrozen)
}

func TestCatalogFingerprintStable(t *testing.T) {
	build := func(reverse bool, callbackOffset uint64) *Catalog {
		catalog := NewCatalog("stable", "v1")
		ids := []TableID{1, 2}
		if reverse {
			ids = []TableID{2, 1}
		}
		for _, id := range ids {
			schema := catalogTestTableSchema(id, "table-"+string(rune('0'+id)))
			schema.PrimaryKeyFunc = func(builder KeyBuilder, record *catalogTestRecord) []byte {
				return builder.AddUint64Field(record.ID + callbackOffset).Bytes()
			}
			table, err := DefineTable(catalog, schema)
			require.NoError(t, err)
			indexIDs := []IndexID{1, 2}
			if reverse {
				indexIDs = []IndexID{2, 1}
			}
			for _, indexID := range indexIDs {
				index := catalogTestIndexSchema(indexID, "index-"+string(rune('0'+indexID)))
				index.IndexKeyFunc = func(builder KeyBuilder, record *catalogTestRecord) []byte {
					return builder.AddStringField(record.Group + string(rune(callbackOffset))).Bytes()
				}
				_, err := DefineIndex(table, index)
				require.NoError(t, err)
			}
		}
		return catalog
	}

	first := build(false, 0)
	second := build(true, 7)
	firstFingerprint, err := first.Fingerprint()
	require.NoError(t, err)
	secondFingerprint, err := second.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, firstFingerprint, secondFingerprint)
	firstManifest, err := first.Manifest()
	require.NoError(t, err)
	secondManifest, err := second.Manifest()
	require.NoError(t, err)
	require.Equal(t, firstManifest, secondManifest)
}

func TestCatalogFingerprintDiff(t *testing.T) {
	catalog := NewCatalog("test", "v1")
	table, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	_, err = DefineIndex(table, catalogTestIndexSchema(1, "by-group"))
	require.NoError(t, err)
	stored, err := catalog.Manifest()
	require.NoError(t, err)
	requested := cloneCatalogManifest(stored)
	requested.Version = "v2"
	requested.Tables[0].Codec.Version = "v2"
	requested.Tables[0].PhysicalSchema = LegacySchemaFamily()
	requested.Tables[0].Indexes[0].Predicate.Version = "v2"
	requested.Tables[0].Indexes[0].Unique = true

	changes := DiffCatalogManifests(stored, requested)
	require.Equal(t, []CatalogChange{
		{Path: "catalog.version", Stored: "v1", Requested: "v2"},
		{Path: "tables[id=1].codec.version", Stored: "v1", Requested: "v2"},
		{Path: "tables[id=1].indexes[id=1].predicate.version", Stored: "v1", Requested: "v2"},
		{Path: "tables[id=1].indexes[id=1].unique", Stored: "false", Requested: "true"},
		{Path: "tables[id=1].physical_schema.name", Stored: "bond/pk-u64", Requested: "pebble/default"},
	}, changes)
}

func TestCatalogRejectsUnknownSchemaAndLayout(t *testing.T) {
	tests := map[string]struct {
		mutate func(*TableSchema[*catalogTestRecord])
		want   string
	}{
		"missing layout version": {
			mutate: func(schema *TableSchema[*catalogTestRecord]) { schema.LogicalLayout.Version = "" },
			want:   "descriptor version is required",
		},
		"unknown key kind": {
			mutate: func(schema *TableSchema[*catalogTestRecord]) { schema.PrimaryKey.Kind = LogicalKeyKind("uint128") },
			want:   "unsupported logical key kind",
		},
		"unknown layout": {
			mutate: func(schema *TableSchema[*catalogTestRecord]) {
				schema.LogicalLayout = Descriptor{Name: "application/table", Version: "v1"}
			},
			want: "unsupported logical layout",
		},
		"unknown family": {
			mutate: func(schema *TableSchema[*catalogTestRecord]) {
				schema.PhysicalSchema = SchemaFamilyDescriptor{Name: "application/unknown", Version: "v1"}
			},
			want: "unknown physical schema family",
		},
		"incompatible family": {
			mutate: func(schema *TableSchema[*catalogTestRecord]) { schema.PhysicalSchema = PKBytesSchemaFamily() },
			want:   "requires primary key kind \"bytes\", got \"uint64\"",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			catalog := NewCatalog("test", "v1")
			schema := catalogTestTableSchema(1, "records")
			test.mutate(&schema)
			_, err := DefineTable(catalog, schema)
			require.NoError(t, err)
			require.ErrorContains(t, catalog.Validate(), test.want)
		})
	}
}

func TestOpenPersistsAndValidatesCatalog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "catalog-db")
	first := NewCatalog("application", "v1")
	_, err := DefineTable(first, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	db, err := Open(dir, &Options{Catalog: first})
	require.NoError(t, err)
	stored, err := catalogDefinitionBytes(db.Backend())
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	require.NoError(t, db.Close())

	equivalent := NewCatalog("application", "v1")
	equivalentSchema := catalogTestTableSchema(1, "records")
	// Callback identity and code are intentionally not durable. The developer
	// must bump the explicit descriptor when callback semantics change.
	equivalentSchema.PrimaryKeyFunc = func(builder KeyBuilder, record *catalogTestRecord) []byte {
		return builder.AddUint64Field(record.ID + 100).Bytes()
	}
	_, err = DefineTable(equivalent, equivalentSchema)
	require.NoError(t, err)
	db, err = Open(dir, &Options{Catalog: equivalent})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = Open(dir, nil)
	require.ErrorIs(t, err, ErrCatalogRequired)

	drifted := NewCatalog("application", "v1")
	driftedSchema := catalogTestTableSchema(1, "records")
	driftedSchema.Codec.Version = "v2"
	_, err = DefineTable(drifted, driftedSchema)
	require.NoError(t, err)
	_, err = Open(dir, &Options{Catalog: drifted})
	var compatibilityError *CatalogCompatibilityError
	require.ErrorAs(t, err, &compatibilityError)
	require.Equal(t, "tables[id=1].codec.version", compatibilityError.Changes[0].Path)

	invalid := NewCatalog("application", "v1")
	invalidSchema := catalogTestTableSchema(1, "records")
	invalidSchema.PhysicalSchema = PKBytesSchemaFamily()
	_, err = DefineTable(invalid, invalidSchema)
	require.NoError(t, err)
	invalidDir := filepath.Join(t.TempDir(), "invalid-before-open")
	_, err = Open(invalidDir, &Options{Catalog: invalid})
	require.ErrorContains(t, err, "requires primary key kind")
	require.NoDirExists(t, invalidDir)
}

func TestOpenRejectsCorruptCatalogMetadata(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "catalog-db")
	catalog := NewCatalog("application", "v1")
	_, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	db, err := Open(dir, &Options{Catalog: catalog})
	require.NoError(t, err)
	require.NoError(t, db.Set(catalogDefinitionKey(), []byte(`{"metadata_version":1}`), Sync))
	require.NoError(t, db.Close())

	equivalent := NewCatalog("application", "v1")
	_, err = DefineTable(equivalent, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	_, err = Open(dir, &Options{Catalog: equivalent})
	require.ErrorContains(t, err, "unsupported catalog manifest version")
}

func TestOpenRejectsIncompatibleBondVersionBeforeCatalogPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "incompatible-db")
	db, err := Open(dir, nil)
	require.NoError(t, err)
	incompatibleVersion := BOND_DB_DATA_VERSION + 1
	require.NoError(t, db.Set(bondDataVersionKey(), []byte("2"), Sync))
	require.NoError(t, db.Close())

	catalog := NewCatalog("application", "v1")
	_, err = DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	_, err = Open(dir, &Options{Catalog: catalog})
	require.ErrorContains(t, err, "bond db version is 2 but expecting 1")

	options, _, err := productionPebbleOptions(DefaultPebbleOptions(), false)
	require.NoError(t, err)
	rawDB, err := openPreparedPebble(dir, options)
	require.NoError(t, err)
	defer rawDB.Close()
	_, closer, err := rawDB.Get(catalogDefinitionKey())
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
	version, exists, err := readBondDataVersion(rawDB)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, incompatibleVersion, version)
}

func TestOpenRejectsMissingBondVersionInNonEmptyDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "unversioned-db")
	options, _, err := productionPebbleOptions(DefaultPebbleOptions(), false)
	require.NoError(t, err)
	rawDB, err := openPreparedPebble(dir, options)
	require.NoError(t, err)
	legacyValueKey := NewUserKey("existing-without-version")
	require.NoError(t, rawDB.Set(legacyValueKey, []byte("preserve"), pebble.Sync))
	require.NoError(t, rawDB.Close())

	catalog := NewCatalog("application", "v1")
	_, err = DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	_, err = Open(dir, &Options{Catalog: catalog})
	require.ErrorContains(t, err, "database version metadata is missing from a pre-existing Pebble store")

	options, _, err = productionPebbleOptions(DefaultPebbleOptions(), false)
	require.NoError(t, err)
	rawDB, err = openPreparedPebble(dir, options)
	require.NoError(t, err)
	defer rawDB.Close()
	_, closer, err := rawDB.Get(catalogDefinitionKey())
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
	value, closer, err := rawDB.Get(legacyValueKey)
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), value)
	require.NoError(t, closer.Close())
}

type catalogAccount struct {
	ID      uint64
	Email   string
	Balance uint64
}

type catalogSession struct {
	Token     []byte
	AccountID uint64
	ExpiresAt time.Time
}

type catalogEvent struct {
	TenantID uint32
	Sequence uint64
	Kind     string
	Created  time.Time
}

type catalogExampleDefinitions struct {
	catalog           *Catalog
	accounts          *TableDefinition[*catalogAccount]
	accountsByEmail   *IndexDefinition[*catalogAccount]
	sessions          *TableDefinition[*catalogSession]
	sessionsByAccount *IndexDefinition[*catalogSession]
	events            *TableDefinition[*catalogEvent]
	eventsByKind      *IndexDefinition[*catalogEvent]
}

func buildCatalogExampleDefinitions(t *testing.T) catalogExampleDefinitions {
	t.Helper()
	catalog := NewCatalog("accounts-service", "v1")
	accounts, err := DefineTable(catalog, TableSchema[*catalogAccount]{
		Name:          "accounts",
		TableID:       1,
		LogicalLayout: BondTableLayoutV1(),
		Codec:         Descriptor{Name: "json", Version: "v1"},
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    KeyDescriptor{Descriptor: Descriptor{Name: "account-id", Version: "v1"}, Kind: LogicalKeyUint64},
		PrimaryKeyFunc: func(builder KeyBuilder, account *catalogAccount) []byte {
			return builder.AddUint64Field(account.ID).Bytes()
		},
		PhysicalSchema: PKUint64SchemaFamily(),
	})
	require.NoError(t, err)
	accountsByEmail, err := DefineIndex(accounts, IndexSchema[*catalogAccount]{
		Name:          "by-email",
		IndexID:       1,
		LogicalLayout: BondIndexLayoutV1(),
		Unique:        true,
		Key:           KeyDescriptor{Descriptor: Descriptor{Name: "account-email", Version: "v1"}, Kind: LogicalKeyBytes},
		Order:         KeyDescriptor{Descriptor: Descriptor{Name: "no-order", Version: "v1"}, Kind: LogicalKeyNone},
		Predicate:     Descriptor{Name: "all-accounts", Version: "v1"},
		IndexKeyFunc: func(builder KeyBuilder, account *catalogAccount) []byte {
			return builder.AddStringField(account.Email).Bytes()
		},
	})
	require.NoError(t, err)

	sessions, err := DefineTable(catalog, TableSchema[*catalogSession]{
		Name:          "sessions",
		TableID:       2,
		LogicalLayout: BondTableLayoutV1(),
		Codec:         Descriptor{Name: "json", Version: "v1"},
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    KeyDescriptor{Descriptor: Descriptor{Name: "session-token", Version: "v1"}, Kind: LogicalKeyBytes},
		PrimaryKeyFunc: func(builder KeyBuilder, session *catalogSession) []byte {
			return builder.AddBytesField(session.Token).Bytes()
		},
		PhysicalSchema: PKBytesSchemaFamily(),
	})
	require.NoError(t, err)
	sessionsByAccount, err := DefineIndex(sessions, IndexSchema[*catalogSession]{
		Name:          "by-account-expiry",
		IndexID:       1,
		LogicalLayout: BondIndexLayoutV1(),
		Key:           KeyDescriptor{Descriptor: Descriptor{Name: "session-account-id", Version: "v1"}, Kind: LogicalKeyUint64},
		Order:         KeyDescriptor{Descriptor: Descriptor{Name: "session-expiry", Version: "v1"}, Kind: LogicalKeyOpaque},
		Predicate:     Descriptor{Name: "all-sessions", Version: "v1"},
		IndexKeyFunc: func(builder KeyBuilder, session *catalogSession) []byte {
			return builder.AddUint64Field(session.AccountID).Bytes()
		},
		IndexOrderFunc: func(order IndexOrder, session *catalogSession) IndexOrder {
			return order.OrderInt64(session.ExpiresAt.UnixNano(), IndexOrderTypeASC)
		},
	})
	require.NoError(t, err)

	events, err := DefineTable(catalog, TableSchema[*catalogEvent]{
		Name:          "events",
		TableID:       3,
		LogicalLayout: BondTableLayoutV1(),
		Codec:         Descriptor{Name: "json", Version: "v1"},
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    KeyDescriptor{Descriptor: Descriptor{Name: "tenant-sequence", Version: "v1"}, Kind: LogicalKeyTuple},
		PrimaryKeyFunc: func(builder KeyBuilder, event *catalogEvent) []byte {
			return builder.AddUint32Field(event.TenantID).AddUint64Field(event.Sequence).Bytes()
		},
		PhysicalSchema: PKOpaqueSchemaFamily(),
	})
	require.NoError(t, err)
	eventsByKind, err := DefineIndex(events, IndexSchema[*catalogEvent]{
		Name:          "by-kind-created",
		IndexID:       1,
		LogicalLayout: BondIndexLayoutV1(),
		Key:           KeyDescriptor{Descriptor: Descriptor{Name: "event-kind", Version: "v1"}, Kind: LogicalKeyBytes},
		Order:         KeyDescriptor{Descriptor: Descriptor{Name: "event-created", Version: "v1"}, Kind: LogicalKeyOpaque},
		Predicate:     Descriptor{Name: "all-events", Version: "v1"},
		IndexKeyFunc: func(builder KeyBuilder, event *catalogEvent) []byte {
			return builder.AddStringField(event.Kind).Bytes()
		},
		IndexOrderFunc: func(order IndexOrder, event *catalogEvent) IndexOrder {
			return order.OrderInt64(event.Created.UnixNano(), IndexOrderTypeASC)
		},
	})
	require.NoError(t, err)
	require.NoError(t, catalog.Validate())
	return catalogExampleDefinitions{
		catalog:           catalog,
		accounts:          accounts,
		accountsByEmail:   accountsByEmail,
		sessions:          sessions,
		sessionsByAccount: sessionsByAccount,
		events:            events,
		eventsByKind:      eventsByKind,
	}
}

func TestBoundTablesAtomicCrossTableBatch(t *testing.T) {
	definitions := buildCatalogExampleDefinitions(t)
	db, err := Open(filepath.Join(t.TempDir(), "catalog-db"), &Options{Catalog: definitions.catalog})
	require.NoError(t, err)
	defer db.Close()

	accounts, err := BindTable(db, definitions.accounts)
	require.NoError(t, err)
	sessions, err := BindTable(db, definitions.sessions)
	require.NoError(t, err)
	events, err := BindTable(db, definitions.events)
	require.NoError(t, err)
	accountsByEmail, err := BindIndex(accounts, definitions.accountsByEmail)
	require.NoError(t, err)
	sessionsByAccount, err := BindIndex(sessions, definitions.sessionsByAccount)
	require.NoError(t, err)
	eventsByKind, err := BindIndex(events, definitions.eventsByKind)
	require.NoError(t, err)
	require.Same(t, db.Backend(), accounts.Database().Backend())
	require.Same(t, db.Backend(), sessions.Database().Backend())
	require.Same(t, db.Backend(), events.Database().Backend())

	now := time.Unix(1_900_000_000, 0).UTC()
	account := &catalogAccount{ID: 42, Email: "peter@example.com", Balance: 125_000}
	session := &catalogSession{Token: []byte("session-7eb9"), AccountID: 42, ExpiresAt: now.Add(time.Hour)}
	event := &catalogEvent{TenantID: 7, Sequence: 991, Kind: "account.created", Created: now}
	batch := db.Batch(BatchTypeReadWrite)
	defer batch.Close()
	require.NoError(t, accounts.InsertOne(context.Background(), account, batch))
	require.NoError(t, sessions.InsertOne(context.Background(), session, batch))
	require.NoError(t, events.InsertOne(context.Background(), event, batch))
	require.False(t, accounts.Exist(account))
	require.False(t, sessions.Exist(session))
	require.False(t, events.Exist(event))
	require.NoError(t, batch.Commit(Sync))

	storedAccount, err := accounts.GetPoint(context.Background(), &catalogAccount{ID: 42})
	require.NoError(t, err)
	require.Equal(t, account, storedAccount)
	storedSession, err := sessions.GetPoint(context.Background(), &catalogSession{Token: []byte("session-7eb9")})
	require.NoError(t, err)
	require.Equal(t, session, storedSession)
	storedEvent, err := events.GetPoint(context.Background(), &catalogEvent{TenantID: 7, Sequence: 991})
	require.NoError(t, err)
	require.Equal(t, event, storedEvent)

	var byEmail []*catalogAccount
	require.NoError(t, accountsByEmail.Query(NewSelectorPoint(&catalogAccount{Email: account.Email})).Execute(context.Background(), &byEmail))
	require.Equal(t, []*catalogAccount{account}, byEmail)
	var byAccount []*catalogSession
	require.NoError(t, sessionsByAccount.Query(NewSelectorPoint(&catalogSession{AccountID: 42})).Execute(context.Background(), &byAccount))
	require.Equal(t, []*catalogSession{session}, byAccount)
	var byKind []*catalogEvent
	require.NoError(t, eventsByKind.Query(NewSelectorPoint(&catalogEvent{Kind: event.Kind})).Execute(context.Background(), &byKind))
	require.Equal(t, []*catalogEvent{event}, byKind)
}

func TestBindRejectsForeignDefinition(t *testing.T) {
	opened := buildCatalogExampleDefinitions(t)
	foreign := buildCatalogExampleDefinitions(t)
	db, err := Open(filepath.Join(t.TempDir(), "catalog-db"), &Options{Catalog: opened.catalog})
	require.NoError(t, err)
	defer db.Close()

	_, err = BindTable(db, foreign.accounts)
	require.ErrorContains(t, err, "not the catalog instance supplied to Open")
}

func TestBoundHandlesDoNotExposeRuntimeMutation(t *testing.T) {
	definitions := buildCatalogExampleDefinitions(t)
	db, err := Open(filepath.Join(t.TempDir(), "catalog-db"), &Options{Catalog: definitions.catalog})
	require.NoError(t, err)
	defer db.Close()
	accounts, err := BindTable(db, definitions.accounts)
	require.NoError(t, err)

	_, exposesAddIndex := reflect.TypeOf(accounts).MethodByName("AddIndex")
	_, exposesReIndex := reflect.TypeOf(accounts).MethodByName("ReIndex")
	_, queryExposesRuntimeTable := reflect.TypeOf(accounts.Query()).MethodByName("Table")
	require.False(t, exposesAddIndex)
	require.False(t, exposesReIndex)
	require.False(t, queryExposesRuntimeTable)
}

func TestBoundQueryIntersectionsRequireExactBoundTableHandle(t *testing.T) {
	catalog := NewCatalog("query-owners", "v1")
	firstDefinition, err := DefineTable(catalog, catalogTestTableSchema(1, "first"))
	require.NoError(t, err)
	firstIndexDefinition, err := DefineIndex(firstDefinition, catalogTestIndexSchema(1, "first-by-group"))
	require.NoError(t, err)
	secondDefinition, err := DefineTable(catalog, catalogTestTableSchema(2, "second"))
	require.NoError(t, err)
	secondIndexDefinition, err := DefineIndex(secondDefinition, catalogTestIndexSchema(1, "second-by-group"))
	require.NoError(t, err)

	firstDB, err := Open(filepath.Join(t.TempDir(), "first-open"), &Options{Catalog: catalog})
	require.NoError(t, err)
	defer firstDB.Close()
	first, err := BindTable(firstDB, firstDefinition)
	require.NoError(t, err)
	firstAgain, err := BindTable(firstDB, firstDefinition)
	require.NoError(t, err)
	second, err := BindTable(firstDB, secondDefinition)
	require.NoError(t, err)
	firstIndex, err := BindIndex(first, firstIndexDefinition)
	require.NoError(t, err)
	secondIndex, err := BindIndex(second, secondIndexDefinition)
	require.NoError(t, err)

	selector := NewSelectorPoint(&catalogTestRecord{Group: "group"})
	var records []*catalogTestRecord
	require.NoError(t, firstIndex.Query(selector).Intersects(firstIndex.Query(selector)).Execute(context.Background(), &records))

	err = firstIndex.Query(selector).Intersects(secondIndex.Query(selector)).Execute(context.Background(), &records)
	require.ErrorContains(t, err, "different bound table handles")
	err = first.Query().Intersects(firstAgain.Query()).Execute(context.Background(), &records)
	require.ErrorContains(t, err, "different bound table handles")

	secondDB, err := Open(filepath.Join(t.TempDir(), "second-open"), &Options{Catalog: catalog})
	require.NoError(t, err)
	defer secondDB.Close()
	firstOnSecondDB, err := BindTable(secondDB, firstDefinition)
	require.NoError(t, err)
	err = first.Query().Intersects(firstOnSecondDB.Query()).Execute(context.Background(), &records)
	require.ErrorContains(t, err, "different bound table handles")
}

func TestCatalogOwnedTableIDRejectsDynamicConstruction(t *testing.T) {
	definitions := buildCatalogExampleDefinitions(t)
	db, err := Open(filepath.Join(t.TempDir(), "catalog-db"), &Options{Catalog: definitions.catalog})
	require.NoError(t, err)
	defer db.Close()

	wrappedDB := struct{ DB }{DB: db}
	require.PanicsWithError(t,
		"bond: table ID 1 is owned by pre-open catalog \"accounts-service\"/\"v1\"; use BindTable with its definition instead of NewTable",
		func() {
			NewTable(TableOptions[*catalogAccount]{
				DB:        wrappedDB,
				TableID:   1,
				TableName: "overlap",
				TablePrimaryKeyFunc: func(builder KeyBuilder, account *catalogAccount) []byte {
					return builder.AddStringField(account.Email).Bytes()
				},
			})
		},
	)
	bound, err := BindTable(db, definitions.accounts)
	require.NoError(t, err)
	require.Equal(t, TableID(1), bound.ID())
}

func TestCatalogBoundAndLegacyTablesCoexist(t *testing.T) {
	definitions := buildCatalogExampleDefinitions(t)
	db, err := Open(filepath.Join(t.TempDir(), "catalog-db"), &Options{Catalog: definitions.catalog})
	require.NoError(t, err)
	defer db.Close()
	bound, err := BindTable(db, definitions.accounts)
	require.NoError(t, err)
	legacy := NewTable(TableOptions[*catalogTestRecord]{
		DB:        db,
		TableID:   9,
		TableName: "legacy-dynamic",
		TablePrimaryKeyFunc: func(builder KeyBuilder, record *catalogTestRecord) []byte {
			return builder.AddUint64Field(record.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	require.NoError(t, bound.InsertOne(context.Background(), &catalogAccount{ID: 1, Email: "bound@example.com"}))
	require.NoError(t, legacy.Insert(context.Background(), []*catalogTestRecord{{ID: 1, Group: "legacy"}}))
	require.True(t, bound.Exist(&catalogAccount{ID: 1}))
	require.True(t, legacy.Exist(&catalogTestRecord{ID: 1}))

	diagnostics, err := db.StorageDiagnostics()
	require.NoError(t, err)
	require.Equal(t, DefaultKeySchemaName(), diagnostics.ActiveWriter)
	require.Equal(t, []string{DefaultKeySchemaName()}, diagnostics.RegisteredReaders)
	require.Empty(t, diagnostics.CatalogRoutes)
}

func TestCatalogAdoptsLegacyDynamicDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "legacy-db")
	legacyDB, err := Open(dir, nil)
	require.NoError(t, err)
	legacy := NewTable(TableOptions[*catalogTestRecord]{
		DB:        legacyDB,
		TableID:   1,
		TableName: "records",
		TablePrimaryKeyFunc: func(builder KeyBuilder, record *catalogTestRecord) []byte {
			return builder.AddUint64Field(record.ID).Bytes()
		},
		Serializer: &serializers.JsonSerializer{},
	})
	written := &catalogTestRecord{ID: 7, Group: "before-catalog"}
	require.NoError(t, legacy.Insert(context.Background(), []*catalogTestRecord{written}))
	require.NoError(t, legacyDB.Close())

	catalog := NewCatalog("adopted", "v1")
	definition, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	db, err := Open(dir, &Options{Catalog: catalog})
	require.NoError(t, err)
	defer db.Close()
	bound, err := BindTable(db, definition)
	require.NoError(t, err)
	read, err := bound.GetPoint(context.Background(), &catalogTestRecord{ID: 7})
	require.NoError(t, err)
	require.Equal(t, written, read)
}

func TestCatalogDoesNotActivatePhysicalFamilies(t *testing.T) {
	definitions := buildCatalogExampleDefinitions(t)
	db, err := Open(filepath.Join(t.TempDir(), "catalog-db"), &Options{Catalog: definitions.catalog})
	require.NoError(t, err)
	defer db.Close()
	diagnostics, err := db.StorageDiagnostics()
	require.NoError(t, err)
	require.Equal(t, DefaultKeySchemaName(), diagnostics.ActiveWriter)
	require.Equal(t, []string{DefaultKeySchemaName()}, diagnostics.RegisteredReaders)
	require.Empty(t, diagnostics.CatalogRoutes)
}
