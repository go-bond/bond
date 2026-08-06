// Command catalog demonstrates the preferred declarative Bond API.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/serializers"
)

type Account struct {
	ID      uint64
	Email   string
	Status  string
	Balance uint64
}

type Session struct {
	Token     []byte
	AccountID uint64
	ExpiresAt time.Time
}

type Event struct {
	TenantID uint32
	Sequence uint64
	Kind     string
	Created  time.Time
	Payload  []byte
}

type definitions struct {
	catalog           *bond.Catalog
	accounts          *bond.TableDefinition[*Account]
	accountsByEmail   *bond.IndexDefinition[*Account]
	sessions          *bond.TableDefinition[*Session]
	sessionsByAccount *bond.IndexDefinition[*Session]
	events            *bond.TableDefinition[*Event]
	eventsByKind      *bond.IndexDefinition[*Event]
}

var (
	tableLayout = bond.BondTableLayoutV1()
	indexLayout = bond.BondIndexLayoutV1()
	jsonCodec   = bond.Descriptor{Name: "json", Version: "v1"}
	noOrder     = bond.KeyDescriptor{
		Descriptor: bond.Descriptor{Name: "no-order", Version: "v1"},
		Kind:       bond.LogicalKeyNone,
	}
)

func defineCatalog() (definitions, error) {
	catalog := bond.NewCatalog("accounts-service", "v1")
	accounts, err := bond.DefineTable(catalog, bond.TableSchema[*Account]{
		Name:          "accounts",
		TableID:       1,
		LogicalLayout: tableLayout,
		Codec:         jsonCodec,
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "account-id", Version: "v1"}, Kind: bond.LogicalKeyUint64},
		PrimaryKeyFunc: func(builder bond.KeyBuilder, account *Account) []byte {
			return builder.AddUint64Field(account.ID).Bytes()
		},
		// Descriptive until a later stock-Pebble phase proves routing support.
		PhysicalSchema: bond.PKUint64SchemaFamily(),
	})
	if err != nil {
		return definitions{}, err
	}
	accountsByEmail, err := bond.DefineIndex(accounts, bond.IndexSchema[*Account]{
		Name:          "by-email",
		IndexID:       1,
		LogicalLayout: indexLayout,
		Unique:        true,
		Key:           bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "account-email", Version: "v1"}, Kind: bond.LogicalKeyBytes},
		Order:         noOrder,
		Predicate:     bond.Descriptor{Name: "all-accounts", Version: "v1"},
		IndexKeyFunc: func(builder bond.KeyBuilder, account *Account) []byte {
			return builder.AddStringField(account.Email).Bytes()
		},
	})
	if err != nil {
		return definitions{}, err
	}

	sessions, err := bond.DefineTable(catalog, bond.TableSchema[*Session]{
		Name:          "sessions",
		TableID:       2,
		LogicalLayout: tableLayout,
		Codec:         jsonCodec,
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "session-token", Version: "v1"}, Kind: bond.LogicalKeyBytes},
		PrimaryKeyFunc: func(builder bond.KeyBuilder, session *Session) []byte {
			return builder.AddBytesField(session.Token).Bytes()
		},
		PhysicalSchema: bond.PKBytesSchemaFamily(),
	})
	if err != nil {
		return definitions{}, err
	}
	sessionsByAccount, err := bond.DefineIndex(sessions, bond.IndexSchema[*Session]{
		Name:          "by-account-expiry",
		IndexID:       1,
		LogicalLayout: indexLayout,
		Key:           bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "session-account", Version: "v1"}, Kind: bond.LogicalKeyUint64},
		Order:         bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "session-expiry", Version: "v1"}, Kind: bond.LogicalKeyOpaque},
		Predicate:     bond.Descriptor{Name: "all-sessions", Version: "v1"},
		IndexKeyFunc: func(builder bond.KeyBuilder, session *Session) []byte {
			return builder.AddUint64Field(session.AccountID).Bytes()
		},
		IndexOrderFunc: func(order bond.IndexOrder, session *Session) bond.IndexOrder {
			return order.OrderInt64(session.ExpiresAt.UnixNano(), bond.IndexOrderTypeASC)
		},
	})
	if err != nil {
		return definitions{}, err
	}

	events, err := bond.DefineTable(catalog, bond.TableSchema[*Event]{
		Name:          "events",
		TableID:       3,
		LogicalLayout: tableLayout,
		Codec:         jsonCodec,
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey:    bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "tenant-sequence", Version: "v1"}, Kind: bond.LogicalKeyTuple},
		PrimaryKeyFunc: func(builder bond.KeyBuilder, event *Event) []byte {
			return builder.AddUint32Field(event.TenantID).AddUint64Field(event.Sequence).Bytes()
		},
		PhysicalSchema: bond.PKOpaqueSchemaFamily(),
	})
	if err != nil {
		return definitions{}, err
	}
	eventsByKind, err := bond.DefineIndex(events, bond.IndexSchema[*Event]{
		Name:          "by-kind-created",
		IndexID:       1,
		LogicalLayout: indexLayout,
		Key:           bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "event-kind", Version: "v1"}, Kind: bond.LogicalKeyBytes},
		Order:         bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "event-created", Version: "v1"}, Kind: bond.LogicalKeyOpaque},
		Predicate:     bond.Descriptor{Name: "all-events", Version: "v1"},
		IndexKeyFunc: func(builder bond.KeyBuilder, event *Event) []byte {
			return builder.AddStringField(event.Kind).Bytes()
		},
		IndexOrderFunc: func(order bond.IndexOrder, event *Event) bond.IndexOrder {
			return order.OrderInt64(event.Created.UnixNano(), bond.IndexOrderTypeASC)
		},
	})
	if err != nil {
		return definitions{}, err
	}
	if err := catalog.Validate(); err != nil {
		return definitions{}, err
	}
	return definitions{
		catalog:           catalog,
		accounts:          accounts,
		accountsByEmail:   accountsByEmail,
		sessions:          sessions,
		sessionsByAccount: sessionsByAccount,
		events:            events,
		eventsByKind:      eventsByKind,
	}, nil
}

func run() error {
	defs, err := defineCatalog()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "bond-catalog-example-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := bond.Open(dir, &bond.Options{Catalog: defs.catalog})
	if err != nil {
		return err
	}
	defer db.Close()

	accounts, err := bond.BindTable(db, defs.accounts)
	if err != nil {
		return err
	}
	accountsByEmail, err := bond.BindIndex(accounts, defs.accountsByEmail)
	if err != nil {
		return err
	}
	sessions, err := bond.BindTable(db, defs.sessions)
	if err != nil {
		return err
	}
	_, err = bond.BindIndex(sessions, defs.sessionsByAccount)
	if err != nil {
		return err
	}
	events, err := bond.BindTable(db, defs.events)
	if err != nil {
		return err
	}
	_, err = bond.BindIndex(events, defs.eventsByKind)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	account := &Account{ID: 42, Email: "peter@example.com", Status: "active", Balance: 125_000}
	batch := db.Batch(bond.BatchTypeReadWrite)
	defer batch.Close()
	if err := accounts.InsertOne(context.Background(), account, batch); err != nil {
		return err
	}
	if err := sessions.InsertOne(context.Background(), &Session{Token: []byte("session-7eb9"), AccountID: 42, ExpiresAt: now.Add(time.Hour)}, batch); err != nil {
		return err
	}
	if err := events.InsertOne(context.Background(), &Event{TenantID: 7, Sequence: 991, Kind: "account.created", Created: now}, batch); err != nil {
		return err
	}
	if err := batch.Commit(bond.Sync); err != nil {
		return err
	}

	var matches []*Account
	if err := accountsByEmail.Query(bond.NewSelectorPoint(&Account{Email: account.Email})).Execute(context.Background(), &matches); err != nil {
		return err
	}
	fmt.Printf("found %d account\n", len(matches))
	return nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
