package bond_test

import (
	"path/filepath"
	"testing"

	"github.com/go-bond/bond"
	"github.com/go-bond/bond/serializers"
	"github.com/stretchr/testify/require"
)

type externalCatalogRecord struct {
	ID uint64
}

// lyingCatalogDB demonstrates that exported wrapper methods are informational:
// authorization remains anchored to the opened database's private identity.
type lyingCatalogDB struct {
	bond.DB
}

func (lyingCatalogDB) Catalog() *bond.Catalog { return nil }

func TestCatalogAuthorizationCannotBeOverriddenByExternalDBWrapper(t *testing.T) {
	catalog := bond.NewCatalog("external-wrapper", "v1")
	definition, err := bond.DefineTable(catalog, bond.TableSchema[*externalCatalogRecord]{
		Name:          "records",
		TableID:       1,
		LogicalLayout: bond.BondTableLayoutV1(),
		Codec:         bond.Descriptor{Name: "json", Version: "v1"},
		Serializer:    &serializers.JsonSerializer{},
		PrimaryKey: bond.KeyDescriptor{
			Descriptor: bond.Descriptor{Name: "record-id", Version: "v1"},
			Kind:       bond.LogicalKeyUint64,
		},
		PrimaryKeyFunc: func(builder bond.KeyBuilder, record *externalCatalogRecord) []byte {
			return builder.AddUint64Field(record.ID).Bytes()
		},
		PhysicalSchema: bond.PKUint64SchemaFamily(),
	})
	require.NoError(t, err)
	db, err := bond.Open(filepath.Join(t.TempDir(), "catalog-db"), &bond.Options{Catalog: catalog})
	require.NoError(t, err)
	defer db.Close()

	wrapper := lyingCatalogDB{DB: db}
	require.Nil(t, wrapper.Catalog())
	require.PanicsWithError(t,
		"bond: table ID 1 is owned by pre-open catalog \"external-wrapper\"/\"v1\"; use BindTable with its definition instead of NewTable",
		func() {
			bond.NewTable(bond.TableOptions[*externalCatalogRecord]{
				DB:        wrapper,
				TableID:   1,
				TableName: "overlap",
				TablePrimaryKeyFunc: func(builder bond.KeyBuilder, record *externalCatalogRecord) []byte {
					return builder.AddUint64Field(record.ID).Bytes()
				},
			})
		},
	)
	bound, err := bond.BindTable(wrapper, definition)
	require.NoError(t, err)
	require.Equal(t, bond.TableID(1), bound.ID())
}
