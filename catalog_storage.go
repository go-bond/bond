package bond

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

const catalogMetadataVersion = 1

type persistedCatalogDefinition struct {
	MetadataVersion int             `json:"metadata_version"`
	Fingerprint     string          `json:"fingerprint"`
	Manifest        CatalogManifest `json:"manifest"`
}

type catalogReconciliation struct {
	catalog        *Catalog
	requiresCommit bool
}

func inspectCatalogDefinition(db *pebble.DB, catalog *Catalog) (catalogReconciliation, error) {
	storedBytes, closer, err := db.Get(catalogDefinitionKey())
	if errors.Is(err, pebble.ErrNotFound) {
		if catalog == nil {
			return catalogReconciliation{}, nil
		}
		return catalogReconciliation{catalog: catalog, requiresCommit: true}, nil
	}
	if err != nil {
		return catalogReconciliation{}, fmt.Errorf("bond: read catalog definition: %w", err)
	}
	data := append([]byte(nil), storedBytes...)
	if err := closer.Close(); err != nil {
		return catalogReconciliation{}, fmt.Errorf("bond: close catalog definition value: %w", err)
	}

	stored, err := decodePersistedCatalogDefinition(data)
	if err != nil {
		return catalogReconciliation{}, err
	}
	if catalog == nil {
		return catalogReconciliation{}, fmt.Errorf(
			"%w %q/%q (fingerprint %s); supply the same pre-open definition in Options.Catalog",
			ErrCatalogRequired,
			stored.Manifest.Name,
			stored.Manifest.Version,
			stored.Fingerprint,
		)
	}
	requestedManifest, err := catalog.Manifest()
	if err != nil {
		return catalogReconciliation{}, err
	}
	requestedFingerprint, err := catalog.Fingerprint()
	if err != nil {
		return catalogReconciliation{}, err
	}
	if stored.Fingerprint == requestedFingerprint {
		return catalogReconciliation{}, nil
	}
	return catalogReconciliation{}, &CatalogCompatibilityError{
		StoredFingerprint:    stored.Fingerprint,
		RequestedFingerprint: requestedFingerprint,
		Changes:              DiffCatalogManifests(stored.Manifest, requestedManifest),
	}
}

func (r catalogReconciliation) commit(db *pebble.DB) error {
	if !r.requiresCommit {
		return nil
	}
	return persistCatalogDefinition(db, r.catalog)
}

func persistCatalogDefinition(db *pebble.DB, catalog *Catalog) error {
	data, err := encodeCatalogDefinition(catalog)
	if err != nil {
		return err
	}
	if err := db.Set(catalogDefinitionKey(), data, pebble.Sync); err != nil {
		return fmt.Errorf("bond: persist catalog definition: %w", err)
	}
	return nil
}

func encodeCatalogDefinition(catalog *Catalog) ([]byte, error) {
	manifest, err := catalog.Manifest()
	if err != nil {
		return nil, err
	}
	fingerprint, err := catalog.Fingerprint()
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(persistedCatalogDefinition{
		MetadataVersion: catalogMetadataVersion,
		Fingerprint:     fingerprint,
		Manifest:        manifest,
	})
	if err != nil {
		return nil, fmt.Errorf("bond: encode catalog definition: %w", err)
	}
	return data, nil
}

func decodePersistedCatalogDefinition(data []byte) (persistedCatalogDefinition, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var stored persistedCatalogDefinition
	if err := decoder.Decode(&stored); err != nil {
		return persistedCatalogDefinition{}, fmt.Errorf("bond: decode persisted catalog definition: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return persistedCatalogDefinition{}, fmt.Errorf("bond: decode persisted catalog definition: %w", err)
	}
	if stored.MetadataVersion != catalogMetadataVersion {
		return persistedCatalogDefinition{}, fmt.Errorf(
			"bond: unsupported catalog metadata version %d (supported %d)",
			stored.MetadataVersion,
			catalogMetadataVersion,
		)
	}
	if err := validatePersistedCatalogManifest(stored.Manifest); err != nil {
		return persistedCatalogDefinition{}, err
	}
	actualFingerprint, err := fingerprintCatalogManifest(stored.Manifest)
	if err != nil {
		return persistedCatalogDefinition{}, err
	}
	if actualFingerprint != stored.Fingerprint {
		return persistedCatalogDefinition{}, fmt.Errorf(
			"bond: persisted catalog fingerprint is corrupt: metadata has %q, manifest computes %q",
			stored.Fingerprint,
			actualFingerprint,
		)
	}
	return stored, nil
}

func validatePersistedCatalogManifest(manifest CatalogManifest) error {
	if manifest.ManifestVersion != catalogManifestVersion {
		return fmt.Errorf(
			"bond: unsupported catalog manifest version %d (supported %d)",
			manifest.ManifestVersion,
			catalogManifestVersion,
		)
	}
	if err := validateNameVersion("persisted catalog", Descriptor{Name: manifest.Name, Version: manifest.Version}); err != nil {
		return err
	}
	tableNames := make(map[string]struct{}, len(manifest.Tables))
	var previousTableID TableID
	for i, table := range manifest.Tables {
		if table.ID == BOND_DB_DATA_TABLE_ID {
			return fmt.Errorf("bond: persisted catalog uses reserved table ID %d", table.ID)
		}
		if i > 0 && table.ID <= previousTableID {
			return errors.New("bond: persisted catalog table IDs are not strictly increasing")
		}
		previousTableID = table.ID
		if table.Name == "" {
			return fmt.Errorf("bond: persisted catalog table ID %d has an empty name", table.ID)
		}
		if _, exists := tableNames[table.Name]; exists {
			return fmt.Errorf("bond: persisted catalog has duplicate table name %q", table.Name)
		}
		tableNames[table.Name] = struct{}{}
		prefix := fmt.Sprintf("persisted catalog table %q (ID %d)", table.Name, table.ID)
		if err := validateNameVersion(prefix+" logical layout", table.LogicalLayout); err != nil {
			return err
		}
		if table.LogicalLayout != BondTableLayoutV1() {
			return fmt.Errorf("bond: %s uses unsupported logical layout %s/%s", prefix, table.LogicalLayout.Name, table.LogicalLayout.Version)
		}
		if err := validateNameVersion(prefix+" codec", table.Codec); err != nil {
			return err
		}
		if err := validateKeyDescriptor(prefix+" primary key", table.PrimaryKey, false); err != nil {
			return err
		}
		if err := validatePhysicalFamily(prefix, table.PhysicalSchema, table.PrimaryKey.Kind); err != nil {
			return err
		}
		if err := validatePersistedIndexes(prefix, table.Indexes); err != nil {
			return err
		}
	}
	return nil
}

func validatePersistedIndexes(tablePrefix string, indexes []CatalogIndexManifest) error {
	names := make(map[string]struct{}, len(indexes))
	var previousID IndexID
	for i, index := range indexes {
		if index.ID == PrimaryIndexID || index.ID == BOND_DB_DATA_USER_SPACE_INDEX_ID {
			return fmt.Errorf("bond: %s uses reserved index ID %d", tablePrefix, index.ID)
		}
		if i > 0 && index.ID <= previousID {
			return fmt.Errorf("bond: %s index IDs are not strictly increasing", tablePrefix)
		}
		previousID = index.ID
		if index.Name == "" {
			return fmt.Errorf("bond: %s index ID %d has an empty name", tablePrefix, index.ID)
		}
		if _, exists := names[index.Name]; exists {
			return fmt.Errorf("bond: %s has duplicate index name %q", tablePrefix, index.Name)
		}
		names[index.Name] = struct{}{}
		prefix := fmt.Sprintf("%s index %q (ID %d)", tablePrefix, index.Name, index.ID)
		if err := validateNameVersion(prefix+" logical layout", index.LogicalLayout); err != nil {
			return err
		}
		if index.LogicalLayout != BondIndexLayoutV1() {
			return fmt.Errorf("bond: %s uses unsupported logical layout %s/%s", prefix, index.LogicalLayout.Name, index.LogicalLayout.Version)
		}
		if err := validateKeyDescriptor(prefix+" key", index.Key, false); err != nil {
			return err
		}
		if err := validateKeyDescriptor(prefix+" order", index.Order, true); err != nil {
			return err
		}
		if err := validateNameVersion(prefix+" predicate", index.Predicate); err != nil {
			return err
		}
	}
	return nil
}

func catalogDefinitionKey() []byte {
	return KeyEncode(Key{
		TableID:    BOND_DB_DATA_TABLE_ID,
		IndexID:    PrimaryIndexID,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("__bond_catalog_definition_v1__"),
	})
}

// catalogDefinitionBytes is used by focused diagnostics/tests without exposing
// the reserved metadata key as a public mutation surface.
func catalogDefinitionBytes(db *pebble.DB) ([]byte, error) {
	data, closer, err := db.Get(catalogDefinitionKey())
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), data...), nil
}
