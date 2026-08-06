# Declarative catalog and API migration

Bond's catalog is a pre-open storage definition. It gives table and index IDs, logical encodings, and application-owned versions durable names before Pebble opens. It does not own a second database, execute writes, or select a physical schema per mutation.

## Lifecycle

1. Create one `Catalog` with a stable application name and version.
2. Call `DefineTable` and `DefineIndex` for every catalog-bound handle.
3. Call `Validate`, or let `Open` validate. Successful validation freezes the catalog.
4. Pass that exact catalog instance in `Options.Catalog` to the single `bond.Open` call.
5. Call `BindTable` and `BindIndex`. Binding compiles handles over the already-open database and performs no I/O or additional Pebble open.
6. Use the ordinary table/query/batch APIs. No `Set`, `Delete`, table, index, or batch method accepts a physical schema.

The complete Accounts/Sessions/Events workflow is compile-tested in [`examples/catalog`](../examples/catalog/main.go). It declares uint64, byte, and tuple primary keys, secondary indexes, one cross-table batch, and a logical index query.

## Durable descriptors versus runtime callbacks

Every storage-relevant behavior has an explicit name and version:

- catalog name/version;
- stable one-byte table ID and table name;
- table logical-layout and codec descriptors;
- primary-key descriptor and logical kind;
- stable per-table index ID/name;
- index logical-layout, key, order, and predicate descriptors;
- uniqueness, multi-key, and partial-index flags; and
- a physical schema-family descriptor.

`BondTableLayoutV1()` and `BondIndexLayoutV1()` are the only logical layouts implemented in this phase. Unknown layout versions fail validation rather than being accepted as a label with no corresponding key implementation.

Serializers and extractor/order/predicate callbacks are runtime-only. Bond never hashes function pointers, reflected function names, or callback machine code as if they were durable semantics. Changing callback behavior therefore requires the application owner to bump the associated descriptor or layout version. Reusing the old descriptor falsely claims compatibility and is an application bug.

`Unique` records the application's durable uniqueness contract. This phase retains Bond's existing secondary-index encoding and does not add the separate primary-key-in-value layout or new uniqueness enforcement described as future work in the project plan.

## Fingerprint and definition drift

Validation sorts tables and indexes by numeric ID, serializes only the explicit durable fields, and computes a SHA-256 fingerprint. Registration order and callback identity cannot change it. The first catalog open stores a versioned canonical manifest and fingerprint in Bond's reserved keyspace.

Open validates any existing Bond data version before it reconciles or persists a catalog. Only an empty, unversioned database is treated as new; its catalog definition and Bond version are committed atomically. A non-empty database with missing or incompatible Bond version metadata fails without gaining catalog metadata.

The open transaction locks stable Bond-specific claim files on the OS sidecar namespace and configured Pebble filesystem from preflight through commit or failure. These tiny reserved files persist so every process always contends on the same inode; release closes the configured-filesystem lock before the OS lock and never unlinks either path. Existing databases keep them below the database and `bond` directories, so an ordinary open does not require a writable parent. Default-filesystem paths are canonicalized through existing symlinked ancestors before both claiming and I/O; custom VFS paths retain their own namespace semantics.

Failed-open recovery is deliberately conservative: Bond never deletes a Pebble file or fixed sidecar based on a prior existence check or a Pebble-looking filename. While holding both stable claims, a genuinely new store first publishes a synced, versioned external initialization marker in the OS `bond` directory, before `pebble.Open` can create storage artifacts. Its random identity is bound to both its unique filename and canonical contents and is validated strictly on retry. After Pebble opens, Bond also writes its reserved pending-initialization key before sidecar work. A marker-bearing, versionless store is resumable only with the valid external marker; a raw Pebble store without it remains rejected. Catalog definition and Bond version are committed atomically while deleting the internal pending key.

The external marker is permanent immutable Bond provenance, not state that successful initialization clears. Reopening a versioned store validates the marker but never enters either internal-pending recovery path because of it. Bond never truncates, replaces, or removes this marker; independently changed content is reported and preserved.

Fixed metadata uses no-replace publication. Bond writes and syncs an exclusively created same-directory temporary file, hard-links it to the final path, syncs the directory, and removes only the uniquely owned temporary link. An identical destination is accepted and re-synced; different content is a conflict. No fixed destination is truncated, overwritten, or removed during failure cleanup.

Later opens must supply an equivalent catalog. A mismatch returns `*bond.CatalogCompatibilityError` with stable paths such as:

```text
tables[id=1].codec.version: "v1" -> "v2"
tables[id=1].indexes[id=2].predicate.version: "v1" -> "v2"
```

A database with catalog metadata cannot be reopened without `Options.Catalog`. Intentional incompatible changes require an application migration/rebuild; changing the stored fingerprint manually is corruption, not migration.

## Migrating from dynamic tables

The catalog API is the preferred definition path, but the existing dynamic API remains supported as the global-schema fallback:

```go
// Preferred: definition exists before Open.
catalog := bond.NewCatalog("service", "v1")
accountsDef, err := bond.DefineTable(catalog, bond.TableSchema[*Account]{
    Name:           "accounts",
    TableID:        1,
    LogicalLayout:  bond.BondTableLayoutV1(),
    Codec:          bond.Descriptor{Name: "json", Version: "v1"},
    Serializer:     &serializers.JsonSerializer{},
    PrimaryKey:     bond.KeyDescriptor{Descriptor: bond.Descriptor{Name: "account-id", Version: "v1"}, Kind: bond.LogicalKeyUint64},
    PrimaryKeyFunc: func(b bond.KeyBuilder, a *Account) []byte { return b.AddUint64Field(a.ID).Bytes() },
    PhysicalSchema: bond.PKUint64SchemaFamily(),
})
if err != nil { return err }

db, err := bond.Open(path, &bond.Options{Catalog: catalog})
if err != nil { return err }
accounts, err := bond.BindTable(db, accountsDef)
```

To adopt an existing dynamic database, define the same table IDs, serializers, and logical key/index callbacks, give those semantics explicit descriptor versions, then open once with the catalog. If the database has no prior catalog metadata, Bond persists this definition. Bond cannot prove that newly declared callbacks match old opaque callbacks; that compatibility review belongs to the application owner.

`NewTable` remains available after open for legacy/dynamic tables whose IDs are not owned by the supplied catalog. It rejects catalog-owned IDs; use `BindTable` for those definitions, including after adopting a legacy database. This prevents an arbitrary post-open callback or index set from overlapping a fingerprinted keyspace. A dynamic table is not added to the frozen catalog and receives no typed physical-family guarantee. Catalog-bound and dynamic handles still share the same Bond/Pebble database and can participate in the same caller-owned batch.

Bound handles expose explicit read, write, scan, and query methods, but they do not expose `AddIndex`, `ReIndex`, the underlying runtime `Table`, or `Query.Table`. Adding an index requires a new pre-open catalog definition and the appropriate migration/rebuild.

## Physical schema status in Phase 5

`LegacySchemaFamily`, `PKUint64SchemaFamily`, `PKUint32SchemaFamily`, `PKBytesSchemaFamily`, and `PKOpaqueSchemaFamily` are validated, fingerprinted descriptors. The `pk-*` assignments are future-facing metadata for the stock-Pebble feasibility work; they are not registered Pebble `KeySchema` implementations and do not create SST routes.

Production opens continue to install exactly the comparer-derived legacy/default writer and reader. The rejected `bond/full-key/v1-*` experiments remain internal. Storage diagnostics therefore report an empty `catalog_routes` list until a later phase proves and implements writer routing on unmodified pinned Pebble.

## Atomic multi-table writes

Bound handles reuse the existing Bond batch contract:

```go
batch := db.Batch(bond.BatchTypeReadWrite)
defer batch.Close()

if err := accounts.InsertOne(ctx, account, batch); err != nil { return err }
if err := sessions.InsertOne(ctx, session, batch); err != nil { return err }
if err := events.InsertOne(ctx, event, batch); err != nil { return err }
return batch.Commit(bond.Sync)
```

There is one WAL, one Pebble batch, and one atomic commit. A physical-schema argument is intentionally absent.
