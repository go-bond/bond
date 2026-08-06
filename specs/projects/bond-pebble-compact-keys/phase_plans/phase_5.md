---
status: complete
---

# Phase 5: Declarative Catalog and Bound Handles

## Overview

This phase adds a pre-open, immutable storage catalog without changing Bond's logical key encoding or the production Pebble writer. Applications declare stable table/index identities, durable codec/layout/key/order/predicate descriptors, and future physical-family assignments before `Open`. Validation freezes the catalog, derives a canonical manifest and SHA-256 fingerprint, and open-time reconciliation persists or compares that manifest in Bond's reserved keyspace with field-level drift diagnostics. Generic definitions then bind to the single opened Bond database and compile into the existing table/index implementation, retaining one Pebble database and caller-owned atomic batches. Existing post-open `NewTable` callers remain the legacy/global-schema fallback.

## Steps

1. Add immutable catalog, descriptor, manifest, fingerprint, and structured-diff types in `catalog.go`. Validate catalog/table/index names and versions, reserved or duplicate IDs/names, required runtime callbacks, logical key kinds, explicit multi-key/partial-index identities, and supported physical-family/key-kind combinations. Canonicalize tables and indexes by numeric IDs so registration order and opaque callback identity cannot affect the fingerprint.
2. Add generic `TableDefinition[T]` and `IndexDefinition[T]` registration functions. Keep callbacks and serializers as runtime-only behavior while copying every durable descriptor into the canonical manifest; freeze all registration after successful validation.
3. Add `BoundTable[T]` and `BoundIndex[T]` in `catalog_bind.go`. Verify definitions belong to the exact catalog supplied to the opened database, compile them through a private table constructor plus `NewIndex`/`AddIndex`, expose explicit typed logical reads/writes/scans and sealed queries, and reuse the caller-owned Bond batch for cross-table atomicity without exposing runtime `AddIndex`, `ReIndex`, `Query.Table`, or any physical-schema mutation argument. Reserve catalog-owned table IDs from public dynamic `NewTable` construction.
4. Extend `Options` and the single `Open` path with optional pre-open catalog validation. Validate an existing Bond data version before catalog reconciliation, atomically initialize catalog/version metadata only for an empty new database, persist a versioned canonical catalog manifest/fingerprint under a reserved Bond metadata key, verify its own integrity, reject opens without a required catalog, and return deterministic field-level diagnostics for incompatible definitions before application writes.
5. Keep the Phase 4 production registry unchanged: catalog physical-family assignments are descriptive/future-facing, rejected Phase 3 readers are not registered, and the active writer remains Pebble's comparer-derived legacy schema. Preserve versioned Bond databases without catalog metadata, allow an explicit catalog to adopt such a database, reject any pre-existing Pebble store that lacks Bond version metadata, and retain post-open dynamic tables under the global legacy writer.
6. Add focused catalog tests for duplicate/reserved identities and names, freeze behavior, order-independent fingerprints, callback exclusion, persisted definition drift, corrupt metadata, unsupported descriptors/families, catalog ownership, missing-catalog reopen, single-open binding, legacy/dynamic coexistence, typed queries, and atomic Accounts/Sessions/Events writes.
7. Add a compile-tested/runnable Accounts/Sessions/Events example and migration documentation showing stable descriptors, binding, logical queries, one shared batch, legacy coexistence, fingerprint drift behavior, and the explicit Phase 5 no-routing/no-custom-writer constraint. Link it from the README and storage compatibility guide.
8. Run `gofmt`, focused catalog/open/table/query/storage tests, example compilation, `go vet`, and `go build`. Do not run repository-wide backup/restore or race suites deferred to Phase 7.

## Tests

- `TestCatalogRejectsDuplicateAndReservedIDs`: duplicate/reserved table and index identities fail deterministically.
- `TestCatalogRejectsDuplicateNames`: table and per-table index name collisions fail without mutating the catalog.
- `TestCatalogFingerprintStable`: table/index registration order and callback implementations do not change the canonical fingerprint.
- `TestCatalogFingerprintDiff`: codec/layout/key/order/predicate/family drift yields stable field-level diagnostics.
- `TestCatalogRejectsUnknownSchemaAndLayout`: unknown families/key kinds and incompatible typed family assignments fail before open.
- `TestCatalogDefinitionsFreezeAfterValidation`: successful validation makes table and index registration immutable.
- `TestOpenPersistsAndValidatesCatalog`: first open stores the manifest; equivalent reopen succeeds; drift, missing catalog, or corrupt metadata fails actionably.
- `TestCatalogBoundAndLegacyTablesCoexist`: a bound table and a post-open dynamic table use the same DB and legacy production writer.
- `TestBoundTablesAtomicCrossTableBatch`: Accounts, Sessions, and Events plus their indexes remain invisible before one shared batch commit and become queryable together afterward.
- `TestBindRejectsForeignDefinition`: a definition from an equivalent but different catalog cannot bind to the opened DB.
- Bound-handle sealing tests prove `AddIndex`, `ReIndex`, and the runtime `Query.Table` escape are absent.
- Catalog ownership tests reject dynamic `NewTable` overlap, including through an embedded DB wrapper, while validated legacy adoption still succeeds.
- `TestCatalogAuthorizationCannotBeOverriddenByExternalDBWrapper`: an external wrapper cannot lie through the exported `Catalog` method to bypass catalog-owned IDs, while legitimate binding through that wrapper remains authorized by the private DB identity.
- `TestBoundQueryIntersectionsRequireExactBoundTableHandle`: same-type queries may intersect only when they originate from the exact same bound handle; different definitions, repeated binds, and different opens fail deterministically.
- Incompatible or missing Bond version tests preserve existing destination keys and do not persist catalog metadata.
- Empty, damaged, pinned temporary/blob-metadata, and custom-filesystem Pebble marker tests distinguish a genuinely new destination from a pre-existing store without using point-key emptiness.
- Stable persistent OS-first/configured-filesystem claim files serialize first-open preflight through commit or failure across processes without an unlink/recreate inode split. Existing databases place their zero-byte reserved claims inside writable database/metadata directories rather than requiring a writable parent. Default-filesystem destinations are canonicalized through symlinked ancestors before claiming and I/O, while custom VFS path semantics remain untouched.
- Failed opens perform no destructive filesystem rollback. Under the stable claims, a synced, versioned, uniquely identified OS-side initialization marker is published without replacement before `pebble.Open`; only a marker-bearing/versionless store with that valid marker may resume. An internal pending key remains a second-stage marker and is deleted in the same synced batch as successful catalog/version initialization. The external marker is permanent immutable Bond provenance: versioned reopens validate it without entering recovery, and Bond never clears, truncates, overwrites, or unlinks it. Fixed sidecars use exclusively created temporary files plus no-replace hard-link publication, accept only byte-identical retries, and never truncate, overwrite, or unlink an unproven destination. Deterministic cross-process, concurrent-first-open, real-path/symlink-alias, stable-claim, pre-metadata failure/retry, raw-store rejection, permanent-marker reopen/replacement, publication collision/replacement, hook/concurrent Pebble-looking sentinel, and independently replaced sidecar tests prove claim identity and conservative ownership. Failure injection also proves failed legacy-catalog adoption does not persist the catalog before its sidecars succeed.
- `TestCatalogDoesNotActivatePhysicalFamilies`: future physical-family descriptors never change the legacy production writer/reader registry or create a schema argument on mutations.
- Compile `examples/catalog` to keep the final migration API and three-table workflow current.

## Verification

Completed on 2026-08-06:

- `gofmt` over all Phase 5 Go sources and tests.
- Focused immutable-publication tests passed with `go test ./utils -run '^TestWriteFileWithSync' -count=1`.
- Focused catalog/open/query/table/custom-filesystem/failure-injection tests passed with `go test . -run '^(TestCatalog|TestOpen|TestBound|TestBind|TestFirstOpenIntent|TestPermanentInitializationIntent|TestFailedFirstOpen|TestConcurrentFirstOpen|TestInspectOpenDestination|TestStorageCompatibilityAtomic|TestWriteStorageCompatibilityRejects|TestProductionOpenDoesNotRewrite)' -count=1`.
- The final permanent-provenance correction passed directly affected default/custom, versioned-reopen, independent-replacement, and raw-store rejection tests.
- `go vet ./...` passed.
- `go build ./...` passed, including `examples/catalog`.
- `go run ./examples/catalog` passed and printed `found 1 account`.
- `git diff --check` passed.

Repository-wide test, backup/restore, and race suites were intentionally not run; they remain deferred by the requested Phase 5 verification scope.
