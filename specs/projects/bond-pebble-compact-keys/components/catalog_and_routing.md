---
status: complete
---

# Component: Catalog and Stock-Pebble Routing Feasibility

## Purpose and Scope

Define stable storage identities, compile typed handles, persist a storage-definition fingerprint, and assess typed/per-range schemas on unmodified pinned Pebble. The catalog describes writes; it is neither another database nor a destination exposing `Set`.

## Catalog Model

A catalog is mutable only while definitions are registered, then validated/frozen before `Open`. Table IDs are globally unique one-byte storage identities. Index IDs are unique within a table. Durable descriptor strings cover codec, logical layout, key/order fields, partial predicates, and physical family; extractor callbacks remain runtime behavior and are not fingerprinted as code.

Validation canonicalizes definitions by IDs, rejects reserved/duplicate identities and names, verifies schemas and supported typed layouts, and computes a deterministic fingerprint. Existing database metadata stores catalog name/version/fingerprint plus canonical fields needed for an actionable diff.

Breaking the current construction API is allowed. The preferred API defines tables and indexes against a catalog before open, passes the catalog in `Options`, then binds typed handles to the opened database. All handle writes use the existing logical key builder and a caller-owned Bond/Pebble batch.

## Typed Family Rules

Initial reusable candidates are `bond/pk-u64/v1`, `bond/pk-u32/v1`, `bond/pk-bytes/v1`, and a full-key/opaque fallback. A family parser must accept every primary, secondary, short-bound, and range-key shape it can encounter in its assigned scope. It may decompose only fields made unambiguous by the durable descriptor.

Adjacent variable-width inner fields in the current logical builder are not safely parseable; typed decomposition stops before them. Fixing that logical collision requires a separate versioned migration project.

## Stock Pebble Feasibility Protocol

1. Inspect the pinned module APIs and output writer construction; add a compile-time or focused test proving whether range policy can select KeySchema.
2. Benchmark typed families globally or in isolated one-family database copies using stock Pebble.
3. If stock range selection exists, compile sorted table ranges, coalesce adjacent identical complete policies, and test exact flush/compaction boundaries.
4. If it does not exist—as observed at Phase 0—record the no-go with API/code anchors and measured global results. Do not modify the external Pebble checkout, add a local replace, or hide a fork in this project.
5. Any generic `SpanPolicy` schema-name extension becomes a separately approved upstream/fork project.

Per-index routing is not implemented unless per-table routing exists on stock Pebble and measurements show a materially larger benefit that justifies additional SST boundaries.

## Test Plan

- `TestCatalogRejectsDuplicateAndReservedIDs`: all identity collisions fail deterministically.
- `TestCatalogFingerprintStable`: registration/map order does not change the fingerprint.
- `TestCatalogFingerprintDiff`: incompatible fields produce actionable diagnostics.
- `TestCatalogRejectsUnknownSchemaAndLayout`: invalid family/layout combinations fail before open.
- `TestBoundTablesAtomicCrossTableBatch`: three typed tables and indexes commit atomically in one DB.
- Compile-tested example covering accounts, sessions, and events without schema mutation arguments.
- `TestTypedFamilyRoundTrip*`: every routed primary/index layout, short bound, and randomized key round-trips.
- `TestStockPebbleRangeSchemaCapability`: records a positive supported path or a stable no-go against the exact pin.
- If supported, route sort/coalescing and flush/compaction exact-boundary tests plus file-count/read-amplification benchmarks.
