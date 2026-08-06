# Phase 7 completion audit

No unmet functional, architecture, component, root-plan, or explicit user
requirement remained at the end of Phase 7. Where the root `PLAN.md` conflicts
with the later project override, the approved override controls: Pebble is the
exact reviewed commit, dependencies are upgraded within existing module paths,
breaking catalog additions are permitted, and storage uses `FormatNewest`
rather than `FormatV2BlobFiles`.

| Functional-spec requirement | Completion evidence |
| --- | --- |
| 1. Storage model | One Bond database still owns one Pebble DB/WAL/LSM. Logical keys, ranges, values, table/index mutations, and batches are unchanged and accept no physical schema. Catalog and lifecycle tests pass. |
| 2. Dependency and format baseline | Exact commit/origin/no-replace evidence is in `provenance.md`; all direct modules are current, the one Phase 7 update is retained, format 30 and the older-fixture ratchet/reopen/restore/downgrade rejection pass. Phase 1's API audit covers filter, compression, value storage, iteration, ingestion, cache, span, and open semantics. |
| 3. Benchmark and policy behavior | Phase 2 supplies deterministic datasets, independent lifecycle oracle, complete manifests, corrected compression attribution, filter metrics, repeated sizes, latency/CPU/allocation/compaction/recovery evidence, and explicit 5% stop gates. Phase 7 reproduced the conclusions. |
| 4. Global full-key schema | All three immutable bundles have unit/property/fuzz/corruption/seek/reuse/concurrency coverage and controlled mixed-schema lifecycle evidence. They are rejected for missing the size gate and empty-key invariant, remain internal, and expose no production reader/writer. |
| 5. Registry and operational compatibility | One production option/open path installs format, comparer, span policy, writer, and readers. Checkpoint/backup metadata and pre-mutation restore validation are versioned and backward compatible. Diagnostics and CLI inspection report bounded storage detail. |
| 6. Catalog and public API | The immutable pre-open catalog validates stable descriptors/fingerprints, persists actionable drift, binds typed handles, and emits ordinary logical batches. The compile-tested example has three key shapes, secondary indexes, atomic cross-table writes, and schema-free queries; legacy dynamic construction remains a documented fallback. |
| 7. Typed/per-range feasibility | Phase 6 re-audited stock pinned Pebble, proved safe internal typed families in isolated copies, measured five deterministic datasets, and recorded a no-go: no family clears 5% and stock range policy cannot select a writer schema. No Pebble checkout, fork, or replace was changed. |
| 8. Error and rollout contracts | Invalid catalogs/options, unknown schemas, future formats/epochs, corrupt metadata/physical blocks, and ambiguous definitions fail actionably before unsafe writes. Readers precede writers and rollback retains them. Migration and operational documentation are public. |
| 9. Acceptance criteria | Dependency, format, baseline, full-key decision, centralized operations, catalog/example, typed no-go, full tests, aggregate race suite, focused compatibility, benchmark reproducibility, docs, build, vet, format, tidy, and diff gates pass. |

## Final decisions

Accepted:

- exact stock Pebble pin and format-30 ratchet;
- comparer-derived legacy schema as production's sole writer and reader;
- corrected legacy compression plus uniform Bloom as the frozen policy;
- centralized options/registry/open and compatibility-aware checkpoint,
  backup, restore, diagnostics, and inspection;
- declarative pre-open catalog with stable descriptors and typed bound handles.

Rejected and retained as evidence:

- all `bond/full-key/v1-b{16,32,64}` production activation;
- all `bond/pk-{u64,u32,bytes}/v1` production activation;
- per-table/per-index writer routing on the pinned stock Pebble API;
- a local Pebble replacement/fork or changes to the separate checkout.

The public operational summary is
[`docs/08-compact-key-final-decisions.md`](../../../../../docs/08-compact-key-final-decisions.md).
