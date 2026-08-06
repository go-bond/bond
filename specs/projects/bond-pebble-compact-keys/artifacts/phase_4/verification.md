# Phase 4 verification

Verified on 2026-08-06 with Go 1.26.5 on linux/amd64. Repository tests that touch fixed paths were run serially.

## Revised phase gate

During implementation, the user explicitly moved the expensive repository-wide backup/restore and full race matrices to Phase 7. Phase 4 therefore gates on formatting/diff checks, repository vet/build, focused unit and integration tests for storage options, schema compatibility, checkpoints, backup metadata, restore validation, inspection CLI behavior, and the controlled mixed-schema rollback. Broad results that had already completed are retained below, but no broad command was restarted after the instruction changed.

## Checks and focused tests

| Command or scope | Result |
| --- | --- |
| `gofmt -d` on every changed Go source | pass; no diff |
| `git diff --check` | pass |
| `go vet ./...` | pass |
| `go build ./...` | pass |
| focused Bond options/registry/diagnostics/checkpoint/open-path/version/migration/legacy-lifecycle/mixed-schema tests | pass; 0.548s |
| focused backup complete/incremental/previous-chain-compatibility/restore-chain/metadata/early-rejection/old-metadata/incomplete-marker/format migration tests | pass; 28.130s |
| focused `bond-cli storage inspect` test | pass; 0.492s |
| post-review focused registry/metadata/diagnostics/open-path tests | pass; 0.061s |
| post-review `bond-cli` known/unknown-schema inspection tests | pass; 0.292s |
| post-review restore compatibility/pre-mutation and legacy fallback tests | pass; 3.686s |
| post-review complete/incremental/interrupted/local-meta/format restore matrix | pass; 20.007s |
| second-review focused reserved-name/atomic-sidecar/supported-format root tests | pass; included in 0.652s focused root run |
| second-review known/unknown/older-format CLI inspection tests | pass; 0.365s |
| second-review focused backup/restore compatibility matrix | pass |
| second-review compact-key manifest/lifecycle tests | pass; 1.905s |
| third-review focused fresh-schema/sync-retry/escape-aware/root tests | pass; included in 0.981s focused root run |
| third-review known/unknown/older-format CLI inspection tests | pass; 0.309s |
| third-review focused backup/restore compatibility matrix | pass |
| third-review compact-key manifest/lifecycle tests | pass; 0.700s |
| fourth-review exact legacy baseline and explicit schema-candidate lifecycle tests | pass; 0.829s |
| final-review benchmark source-fingerprint dependency/lifecycle coverage | pass; 0.641s |
| `cd _benchmarks && go vet ./compactkeys/...` | pass |
| `cd _benchmarks && go build ./...` | pass |
| focused compact-key manifest/checkpoint compatibility/lifecycle tests | pass; 0.633s |

Focused commands covered these named Phase 4 behaviors directly:

- production options always install `FormatNewest`, the Bond comparer, the stock span policy, the legacy/default writer, and exactly one legacy/default reader;
- production `bond.Open` rejects internal full-key readers/writers and a noncanonical implementation under the legacy durable name before creating the requested database directory;
- every caller-prepopulated reserved legacy name is rejected; production preparation installs a fresh private schema per options/open, and focused tests cover in-place same-code/different-capture mutation plus isolation while another database remains live;
- compatibility sidecars preserve the previous target across a simulated pre-rename crash boundary, atomically replace truncated content, and are not rewritten when an ordinary reopen produces identical metadata;
- an injected post-rename directory-sync failure is returned, and a byte-identical retry repeats and completes directory sync rather than masking the failure;
- an AST audit permits the production source tree's only direct `pebble.Open` call in `openPreparedPebble`; normal open and migration feed that call through the production option preparer;
- offline inspection reads SST properties without opening the database or initializing unavailable key seekers; a controlled CLI test reports an unknown `bond/full-key/v1-b32` SST by durable name;
- storage and CLI inspection accept and report `pebble.FormatMinSupported` databases before migration;
- an end-to-end SST test preserves an unknown durable schema name containing the diagnostic delimiter, escaped quotes, and backslashes;
- live and offline diagnostics agree on active/registered/encountered schema names, SST files/bytes, unknown names, and the empty pre-catalog route list;
- Bond checkpoints write both `PEBBLE_FORMAT_VERSION` and `STORAGE_COMPATIBILITY.json`;
- backup metadata and checkpoint file lists carry reader epoch, format major, and required schema names;
- incremental backup creation rejects an unreadable previous backup before creating a checkpoint;
- future epochs/formats and unregistered schemas fail actionably;
- restore rejects missing readers, empty current schema requirements, formats below `pebble.FormatMinSupported`, and contradictory format fields before cleaning an interrupted destination, creating markers, or downloading files;
- metadata created before the optional compatibility field remains restorable under the conservative legacy/default rule;
- complete and incremental restore chains plus interrupted-restore cleanup still pass after validation was reordered;
- controlled raw-Pebble mixed legacy/full-key SSTs reopen, compact, checkpoint, and roll the writer back to legacy while retaining internal readers; the same database is diagnosed as requiring an unknown schema from the production registry's perspective;
- non-schema benchmark baselines retain exactly one legacy writer/reader, while explicit schema candidates keep their controlled reader set; manifest v3 checkpoint requirements contain encountered SST schemas rather than every available experimental reader.

## Already-completed broad evidence

Before the phase gate was narrowed, `go test -p 1 ./... -count=1` completed successfully: root 37.615s, backup 349.057s, and all remaining packages passed. This result is retained but is not required by the revised Phase 4 gate.

A subsequent `go test -p 1 -race ./... -count=1` began and the root package passed in 39.781s. The command was intentionally stopped while the backup package was running after the user deferred the broad race matrix to Phase 7. It is not represented as a full race pass. A previously completed focused compact-key race run passed in 3.234s, but race is likewise not a Phase 4 gate.

The known pre-existing `cd _benchmarks && go vet ./...` generated-suite failure remains unchanged (`suites/table_common_test.go` references an absent generated `MarshalMsg`). As in Phases 2 and 3, the applicable new harness tree `./compactkeys/...` passes vet, and the complete benchmark module passes build.

## Phase 7 deferred matrix

Phase 7 retains the repository-wide serial backup/restore run, full repository race run, final format/version/restore matrix, and reproducibility audit as final-verification work. Phase 4 did not restart those expensive commands after the sequencing change.
