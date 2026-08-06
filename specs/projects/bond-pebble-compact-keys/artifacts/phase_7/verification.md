# Phase 7 verification

Final validation was performed on 2026-08-06. Repository tests were serialized
because several existing suites use fixed database paths.

## Build and test gates

| Gate | Final result |
| --- | --- |
| exact `make build` | pass after final source fixes |
| `go test -p 1 ./... -count=1` | pass; root 62.196s, backup 364.682s, bloom 5.925s, integration tests 20.947s |
| aggregate `go test -p 1 -race ./... -count=1` | pass; every non-root package passed in the full serialized run, including backup 366.890s, bloom 17.573s, and integration tests 22.437s; root passed its affected rerun in 65.150s |
| focused repaired root race tests | pass in 2.048s |
| focused version/open/restore/rollback matrix | pass across root, backup, integration tests, and CLI inspection |
| `go run ./examples/catalog` | pass; printed `found 1 account` |
| `go vet ./...` | pass |
| `cd _benchmarks && go vet ./compactkeys/...` | pass |
| `cd _benchmarks && go build ./...` | pass |
| `go mod tidy -go=1.25.3 -diff` | pass; no diff |
| `gofmt -d` over actual `*.go` sources | pass; no diff |
| `git diff --check` | pass |

The focused compatibility matrix covered Bond version/open behavior, production
option and diagnostics validation, the old-format inspection path, centralized
open enforcement, format migration, complete and incremental backup/restore,
checkpoint metadata, old backup metadata, restore rejection before destination
mutation, restore-to-incremental continuation, point-in-time restore, catalog
data/index/batch restore, and controlled mixed-schema reopen/compaction/writer
rollback. The three CLI inspection cases passed in 0.511s; selected root cases
passed in 9.918s; selected backup cases passed in 9.996s; and non-duplicated
integration restore cases passed in 9.589s.

## Failures found and corrected

The first full suite exposed a cross-phase publication regression. Phase 5 had
made `utils.WriteFileWithSync` an immutable no-replace publisher, while mutable
backup metadata, format sidecars, Bloom-filter files, and storage compatibility
metadata still called it. This broke version migration and multiple complete,
incremental, and point-in-time restore paths.

Phase 7 added `utils.ReplaceFileWithSync`: it writes and syncs a unique
same-directory temporary file, preserves the old target until atomic rename,
syncs the directory, avoids rewriting byte-identical content, and permits a
directory-sync retry. Mutable callers now use it. Storage compatibility retains
its equivalent injectable atomic-replacement implementation so failure-ordering
tests can prove pre-rename preservation and post-rename sync retry. Initialization
intent files continue using the immutable no-replace API. New tests cover mutable
replacement and target preservation; the filter filesystem test now proves a
second write replaces content.

The first race run also found two test-fixture defects rather than production
races: a custom VFS assertion retained a caller-owned byte slice that the VFS
was allowed to mutate, and a subprocess test read `bytes.Buffer`/process state
while `os/exec` was still writing. The tests now compare a fresh marker and do
not inspect those values concurrently. Both focused race cases and the complete
root race package pass. Already-green expensive packages were not rerun after
this root-only repair.

## Production path audit

There is exactly one non-test production `pebble.Open`, in
`openPreparedPebble` (`bond.go`). Normal `Open` and
`MigratePebbleFormatVersion` both obtain validated options from
`productionPebbleOptions` before calling it. That preparation fixes the Bond
comparer, format 30, stock span policy, legacy writer, and legacy-only reader
registry and rejects caller attempts to install experimental schemas.

Bond's public `Checkpoint` wraps Pebble checkpoint creation and adds the format
and storage-compatibility sidecars. Backup calls the Bond wrapper. Restore
validates the whole metadata chain before destination mutation. Offline
inspection uses production read policy but reads SST property blocks without
opening the database. The raw checkpoint calls in compact-key benchmarks are
intentionally isolated lifecycle experiments. The AST guard
`TestProductionPebbleOpenCallsAreCentralized` passed.

## Documentation audit

The root README and docs index now point to the multi-table catalog example,
benchmark methodology, project architecture, compatibility rules, and a final
decision record. The final record states the one-Bond-DB/one-Pebble-DB model,
schema-free mutation API, exact pin and format ratchet, backup/restore and
reader-retention rules, accepted catalog/baseline, rejected full-key and typed
families, and stock routing no-go. Broken architecture/research/example links
and the obsolete claim that changing compatibility sidecars were immutable
publications were corrected.
