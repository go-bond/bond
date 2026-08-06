# Phase 6 verification

Completed on 2026-08-06 within the requested Phase 6 scope.

## Focused correctness

- `go test . -run '^(TestTypedFamily.*|TestTypedSchemaSelectionIsNotInProductionOptions|TestStockPebbleRangeSchemaCapability|TestCatalogDoesNotActivatePhysicalFamilies)$' -count=1` — pass.
- `cd _benchmarks && go test ./compactkeys -run '^(TestDatasetGeneratorDeterministic|TestDatasetGeneratorCoversShapes|TestLifecycleTypedFamilyCandidates|TestManifestRoundTrip)$' -count=1` — pass.
- Direct tests cover immutable names/columns, typed and opaque point forms, randomized keys, exact/gap/empty/short/truncated seeks, comparer splits, forward/reverse blocks, concurrent seekers, short range-tombstone bounds, flush/compaction/reopen, production registry exclusion, and the stock routing capability no-go.
- Every one of the 30 retained lifecycle manifests passed the independent exact-record and ordered-scan oracle, mutation/snapshot/flush/compaction sequence, checkpoint open, and reopen.

## Measurements

- Retained command: `cd _benchmarks && go run ./compactkeys/cmd/typed-key-schema --seed=20260806 --rows=2000 --warmups=0 --repetitions=3 --output=../specs/projects/bond-pebble-compact-keys/artifacts/phase_6/typed-candidates` — pass, 30 manifests plus CSV.
- Focused allocation command: `cd _benchmarks && go test ./compactkeys -run '^$' -bench '^BenchmarkBondLifecycleTypedFamilies$' -benchmem -benchtime=1x -count=1` — pass.
- Stable byte and timing/allocation interpretation is retained in `analysis.md`; no candidate passed the 5% size gate.

## Formatting, vet, build, and example

- `gofmt -d` over every changed Phase 6 Go source/test — empty output.
- Repository-root `go vet ./...` — pass.
- Nested benchmark-module `go vet ./compactkeys/...` — pass.
- Repository-root `go build ./...` — pass, including `examples/catalog`.
- Nested benchmark-module `go build ./...` — pass, including the retained-result command.
- `go run ./examples/catalog` — pass; printed `found 1 account`.
- `git diff --check` — pass.
- Module/check-out drift guard — pass: no `go.mod`/`go.sum` diff, no external Pebble checkout diff, exact approved Pebble hash.

An exploratory `go vet ./...` from the nested `_benchmarks` module additionally reaches unrelated pre-existing `suites` tests and fails because `TokenBalance` lacks generated `MarshalMsg`; the changed `compactkeys/...` scope vets cleanly and the whole nested module builds cleanly. No unrelated generated benchmark suite code was changed.

Repository-wide tests, race suites, and backup/restore matrices were intentionally not run; they remain Phase 7 work under the explicit operational constraint.
