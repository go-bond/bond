# Project Documentation

## Table of Contents

- [Backup brief](./01-backup-brief.md)
- [Backup plan](./02-backup-plan.md)
- [Restore optimization](./03-backup-restore-optimization.md)
- [Backup chain integrity](./04-backup-chain-integrity.md)
- [Compact-key project architecture](../specs/projects/bond-pebble-compact-keys/architecture.md)
- [Storage compatibility and inspection](./05-storage-compatibility.md)
- [Declarative catalog and API migration](./06-declarative-catalog.md)
- [Typed KeySchema feasibility on stock Pebble](./07-typed-schema-feasibility.md)
- [Compact-key final decisions](./08-compact-key-final-decisions.md)
- [Compact-key benchmark methodology](../_benchmarks/compactkeys/README.md)
- [Research](./_research/README.md)

Bond opens one Pebble database per Bond database. Logical table and index APIs
build ordinary byte keys; callers never choose an SST schema on `Set`, `Delete`,
or batch operations. The compact-key documents explain the physical-storage
experiments, compatibility rules, and the retained production configuration.
