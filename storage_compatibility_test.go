package bond

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/go-bond/bond/internal/fullkeyexperiment"
	"github.com/go-bond/bond/utils"
	"github.com/stretchr/testify/require"
)

func TestProductionOptionsAlwaysInstallRegistry(t *testing.T) {
	legacyName := newProductionSchemaRegistry(DefaultKeyComparer()).active
	custom := DefaultPebbleOptions(LowPerformance)
	custom.FormatMajorVersion = pebble.FormatPrePebblev1MarkedCompacted
	custom.MemTableSize = 3 << 20

	for _, requested := range []*pebble.Options{nil, DefaultPebbleOptions(), custom} {
		opts, registry, err := productionPebbleOptions(requested, false)
		require.NoError(t, err)
		require.Equal(t, pebble.FormatNewest, opts.FormatMajorVersion)
		require.Equal(t, DefaultKeyComparer().Name, opts.Comparer.Name)
		require.Equal(t, legacyName, opts.KeySchema)
		require.Equal(t, []string{legacyName}, registry.readerName)
		require.Len(t, opts.KeySchemas, 1)
		require.Contains(t, opts.KeySchemas, legacyName)
		require.NotContains(t, opts.KeySchemas, fullkeyexperiment.NameB16)
		policy, err := opts.SpanPolicyFunc(pebble.UserKeyBounds{})
		require.NoError(t, err)
		require.Equal(t, pebble.ValueStorageLowReadLatency, policy.ValueStoragePolicy)
	}
	require.Equal(t, uint64(3<<20), custom.MemTableSize)
	require.Equal(t, pebble.FormatPrePebblev1MarkedCompacted, custom.FormatMajorVersion)
}

func TestProductionOpenRejectsNonProductionSchemas(t *testing.T) {
	for _, writer := range []string{"", fullkeyexperiment.NameB16} {
		opts, err := experimentalPebbleOptions(writer)
		require.NoError(t, err)
		dir := filepath.Join(t.TempDir(), "must-not-exist")
		_, err = Open(dir, &Options{PebbleOptions: opts})
		if writer == "" {
			require.ErrorContains(t, err, "reserved production key schema")
		} else {
			require.ErrorContains(t, err, "production writer schema")
		}
		_, statErr := os.Stat(dir)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
}

func TestProductionOpenRejectsNoncanonicalLegacySchema(t *testing.T) {
	tests := []struct {
		name        string
		counterfeit func() *colblk.KeySchema
	}{
		{
			name: "replaced callback",
			counterfeit: func() *colblk.KeySchema {
				legacy := colblk.DefaultKeySchema(DefaultKeyComparer(), defaultKeySchemaBundleSize)
				counterfeit := legacy
				counterfeit.NewKeyWriter = func() colblk.KeyWriter {
					return legacy.NewKeyWriter()
				}
				return &counterfeit
			},
		},
		{
			name: "in-place same-code different capture",
			counterfeit: func() *colblk.KeySchema {
				original := colblk.DefaultKeySchema(DefaultKeyComparer(), defaultKeySchemaBundleSize)
				counterfeitComparer := DefaultKeyComparer()
				counterfeitComparer.Split = func([]byte) int { return 0 }
				replacement := colblk.DefaultKeySchema(counterfeitComparer, defaultKeySchemaBundleSize)
				require.Equal(t, original.Name, replacement.Name)
				counterfeit := &original
				*counterfeit = replacement
				return counterfeit
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := BuildPebbleOptions(LowPerformance)
			counterfeit := test.counterfeit()
			require.Equal(t, DefaultKeySchemaName(), counterfeit.Name)
			opts.KeySchemas = sstable.MakeKeySchemas(counterfeit)
			dir := filepath.Join(t.TempDir(), "must-not-exist")

			_, err := Open(dir, &Options{PebbleOptions: opts})
			require.ErrorContains(t, err, "reserved production key schema")
			_, statErr := os.Stat(dir)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestProductionSchemasArePrivateWhileAnotherDBIsOpen(t *testing.T) {
	firstDB, err := Open(filepath.Join(t.TempDir(), "first"), nil)
	require.NoError(t, err)
	defer firstDB.Close()
	secondDB, err := Open(filepath.Join(t.TempDir(), "second"), nil)
	require.NoError(t, err)
	firstSchema := firstDB.(*_db).storage.readers[DefaultKeySchemaName()]
	secondSchema := secondDB.(*_db).storage.readers[DefaultKeySchemaName()]
	require.NotSame(t, firstSchema, secondSchema)
	require.NoError(t, secondDB.Close())

	callerComparer := DefaultKeyComparer()
	callerSchema := colblk.DefaultKeySchema(callerComparer, defaultKeySchemaBundleSize)
	callerOptions := BuildPebbleOptions(LowPerformance)
	callerOptions.KeySchemas = sstable.MakeKeySchemas(&callerSchema)
	_, err = Open(filepath.Join(t.TempDir(), "rejected"), &Options{PebbleOptions: callerOptions})
	require.ErrorContains(t, err, "reserved production key schema")

	replacementComparer := DefaultKeyComparer()
	replacementComparer.Split = func([]byte) int { return 0 }
	replacement := colblk.DefaultKeySchema(replacementComparer, defaultKeySchemaBundleSize)
	callerSchema = replacement
	require.NotSame(t, firstSchema, &callerSchema)
	require.NoError(t, firstDB.Set([]byte("key"), []byte("value"), Sync))
	require.NoError(t, firstDB.Backend().Flush())
	value, closer, err := firstDB.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, "value", string(value))
	require.NoError(t, closer.Close())

	_, fresh, err := productionPebbleOptions(nil, false)
	require.NoError(t, err)
	require.NotSame(t, firstSchema, fresh.readers[fresh.active])
	require.NotSame(t, &callerSchema, fresh.readers[fresh.active])
}

func TestStorageDiagnosticsReportsSchemasAndBytes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, &Options{PebbleOptions: LowPerformancePebbleOptions()})
	require.NoError(t, err)
	for i := range 32 {
		require.NoError(t, db.Set([]byte{byte(i + 1)}, []byte("value"), NoSync))
	}
	require.NoError(t, db.Backend().Flush())

	diagnostics, err := db.StorageDiagnostics()
	require.NoError(t, err)
	require.Equal(t, StorageReaderEpoch, diagnostics.ReaderEpoch)
	require.Equal(t, uint64(pebble.FormatNewest), diagnostics.FormatMajor)
	require.Equal(t, diagnostics.RegisteredReaders[0], diagnostics.ActiveWriter)
	require.Len(t, diagnostics.RegisteredReaders, 1)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 1)
	require.Equal(t, diagnostics.ActiveWriter, diagnostics.EncounteredSSTSchemas[0].Name)
	require.Positive(t, diagnostics.EncounteredSSTSchemas[0].Files)
	require.Positive(t, diagnostics.EncounteredSSTSchemas[0].Bytes)
	require.Empty(t, diagnostics.UnknownSchemas)
	require.NotNil(t, diagnostics.CatalogRoutes)

	compatibility, err := db.StorageCompatibility()
	require.NoError(t, err)
	require.Equal(t, []string{diagnostics.ActiveWriter}, compatibility.RequiredKeySchema)
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, db.Checkpoint(checkpointDir))
	checkpointCompatibility, err := ReadStorageCompatibility(checkpointDir)
	require.NoError(t, err)
	require.Equal(t, compatibility, *checkpointCompatibility)
	checkpointFormat, err := PebbleFormatVersion(checkpointDir)
	require.NoError(t, err)
	require.Equal(t, uint64(PebbleDBFormat), checkpointFormat)
	require.NoError(t, db.Close())

	stored, err := ReadStorageCompatibility(dir)
	require.NoError(t, err)
	require.Equal(t, compatibility, *stored)
	offline, err := InspectStorageDirectory(dir)
	require.NoError(t, err)
	require.Equal(t, diagnostics, offline)
}

func TestStorageCompatibilityValidation(t *testing.T) {
	legacyName := newProductionSchemaRegistry(DefaultKeyComparer()).active
	require.NoError(t, ValidateStorageCompatibility(StorageCompatibility{
		ReaderEpoch:       StorageReaderEpoch,
		FormatMajor:       uint64(pebble.FormatNewest),
		RequiredKeySchema: []string{legacyName},
	}))
	require.NoError(t, ValidateStorageCompatibility(StorageCompatibility{
		FormatMajor:       uint64(pebble.FormatV2BlobFiles),
		RequiredKeySchema: []string{legacyName},
	}))
	require.ErrorContains(t, ValidateStorageCompatibility(StorageCompatibility{
		ReaderEpoch: StorageReaderEpoch + 1,
	}), "newer than supported epoch")
	require.ErrorContains(t, ValidateStorageCompatibility(StorageCompatibility{
		FormatMajor: uint64(pebble.FormatNewest) + 1,
	}), "newer than supported format")
	require.ErrorContains(t, ValidateStorageCompatibility(StorageCompatibility{
		FormatMajor:       uint64(pebble.FormatNewest),
		RequiredKeySchema: []string{fullkeyexperiment.NameB32},
	}), "unregistered key schemas")
	require.ErrorContains(t, ValidateStorageCompatibility(StorageCompatibility{
		FormatMajor:       uint64(pebble.FormatMinSupported) - 1,
		RequiredKeySchema: []string{legacyName},
	}), "older than minimum supported format")
	require.ErrorContains(t, ValidateStorageCompatibility(StorageCompatibility{
		ReaderEpoch: StorageReaderEpoch,
		FormatMajor: uint64(pebble.FormatNewest),
	}), "missing required key schemas")
	require.ErrorContains(t, ValidateStorageCompatibility(StorageCompatibility{
		ReaderEpoch:       StorageReaderEpoch,
		FormatMajor:       uint64(pebble.FormatNewest),
		RequiredKeySchema: []string{legacyName, legacyName},
	}), "strictly sorted and unique")
}

func TestProductionOpenDoesNotRewriteUnchangedStorageCompatibility(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	path := filepath.Join(dir, "bond", StorageCompatibilityFile)
	before, err := os.Stat(path)
	require.NoError(t, err)

	db, err = Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after), "unchanged metadata should retain the same file")
}

func TestStorageCompatibilityAtomicWriteRejectsReplacement(t *testing.T) {
	dir := t.TempDir()
	compatibility := StorageCompatibility{
		ReaderEpoch:       StorageReaderEpoch,
		FormatMajor:       uint64(PebbleDBFormat),
		RequiredKeySchema: []string{DefaultKeySchemaName()},
	}
	require.NoError(t, WriteStorageCompatibility(dir, compatibility))
	path := filepath.Join(dir, "bond", StorageCompatibilityFile)
	original, err := os.ReadFile(path)
	require.NoError(t, err)

	err = writeFileAtomically(path, []byte("replacement"), 0o644, atomicFileWriteHooks{})
	require.ErrorIs(t, err, utils.ErrFileContentConflict)
	current, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, current)
	temporaryFiles, err := filepath.Glob(filepath.Join(dir, "bond", "."+StorageCompatibilityFile+".tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temporaryFiles)
}

func TestStorageCompatibilityRetriesDirectorySyncAfterPublicationFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "bond")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, StorageCompatibilityFile)
	syncCalls := 0
	hooks := atomicFileWriteHooks{
		syncDirectory: func(dir string) error {
			syncCalls++
			if syncCalls == 1 {
				return errors.New("injected directory sync failure")
			}
			return syncAtomicFileDirectory(dir, atomicFileWriteHooks{})
		},
	}

	err := writeFileAtomically(path, []byte("new"), 0o644, hooks)
	require.ErrorContains(t, err, "injected directory sync failure")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "new", string(data), "publication completes before the injected sync failure")
	require.NoError(t, writeFileAtomically(path, []byte("new"), 0o644, hooks))
	require.Equal(t, 2, syncCalls, "unchanged retry must repeat directory sync")
}

func TestWriteStorageCompatibilityRejectsTruncatedFileWithoutReplacingIt(t *testing.T) {
	dir := t.TempDir()
	metadataDir := filepath.Join(dir, "bond")
	require.NoError(t, os.MkdirAll(metadataDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(metadataDir, StorageCompatibilityFile), []byte("{\n"), 0o644,
	))
	compatibility := StorageCompatibility{
		ReaderEpoch:       StorageReaderEpoch,
		FormatMajor:       uint64(PebbleDBFormat),
		RequiredKeySchema: []string{DefaultKeySchemaName()},
	}

	err := WriteStorageCompatibility(dir, compatibility)
	require.ErrorIs(t, err, utils.ErrFileContentConflict)
	stored, readErr := os.ReadFile(filepath.Join(metadataDir, StorageCompatibilityFile))
	require.NoError(t, readErr)
	require.Equal(t, []byte("{\n"), stored)
}

func TestInspectStorageDirectorySupportsOlderFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	format := pebble.FormatMinSupported
	opts := BuildPebbleOptions(LowPerformance)
	opts.FormatMajorVersion = format
	db, err := pebble.Open(dir, opts)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "bond"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "bond", PebbleFormatFile), []byte(fmt.Sprintf("%d", format)), 0o644,
	))
	require.NoError(t, WriteStorageCompatibility(dir, StorageCompatibility{
		ReaderEpoch:       StorageReaderEpoch,
		FormatMajor:       uint64(format),
		RequiredKeySchema: []string{DefaultKeySchemaName()},
	}))

	diagnostics, err := InspectStorageDirectory(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(format), diagnostics.FormatMajor)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 1)
	require.Equal(t, DefaultKeySchemaName(), diagnostics.EncounteredSSTSchemas[0].Name)
}

func TestInspectStorageDirectoryReportsUnknownSchemaNameContainingDiagnosticDelimiter(t *testing.T) {
	const unusualName = `bond/unusual; known key schemas: "quoted"\\v1`
	dir := filepath.Join(t.TempDir(), "db")
	opts := BuildPebbleOptions(LowPerformance)
	legacy := colblk.DefaultKeySchema(opts.Comparer, defaultKeySchemaBundleSize)
	unusual, err := fullkeyexperiment.New(opts.Comparer, 32)
	require.NoError(t, err)
	unusual.Name = unusualName
	opts.KeySchema = unusualName
	opts.KeySchemas = sstable.MakeKeySchemas(&legacy, &unusual)
	db, err := pebble.Open(dir, opts)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "bond"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "bond", PebbleFormatFile),
		[]byte(fmt.Sprintf("%d", PebbleDBFormat)),
		0o644,
	))

	diagnostics, err := InspectStorageDirectory(dir)
	require.NoError(t, err)
	require.Equal(t, []string{unusualName}, diagnostics.UnknownSchemas)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 1)
	require.Equal(t, unusualName, diagnostics.EncounteredSSTSchemas[0].Name)
}

func TestProductionPebbleOpenCallsAreCentralized(t *testing.T) {
	var openCalls []string
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != "." && (strings.HasPrefix(path, "_benchmarks") || strings.HasPrefix(path, "docs") || strings.HasPrefix(path, "specs") || strings.HasPrefix(path, ".git")) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fileset := token.NewFileSet()
		file, err := parser.ParseFile(fileset, path, nil, 0)
		if err != nil {
			return err
		}
		importsPebble := false
		for _, spec := range file.Imports {
			if spec.Path.Value == `"github.com/cockroachdb/pebble"` && (spec.Name == nil || spec.Name.Name == "pebble") {
				importsPebble = true
				break
			}
		}
		if !importsPebble {
			return nil
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Open" {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if ok && identifier.Name == "pebble" {
					openCalls = append(openCalls, path+":"+function.Name.Name)
				}
				return true
			})
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"bond.go:openPreparedPebble"}, openCalls)
}
