package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
	"github.com/go-bond/bond/internal/fullkeyexperiment"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestStorageInspectCommand(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := bond.Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), bond.Sync))
	require.NoError(t, db.Backend().Flush())
	require.NoError(t, db.Close())

	var output bytes.Buffer
	app := cli.NewApp()
	app.Writer = &output
	app.Commands = []*cli.Command{StorageCommand}
	require.NoError(t, app.Run([]string{"bond-cli", "storage", "inspect", "--dir", dir}))

	var diagnostics bond.StorageDiagnostics
	require.NoError(t, json.Unmarshal(output.Bytes(), &diagnostics))
	require.NotEmpty(t, diagnostics.ActiveWriter)
	require.Equal(t, []string{diagnostics.ActiveWriter}, diagnostics.RegisteredReaders)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 1)
	require.Positive(t, diagnostics.EncounteredSSTSchemas[0].Bytes)
	require.NotNil(t, diagnostics.CatalogRoutes)
}

func TestStorageInspectCommandReportsUnknownSchema(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	opts := bond.BuildPebbleOptions(bond.LowPerformance)
	require.NoError(t, fullkeyexperiment.Configure(opts, fullkeyexperiment.NameB32))
	db, err := pebble.Open(dir, opts)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "bond"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "bond", bond.PebbleFormatFile),
		[]byte(fmt.Sprintf("%d", bond.PebbleDBFormat)),
		0o644,
	))
	require.NoError(t, bond.WriteStorageCompatibility(dir, bond.StorageCompatibility{
		ReaderEpoch:       bond.StorageReaderEpoch,
		FormatMajor:       uint64(bond.PebbleDBFormat),
		RequiredKeySchema: []string{fullkeyexperiment.NameB32},
	}))

	var output bytes.Buffer
	app := cli.NewApp()
	app.Writer = &output
	app.Commands = []*cli.Command{StorageCommand}
	require.NoError(t, app.Run([]string{"bond-cli", "storage", "inspect", "--dir", dir}))

	var diagnostics bond.StorageDiagnostics
	require.NoError(t, json.Unmarshal(output.Bytes(), &diagnostics))
	require.Equal(t, []string{fullkeyexperiment.NameB32}, diagnostics.UnknownSchemas)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 1)
	require.Equal(t, fullkeyexperiment.NameB32, diagnostics.EncounteredSSTSchemas[0].Name)
	require.Positive(t, diagnostics.EncounteredSSTSchemas[0].Bytes)
}

func TestStorageInspectCommandSupportsOlderFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	format := pebble.FormatMinSupported
	opts := bond.BuildPebbleOptions(bond.LowPerformance)
	opts.FormatMajorVersion = format
	db, err := pebble.Open(dir, opts)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "bond"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "bond", bond.PebbleFormatFile),
		[]byte(fmt.Sprintf("%d", format)),
		0o644,
	))

	var output bytes.Buffer
	app := cli.NewApp()
	app.Writer = &output
	app.Commands = []*cli.Command{StorageCommand}
	require.NoError(t, app.Run([]string{"bond-cli", "storage", "inspect", "--dir", dir}))

	var diagnostics bond.StorageDiagnostics
	require.NoError(t, json.Unmarshal(output.Bytes(), &diagnostics))
	require.Equal(t, uint64(format), diagnostics.FormatMajor)
	require.Len(t, diagnostics.EncounteredSSTSchemas, 1)
}
