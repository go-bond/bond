package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-bond/bond"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

//go:embed testdata/pre-upgrade-v0.2.17.tar.gz
var preUpgradeFixture []byte

func TestFormatNewestMigration(t *testing.T) {
	require.Equal(t, pebble.FormatMajorVersion(30), pebble.FormatNewest)
	require.Equal(t, pebble.FormatNewest, bond.PebbleDBFormat)

	ctx := context.Background()
	testRoot := t.TempDir()
	dbDir := filepath.Join(testRoot, "pre-upgrade-db")
	restoreDir := filepath.Join(testRoot, "restored-db")
	extractPreUpgradeFixture(t, dbDir)

	oldFormatVersion, err := os.ReadFile(filepath.Join(dbDir, "bond", bond.PebbleFormatFile))
	require.NoError(t, err)
	require.Equal(t, "26", string(oldFormatVersion))

	preUpgradeHelper := buildPreUpgradeHelper(t)
	output, err := runPreUpgradeHelper(preUpgradeHelper, "open", dbDir)
	require.NoErrorf(t, err, "pre-upgrade binary failed to open its fixture: %s", output)

	require.NoError(t, bond.MigratePebbleFormatVersion(dbDir, uint64(pebble.FormatNewest)))
	formatVersion, err := bond.PebbleFormatVersion(dbDir)
	require.NoError(t, err)
	require.Equal(t, uint64(30), formatVersion)
	require.ErrorContains(t,
		bond.MigratePebbleFormatVersion(dbDir, uint64(pebble.FormatV2BlobFiles)),
		"cannot downgrade pebble format",
	)

	olderOptions := bond.DefaultOptions(bond.MediumPerformance)
	olderOptions.PebbleOptions.FormatMajorVersion = pebble.FormatV2BlobFiles
	_, err = bond.Open(dbDir, olderOptions)
	require.ErrorContains(t, err, "the user trying to open pebble version")

	db := openTestDB(t, dbDir)
	require.Equal(t, pebble.FormatNewest, db.Backend().FormatMajorVersion())
	requireFixtureValue(t, db)
	expected := collectAllKVs(t, db)

	bucket := objstore.NewInMemBucket()
	meta, err := Backup(ctx, db, bucket, BackupOptions{
		Prefix:        "format-migration",
		Type:          BackupTypeComplete,
		CheckpointDir: filepath.Join(testRoot, "checkpoint"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(30), meta.PebbleFormatVersion)
	require.NoError(t, db.Close())

	require.NoError(t, Restore(ctx, bucket, RestoreOptions{
		Prefix:     "format-migration",
		RestoreDir: restoreDir,
	}))
	restored := openTestDB(t, restoreDir)
	require.Equal(t, pebble.FormatNewest, restored.Backend().FormatMajorVersion())
	requireFixtureValue(t, restored)
	require.Equal(t, expected, collectAllKVs(t, restored))
	require.NoError(t, restored.Close())

	// Bypass Bond v0.2.17's sidecar check so its old Pebble engine must inspect
	// and reject the physical format-30 marker itself.
	require.NoError(t, os.WriteFile(
		filepath.Join(restoreDir, "bond", bond.PebbleFormatFile),
		oldFormatVersion,
		0o644,
	))
	output, err = runPreUpgradeHelper(preUpgradeHelper, "open", restoreDir)
	require.Error(t, err)
	require.NotContains(t, output, "the user trying to open pebble version")
	require.Contains(t, output, "format major version 30")
}

func requireFixtureValue(t *testing.T, db bond.DB) {
	t.Helper()
	key := bond.KeyEncode(bond.Key{
		TableID:    0xc0,
		IndexID:    bond.PrimaryIndexID,
		Index:      []byte{},
		IndexOrder: []byte{},
		PrimaryKey: []byte("pre-upgrade-key"),
	})
	value, closer, err := db.Get(key)
	require.NoError(t, err)
	defer closer.Close()
	require.Equal(t, "pre-upgrade-value", string(value))
}

func buildPreUpgradeHelper(t *testing.T) string {
	t.Helper()
	_, testFilename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	helperDir := filepath.Join(filepath.Dir(testFilename), "..", "internal", "testfixtures", "preupgrade")
	helperBinary := filepath.Join(t.TempDir(), "preupgrade-helper")
	cmd := exec.Command("go", "build", "-o", helperBinary, ".")
	cmd.Dir = helperDir
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "build pre-upgrade helper: %s", output)
	return helperBinary
}

func runPreUpgradeHelper(helperBinary, command, dbDir string) (string, error) {
	output, err := exec.Command(helperBinary, command, dbDir).CombinedOutput()
	return string(output), err
}

func extractPreUpgradeFixture(t *testing.T, destination string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(destination, 0o755))
	gzipReader, err := gzip.NewReader(bytes.NewReader(preUpgradeFixture))
	require.NoError(t, err)
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		name := filepath.Clean(header.Name)
		target := filepath.Join(destination, name)
		require.True(t,
			target == destination || strings.HasPrefix(target, destination+string(os.PathSeparator)),
			"fixture path escapes destination: %q", header.Name,
		)
		switch header.Typeflag {
		case tar.TypeDir:
			require.NoError(t, os.MkdirAll(target, os.FileMode(header.Mode)))
		case tar.TypeReg:
			require.NoError(t, os.MkdirAll(filepath.Dir(target), 0o755))
			file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			require.NoError(t, err)
			_, copyErr := io.Copy(file, tarReader)
			closeErr := file.Close()
			require.NoError(t, copyErr)
			require.NoError(t, closeErr)
		default:
			require.FailNow(t, fmt.Sprintf("unsupported fixture entry %q", header.Name))
		}
	}
}
