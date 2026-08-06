package bond

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

const (
	openClaimHelperEnv     = "BOND_OPEN_CLAIM_HELPER"
	openClaimHelperDirEnv  = "BOND_OPEN_CLAIM_HELPER_DIR"
	openClaimHelperReady   = "BOND_OPEN_CLAIM_HELPER_READY"
	openClaimHelperRelease = "BOND_OPEN_CLAIM_HELPER_RELEASE"
)

type recordingListFS struct {
	vfs.FS
	allowed string
	listed  []string
}

func (fs *recordingListFS) List(dirname string) ([]string, error) {
	fs.listed = append(fs.listed, dirname)
	if dirname != fs.allowed {
		return nil, errors.New("unexpected nested directory listing")
	}
	return fs.FS.List(dirname)
}

func minimalOpenTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	catalog := NewCatalog("open-transaction", "v1")
	_, err := DefineTable(catalog, catalogTestTableSchema(1, "records"))
	require.NoError(t, err)
	return catalog
}

func requireCatalogMetadataAbsent(t *testing.T, db *pebble.DB) {
	t.Helper()
	_, closer, err := db.Get(catalogDefinitionKey())
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func writeVFSFile(t *testing.T, fs vfs.FS, path string, data []byte) {
	t.Helper()
	file, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = file.Write(data)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func readVFSFile(t *testing.T, fs vfs.FS, path string) []byte {
	t.Helper()
	file, err := fs.Open(path)
	require.NoError(t, err)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	return data
}

func initializationIntentPaths(t *testing.T, dirname string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(
		dirname,
		"bond",
		bondInitializationIntentPrefix+"*",
	))
	require.NoError(t, err)
	return paths
}

func TestOpenRejectsPreexistingEmptyPebbleStoreWithoutBondVersion(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "empty-pebble")
	options, _, err := productionPebbleOptions(DefaultPebbleOptions(), false)
	require.NoError(t, err)
	rawDB, err := openPreparedPebble(dir, options)
	require.NoError(t, err)
	require.NoError(t, rawDB.Close())

	_, err = Open(dir, &Options{Catalog: minimalOpenTestCatalog(t)})
	require.ErrorContains(t, err, "missing from a pre-existing Pebble store")
	require.FileExists(t, filepath.Join(dir, "bond", bondOpenOSClaimName))
	require.Empty(t, initializationIntentPaths(t, dir))

	options, _, err = productionPebbleOptions(DefaultPebbleOptions(), false)
	require.NoError(t, err)
	rawDB, err = openPreparedPebble(dir, options)
	require.NoError(t, err)
	defer rawDB.Close()
	requireCatalogMetadataAbsent(t, rawDB)
}

func TestOpenPreservesDamagedPreexistingPebbleMarkers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "damaged-pebble")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	currentPath := filepath.Join(dir, "CURRENT")
	currentContents := []byte("not-a-valid-manifest\n")
	require.NoError(t, os.WriteFile(currentPath, currentContents, 0o644))

	_, err := Open(dir, &Options{Catalog: minimalOpenTestCatalog(t)})
	require.Error(t, err)
	stored, readErr := os.ReadFile(currentPath)
	require.NoError(t, readErr)
	require.Equal(t, currentContents, stored)
	require.FileExists(t, filepath.Join(dir, "bond", bondOpenOSClaimName))
}

func TestOpenRecognizesPinnedTemporaryAndBlobMetadataMarkers(t *testing.T) {
	markers := []string{
		"CURRENT.000123.dbtmp",
		"temporary.000123.dbtmp",
		"000123.blobmeta.4096",
	}
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	for _, filesystem := range filesystems {
		for _, marker := range markers {
			t.Run(filesystem.name+"/"+marker, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "damaged-pebble")
				configuredFS := vfs.FS(vfs.Default)
				requested := DefaultPebbleOptions()
				if filesystem.custom {
					configuredFS = vfs.NewMem()
					requested.FS = configuredFS
				}
				require.NoError(t, configuredFS.MkdirAll(dir, 0o755))
				markerContents := []byte("damaged-state-marker")
				writeVFSFile(t, configuredFS, configuredFS.PathJoin(dir, marker), markerContents)

				_, err := Open(dir, &Options{Catalog: minimalOpenTestCatalog(t), PebbleOptions: requested})
				require.ErrorContains(t, err, "pre-existing Pebble artifacts are present without a current manifest pointer")
				require.Equal(t, markerContents, readVFSFile(t, configuredFS, configuredFS.PathJoin(dir, marker)))
				entries, err := configuredFS.List(dir)
				require.NoError(t, err)
				require.Contains(t, entries, marker)
				require.Contains(t, entries, bondOpenPebbleClaimName)
				require.FileExists(t, filepath.Join(dir, "bond", bondOpenOSClaimName))
			})
		}
	}
}

func TestInspectOpenDestinationListsOnlyPebbleTopLevel(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "top-level-only")
	memoryFS := vfs.NewMem()
	require.NoError(t, memoryFS.MkdirAll(memoryFS.PathJoin(dir, "unrelated", "nested"), 0o755))
	recordingFS := &recordingListFS{FS: memoryFS, allowed: dir}

	transaction, err := inspectOpenDestination(recordingFS, dir)
	require.NoError(t, err)
	require.Equal(t, []string{dir}, recordingFS.listed)
	require.NoError(t, transaction.claim.Close())
}

func TestOpenTreatsPrecreatedEmptyDirectoryAsNewStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "empty-directory")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	db, err := Open(dir, &Options{Catalog: minimalOpenTestCatalog(t)})
	require.NoError(t, err)
	version, exists, err := readBondDataVersion(db.Backend())
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, BOND_DB_DATA_VERSION, version)
	stored, err := catalogDefinitionBytes(db.Backend())
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	require.NoError(t, db.Close())
}

func TestOpenDetectsPreexistingPebbleStoreOnCustomFilesystem(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "custom-fs")
	memoryFS := vfs.NewMem()
	requested := DefaultPebbleOptions()
	requested.FS = memoryFS
	options, _, err := productionPebbleOptions(requested, false)
	require.NoError(t, err)
	rawDB, err := openPreparedPebble(dir, options)
	require.NoError(t, err)
	require.NoError(t, rawDB.Close())

	_, err = Open(dir, &Options{Catalog: minimalOpenTestCatalog(t), PebbleOptions: requested})
	require.ErrorContains(t, err, "missing from a pre-existing Pebble store")
	require.FileExists(t, filepath.Join(dir, "bond", bondOpenOSClaimName))
	require.Empty(t, initializationIntentPaths(t, dir))

	options, _, err = productionPebbleOptions(requested, false)
	require.NoError(t, err)
	rawDB, err = openPreparedPebble(dir, options)
	require.NoError(t, err)
	defer rawDB.Close()
	requireCatalogMetadataAbsent(t, rawDB)
}

func TestFirstOpenIntentSurvivesPreMetadataFailuresAndPermitsRetry(t *testing.T) {
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	stages := []struct {
		name    string
		install func(string, error)
	}{
		{
			name: "immediately-after-pebble-creation",
			install: func(dirname string, injected error) {
				afterOpenPreparedPebble = func(*pebble.DB) error {
					intent, err := inspectBondInitializationIntent(dirname)
					if err != nil {
						return err
					}
					if intent == nil {
						return errors.New("external initialization intent was not published before Pebble opened")
					}
					return injected
				}
			},
		},
		{
			name: "pending-read",
			install: func(_ string, injected error) {
				readOpenBondInitializationPending = func(*pebble.DB) (bool, error) {
					return false, injected
				}
			},
		},
		{
			name: "pending-write",
			install: func(_ string, injected error) {
				markOpenBondInitializationPending = func(*pebble.DB) error { return injected }
			},
		},
	}
	previousAfterOpen := afterOpenPreparedPebble
	previousReadPending := readOpenBondInitializationPending
	previousMarkPending := markOpenBondInitializationPending
	t.Cleanup(func() {
		afterOpenPreparedPebble = previousAfterOpen
		readOpenBondInitializationPending = previousReadPending
		markOpenBondInitializationPending = previousMarkPending
	})

	for _, filesystem := range filesystems {
		for _, stage := range stages {
			t.Run(filesystem.name+"/"+stage.name, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "pre-metadata-failure")
				requested := DefaultPebbleOptions()
				if filesystem.custom {
					requested.FS = vfs.NewMem()
				}
				catalog := minimalOpenTestCatalog(t)
				afterOpenPreparedPebble = previousAfterOpen
				readOpenBondInitializationPending = previousReadPending
				markOpenBondInitializationPending = previousMarkPending
				injected := errors.New("injected " + stage.name + " failure")
				stage.install(dir, injected)

				_, err := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
				require.ErrorIs(t, err, injected)
				require.Len(t, initializationIntentPaths(t, dir), 1)
				intent, inspectErr := inspectBondInitializationIntent(dir)
				require.NoError(t, inspectErr)
				require.NotNil(t, intent)

				afterOpenPreparedPebble = previousAfterOpen
				readOpenBondInitializationPending = previousReadPending
				markOpenBondInitializationPending = previousMarkPending
				prepared, _, prepareErr := productionPebbleOptions(requested, false)
				require.NoError(t, prepareErr)
				rawDB, openErr := openPreparedPebble(dir, prepared)
				require.NoError(t, openErr)
				pending, pendingErr := readBondInitializationPending(rawDB)
				require.NoError(t, pendingErr)
				require.False(t, pending)
				_, hasVersion, versionErr := readBondDataVersion(rawDB)
				require.NoError(t, versionErr)
				require.False(t, hasVersion)
				require.NoError(t, rawDB.Close())

				db, retryErr := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
				require.NoError(t, retryErr)
				require.Len(t, initializationIntentPaths(t, dir), 1)
				require.NoError(t, db.Close())
			})
		}
	}
}

func TestFirstOpenIntentValidatesStrictlyOnRetry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "invalid-intent-retry")
	previousAfterOpen := afterOpenPreparedPebble
	afterOpenPreparedPebble = func(*pebble.DB) error { return errors.New("injected post-open failure") }
	t.Cleanup(func() { afterOpenPreparedPebble = previousAfterOpen })

	_, err := Open(dir, nil)
	require.ErrorContains(t, err, "injected post-open failure")
	paths := initializationIntentPaths(t, dir)
	require.Len(t, paths, 1)
	corrupt := []byte("not-a-valid-bond-intent\n")
	require.NoError(t, os.WriteFile(paths[0], corrupt, 0o600))
	afterOpenPreparedPebble = previousAfterOpen

	_, err = Open(dir, nil)
	require.ErrorContains(t, err, "decode external initialization intent")
	stored, readErr := os.ReadFile(paths[0])
	require.NoError(t, readErr)
	require.Equal(t, corrupt, stored)
}

func TestFirstOpenIntentPersistsAfterMetadataAndReopen(t *testing.T) {
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	previousReadPending := readOpenBondInitializationPending
	previousMarkPending := markOpenBondInitializationPending
	t.Cleanup(func() {
		readOpenBondInitializationPending = previousReadPending
		markOpenBondInitializationPending = previousMarkPending
	})
	for _, filesystem := range filesystems {
		t.Run(filesystem.name, func(t *testing.T) {
			readOpenBondInitializationPending = previousReadPending
			markOpenBondInitializationPending = previousMarkPending
			defer func() {
				readOpenBondInitializationPending = previousReadPending
				markOpenBondInitializationPending = previousMarkPending
			}()
			dir := filepath.Join(t.TempDir(), "permanent-intent")
			requested := DefaultPebbleOptions()
			if filesystem.custom {
				requested.FS = vfs.NewMem()
			}
			catalog := minimalOpenTestCatalog(t)
			db, err := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
			require.NoError(t, err)
			paths := initializationIntentPaths(t, dir)
			require.Len(t, paths, 1)
			intentBefore, readErr := os.ReadFile(paths[0])
			require.NoError(t, readErr)
			version, exists, versionErr := readBondDataVersion(db.Backend())
			require.NoError(t, versionErr)
			require.True(t, exists)
			require.Equal(t, BOND_DB_DATA_VERSION, version)
			pending, pendingErr := readBondInitializationPending(db.Backend())
			require.NoError(t, pendingErr)
			require.False(t, pending)
			storedCatalog, catalogErr := catalogDefinitionBytes(db.Backend())
			require.NoError(t, catalogErr)
			require.NotEmpty(t, storedCatalog)
			require.NoError(t, db.Close())

			// Once version metadata exists, the permanent provenance marker must
			// not enter either internal-pending recovery path.
			readOpenBondInitializationPending = func(*pebble.DB) (bool, error) {
				return false, errors.New("unexpected pending read on versioned reopen")
			}
			markOpenBondInitializationPending = func(*pebble.DB) error {
				return errors.New("unexpected pending write on versioned reopen")
			}
			db, retryErr := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
			require.NoError(t, retryErr)
			pathsAfter := initializationIntentPaths(t, dir)
			require.Equal(t, paths, pathsAfter)
			intentAfter, readErr := os.ReadFile(pathsAfter[0])
			require.NoError(t, readErr)
			require.Equal(t, intentBefore, intentAfter)
			require.NoError(t, db.Close())
		})
	}
}

func TestPermanentInitializationIntentNeverRemovesIndependentReplacement(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "intent-replacement")
	replacement := []byte("independent replacement")
	db, err := Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	paths := initializationIntentPaths(t, dir)
	require.Len(t, paths, 1)
	require.NoError(t, os.WriteFile(paths[0], replacement, 0o600))

	_, err = Open(dir, nil)
	require.ErrorContains(t, err, "decode external initialization intent")
	stored, readErr := os.ReadFile(paths[0])
	require.NoError(t, readErr)
	require.Equal(t, replacement, stored)
}

func TestFailedFirstOpenPreservesNewStoreAndCanRetryOnCustomFilesystem(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "custom-fs-failure")
	memoryFS := vfs.NewMem()
	requested := DefaultPebbleOptions()
	requested.FS = memoryFS
	previousWrite := writeOpenStorageCompatibility
	writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
		if err := WriteStorageCompatibility(dirname, compatibility); err != nil {
			return err
		}
		return errors.New("injected sidecar failure")
	}
	t.Cleanup(func() { writeOpenStorageCompatibility = previousWrite })

	catalog := minimalOpenTestCatalog(t)
	_, err := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
	require.ErrorContains(t, err, "injected sidecar failure")
	_, err = memoryFS.Stat(dir)
	require.NoError(t, err)
	require.DirExists(t, dir)
	writeOpenStorageCompatibility = previousWrite
	db, err := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
	require.NoError(t, err)
	pending, err := readBondInitializationPending(db.Backend())
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, db.Close())
}

func TestConcurrentFirstOpenUsesExclusiveDestinationClaim(t *testing.T) {
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	for _, filesystem := range filesystems {
		t.Run(filesystem.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "concurrent-first-open")
			requested := DefaultPebbleOptions()
			if filesystem.custom {
				requested.FS = vfs.NewMem()
			}
			catalog := minimalOpenTestCatalog(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			previousWrite := writeOpenStorageCompatibility
			writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
				close(entered)
				<-release
				return WriteStorageCompatibility(dirname, compatibility)
			}
			t.Cleanup(func() {
				writeOpenStorageCompatibility = previousWrite
				select {
				case <-release:
				default:
					close(release)
				}
			})

			type openResult struct {
				db  DB
				err error
			}
			firstResult := make(chan openResult, 1)
			go func() {
				db, err := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
				firstResult <- openResult{db: db, err: err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first Open did not reach the held sidecar step")
			}

			_, secondErr := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
			require.ErrorContains(t, secondErr, "another Open may be active")
			close(release)
			result := <-firstResult
			require.NoError(t, result.err)
			require.NotNil(t, result.db)
			require.NoError(t, result.db.Close())
		})
	}
}

func TestOpenClaimCoordinatesAcrossProcesses(t *testing.T) {
	if os.Getenv(openClaimHelperEnv) == "1" {
		dir := os.Getenv(openClaimHelperDirEnv)
		transaction, err := inspectOpenDestination(vfs.Default, dir)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(os.Getenv(openClaimHelperReady), []byte("ready"), 0o644))
		require.Eventually(t, func() bool {
			_, err := os.Stat(os.Getenv(openClaimHelperRelease))
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, transaction.claim.Close())
		return
	}

	root := t.TempDir()
	dir := filepath.Join(root, "database")
	ready := filepath.Join(root, "helper-ready")
	release := filepath.Join(root, "helper-release")
	command := exec.Command(os.Args[0], "-test.run=^TestOpenClaimCoordinatesAcrossProcesses$")
	command.Env = append(os.Environ(),
		openClaimHelperEnv+"=1",
		openClaimHelperDirEnv+"="+dir,
		openClaimHelperReady+"="+ready,
		openClaimHelperRelease+"="+release,
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
		}
	})
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, output.String())

	_, err := inspectOpenDestination(vfs.Default, dir)
	require.ErrorContains(t, err, "another Open may be active")
	require.NoError(t, os.WriteFile(release, []byte("release"), 0o644))
	require.NoError(t, command.Wait(), output.String())
	require.FileExists(t, filepath.Join(dir, "bond", bondOpenOSClaimName))
	require.FileExists(t, filepath.Join(dir, bondOpenPebbleClaimName))
	require.NoFileExists(t, filepath.Join(dir, "bond", ".bond-open-os.lock"))
	require.NoFileExists(t, filepath.Join(dir, ".bond-open-pebble.lock"))
}

func TestConcurrentFirstOpenCanonicalizesRealAndSymlinkAlias(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	require.NoError(t, os.MkdirAll(realParent, 0o755))
	aliasParent := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	realDir := filepath.Join(realParent, "database")
	aliasDir := filepath.Join(aliasParent, "database")
	entered := make(chan struct{})
	release := make(chan struct{})
	previousWrite := writeOpenStorageCompatibility
	writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
		close(entered)
		<-release
		return WriteStorageCompatibility(dirname, compatibility)
	}
	t.Cleanup(func() {
		writeOpenStorageCompatibility = previousWrite
		select {
		case <-release:
		default:
			close(release)
		}
	})

	type openResult struct {
		db  DB
		err error
	}
	firstResult := make(chan openResult, 1)
	go func() {
		db, err := Open(aliasDir, nil)
		firstResult <- openResult{db: db, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("symlink-alias Open did not reach the held sidecar step")
	}

	_, secondErr := Open(realDir, nil)
	require.ErrorContains(t, secondErr, "another Open may be active")
	close(release)
	result := <-firstResult
	require.NoError(t, result.err)
	require.Equal(t, realDir, result.db.Dir())
	require.NoError(t, result.db.Close())
	require.FileExists(t, filepath.Join(realDir, "bond", bondOpenOSClaimName))
	require.FileExists(t, filepath.Join(realDir, bondOpenPebbleClaimName))
}

func TestOpenClaimsAreStableAndExistingOpenDoesNotWriteParent(t *testing.T) {
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	for _, filesystem := range filesystems {
		t.Run(filesystem.name, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "database")
			requested := DefaultPebbleOptions()
			configuredFS := vfs.FS(vfs.Default)
			if filesystem.custom {
				configuredFS = vfs.NewMem()
				requested.FS = configuredFS
			}
			db, err := Open(dir, &Options{PebbleOptions: requested})
			require.NoError(t, err)
			require.NoError(t, db.Close())
			osClaimPath := filepath.Join(dir, "bond", bondOpenOSClaimName)
			pebbleClaimPath := configuredFS.PathJoin(dir, bondOpenPebbleClaimName)
			require.FileExists(t, osClaimPath)
			_, err = configuredFS.Stat(pebbleClaimPath)
			require.NoError(t, err)
			parentEntries, err := os.ReadDir(parent)
			require.NoError(t, err)
			require.Len(t, parentEntries, 1)
			require.Equal(t, "database", parentEntries[0].Name())

			transaction, err := inspectOpenDestination(configuredFS, dir)
			require.NoError(t, err)
			require.Equal(t, osClaimPath, transaction.claim.os.path)
			require.Equal(t, pebbleClaimPath, transaction.claim.pebble.path)
			require.NoError(t, transaction.claim.Close())
			require.FileExists(t, osClaimPath)
			_, err = configuredFS.Stat(pebbleClaimPath)
			require.NoError(t, err)

			require.NoError(t, os.Chmod(parent, 0o555))
			t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
			db, err = Open(dir, &Options{PebbleOptions: requested})
			require.NoError(t, err)
			require.NoError(t, db.Close())
			require.NoError(t, os.Chmod(parent, 0o755))
			require.FileExists(t, osClaimPath)
			_, err = configuredFS.Stat(pebbleClaimPath)
			require.NoError(t, err)
		})
	}
}

func TestFailedFirstOpenPreservesHookCreatedSentinels(t *testing.T) {
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	for _, filesystem := range filesystems {
		t.Run(filesystem.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "sentinel-failure")
			requested := DefaultPebbleOptions()
			configuredFS := vfs.FS(vfs.Default)
			if filesystem.custom {
				configuredFS = vfs.NewMem()
				requested.FS = configuredFS
			}
			osSentinel := filepath.Join(dir, "hook-os-sentinel")
			pebbleSentinel := configuredFS.PathJoin(dir, "hook-pebble-sentinel")
			pebbleLookingSentinel := configuredFS.PathJoin(dir, "999999.sst")
			previousWrite := writeOpenStorageCompatibility
			writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
				if err := WriteStorageCompatibility(dirname, compatibility); err != nil {
					return err
				}
				if err := os.WriteFile(osSentinel, []byte("os-sentinel"), 0o644); err != nil {
					return err
				}
				if filesystem.custom {
					file, err := configuredFS.Create(pebbleSentinel, vfs.WriteCategoryUnspecified)
					if err != nil {
						return err
					}
					if _, err := file.Write([]byte("pebble-sentinel")); err != nil {
						_ = file.Close()
						return err
					}
					if err := file.Close(); err != nil {
						return err
					}
					file, err = configuredFS.Create(pebbleLookingSentinel, vfs.WriteCategoryUnspecified)
					if err != nil {
						return err
					}
					if _, err := file.Write([]byte("pebble-looking-sentinel")); err != nil {
						_ = file.Close()
						return err
					}
					if err := file.Close(); err != nil {
						return err
					}
				} else if err := os.WriteFile(pebbleLookingSentinel, []byte("pebble-looking-sentinel"), 0o644); err != nil {
					return err
				}
				return errors.New("injected failure after sentinels")
			}
			t.Cleanup(func() { writeOpenStorageCompatibility = previousWrite })

			_, err := Open(dir, &Options{Catalog: minimalOpenTestCatalog(t), PebbleOptions: requested})
			require.ErrorContains(t, err, "injected failure after sentinels")
			stored, err := os.ReadFile(osSentinel)
			require.NoError(t, err)
			require.Equal(t, []byte("os-sentinel"), stored)
			require.FileExists(t, filepath.Join(dir, "bond", PebbleFormatFile))
			require.FileExists(t, filepath.Join(dir, "bond", StorageCompatibilityFile))
			if filesystem.custom {
				require.Equal(t, []byte("pebble-sentinel"), readVFSFile(t, configuredFS, pebbleSentinel))
			}
			require.Equal(t, []byte("pebble-looking-sentinel"), readVFSFile(t, configuredFS, pebbleLookingSentinel))
		})
	}
}

func TestFailedFirstOpenPreservesConcurrentPebbleLookingSentinel(t *testing.T) {
	filesystems := []struct {
		name   string
		custom bool
	}{
		{name: "default"},
		{name: "custom", custom: true},
	}
	for _, filesystem := range filesystems {
		t.Run(filesystem.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "concurrent-sentinel-failure")
			requested := DefaultPebbleOptions()
			configuredFS := vfs.FS(vfs.Default)
			if filesystem.custom {
				configuredFS = vfs.NewMem()
				requested.FS = configuredFS
			}
			entered := make(chan struct{})
			sentinelCreated := make(chan struct{})
			previousWrite := writeOpenStorageCompatibility
			writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
				if err := WriteStorageCompatibility(dirname, compatibility); err != nil {
					return err
				}
				close(entered)
				<-sentinelCreated
				return errors.New("injected failure after concurrent sentinel")
			}
			t.Cleanup(func() {
				writeOpenStorageCompatibility = previousWrite
				select {
				case <-sentinelCreated:
				default:
					close(sentinelCreated)
				}
			})

			catalog := minimalOpenTestCatalog(t)
			errResult := make(chan error, 1)
			go func() {
				_, err := Open(dir, &Options{Catalog: catalog, PebbleOptions: requested})
				errResult <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Open did not reach the held sidecar step")
			}

			sentinel := configuredFS.PathJoin(dir, "888888.sst")
			writeVFSFile(t, configuredFS, sentinel, []byte("concurrent-pebble-looking-sentinel"))
			close(sentinelCreated)
			err := <-errResult
			require.ErrorContains(t, err, "injected failure after concurrent sentinel")
			require.Equal(t, []byte("concurrent-pebble-looking-sentinel"), readVFSFile(t, configuredFS, sentinel))
		})
	}
}

func TestFailedFirstOpenPreservesIndependentlyReplacedFixedSidecar(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "independent-sidecar-failure")
	entered := make(chan struct{})
	replaced := make(chan struct{})
	previousWrite := writeOpenStorageCompatibility
	writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
		if err := WriteStorageCompatibility(dirname, compatibility); err != nil {
			return err
		}
		close(entered)
		<-replaced
		return errors.New("injected failure after independent sidecar replacement")
	}
	t.Cleanup(func() {
		writeOpenStorageCompatibility = previousWrite
		select {
		case <-replaced:
		default:
			close(replaced)
		}
	})

	catalog := minimalOpenTestCatalog(t)
	errResult := make(chan error, 1)
	go func() {
		_, err := Open(dir, &Options{Catalog: catalog})
		errResult <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Open did not reach the held sidecar step")
	}

	formatPath := filepath.Join(dir, "bond", PebbleFormatFile)
	independent := []byte("independently-replaced-format-sidecar")
	require.NoError(t, os.WriteFile(formatPath, independent, 0o644))
	close(replaced)
	err := <-errResult
	require.ErrorContains(t, err, "injected failure after independent sidecar replacement")
	stored, err := os.ReadFile(formatPath)
	require.NoError(t, err)
	require.Equal(t, independent, stored)
}

func TestCatalogAdoptionIsNotPersistedWhenSidecarWriteFails(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "legacy-adoption")
	legacyDB, err := Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, legacyDB.Close())

	previousWrite := writeOpenStorageCompatibility
	writeOpenStorageCompatibility = func(dirname string, compatibility StorageCompatibility) error {
		if err := WriteStorageCompatibility(dirname, compatibility); err != nil {
			return err
		}
		return errors.New("injected sidecar failure")
	}
	_, err = Open(dir, &Options{Catalog: minimalOpenTestCatalog(t)})
	writeOpenStorageCompatibility = previousWrite
	require.ErrorContains(t, err, "injected sidecar failure")

	legacyDB, err = Open(dir, nil)
	require.NoError(t, err)
	requireCatalogMetadataAbsent(t, legacyDB.Backend())
	require.NoError(t, legacyDB.Close())
}
