package bond

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	crdberrors "github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	bondOpenOSClaimName     = ".bond-open-claim-os-v1.lock"
	bondOpenPebbleClaimName = ".bond-open-claim-pebble-v1.lock"
)

var processOpenClaims = struct {
	sync.Mutex
	active map[string]struct{}
}{active: make(map[string]struct{})}

type openDestinationTransaction struct {
	claim *openDestinationClaim

	pebbleBefore       map[string]struct{}
	preexisting        bool
	hasManifestPointer bool
}

type openDestinationClaim struct {
	pebble          *persistentFilesystemClaim
	os              *persistentFilesystemClaim
	processIdentity string
	processHeld     bool
}

type persistentFilesystemClaim struct {
	path   string
	closer io.Closer
}

func inspectOpenDestination(fs vfs.FS, dirname string) (openDestinationTransaction, error) {
	claim, err := acquireOpenDestinationClaim(fs, dirname)
	if err != nil {
		return openDestinationTransaction{}, err
	}
	transaction := openDestinationTransaction{
		claim:        claim,
		pebbleBefore: make(map[string]struct{}),
	}
	failed := true
	defer func() {
		if failed {
			_ = claim.Close()
		}
	}()

	_, transaction.pebbleBefore, err = inspectPebbleTopLevel(fs, dirname)
	if err != nil {
		return openDestinationTransaction{}, fmt.Errorf("bond: inspect Pebble destination before open: %w", err)
	}
	for name := range transaction.pebbleBefore {
		if name == "CURRENT" || strings.HasPrefix(name, "marker.manifest.") {
			transaction.hasManifestPointer = true
		}
		if isPebbleStorageMarker(name) {
			transaction.preexisting = true
		}
	}

	failed = false
	return transaction, nil
}

func acquireOpenDestinationClaim(fs vfs.FS, dirname string) (*openDestinationClaim, error) {
	// Every Open writes Bond sidecars through the OS filesystem, so the OS
	// claim is the common first lock. The configured Pebble-FS claim follows.
	// Distinct filenames make this ordering safe when both namespaces are the
	// default filesystem while still coordinating custom filesystems.
	processIdentity, err := canonicalDefaultFSDestination(dirname)
	if err != nil {
		return nil, fmt.Errorf("bond: resolve database open claim identity: %w", err)
	}
	claim := &openDestinationClaim{processIdentity: processIdentity}
	processOpenClaims.Lock()
	if _, exists := processOpenClaims.active[processIdentity]; exists {
		processOpenClaims.Unlock()
		return nil, fmt.Errorf("bond: claim database open for %q (another Open may be active)", dirname)
	}
	processOpenClaims.active[processIdentity] = struct{}{}
	claim.processHeld = true
	processOpenClaims.Unlock()

	claim.os, err = acquirePersistentFilesystemClaim(vfs.Default, dirname, bondOpenOSClaimName, true)
	if err != nil {
		_ = claim.Close()
		return nil, fmt.Errorf("bond: claim database open for %q on OS filesystem (another Open may be active): %w", dirname, err)
	}
	claim.pebble, err = acquirePersistentFilesystemClaim(fs, dirname, bondOpenPebbleClaimName, false)
	if err != nil {
		closeErr := claim.Close()
		return nil, errors.Join(
			fmt.Errorf("bond: claim database open for %q on configured Pebble filesystem (another Open may be active): %w", dirname, err),
			closeErr,
		)
	}
	return claim, nil
}

func acquirePersistentFilesystemClaim(
	fs vfs.FS,
	dirname string,
	name string,
	useBondDirectory bool,
) (*persistentFilesystemClaim, error) {
	directory := dirname
	if useBondDirectory {
		directory = fs.PathJoin(dirname, "bond")
	}
	if err := fs.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	path := fs.PathJoin(directory, name)
	closer, err := fs.Lock(path)
	if err != nil {
		return nil, err
	}
	return &persistentFilesystemClaim{path: path, closer: closer}, nil
}

func (c *openDestinationClaim) Close() error {
	if c == nil {
		return nil
	}
	var closeErr error
	// Release in reverse acquisition order. Stable claim paths are never
	// unlinked, so every opener always contends on the same inode.
	if c.pebble != nil {
		closeErr = errors.Join(closeErr, c.pebble.Close())
		c.pebble = nil
	}
	if c.os != nil {
		closeErr = errors.Join(closeErr, c.os.Close())
		c.os = nil
	}
	if c.processHeld {
		processOpenClaims.Lock()
		delete(processOpenClaims.active, c.processIdentity)
		processOpenClaims.Unlock()
		c.processHeld = false
	}
	return closeErr
}

func (c *persistentFilesystemClaim) Close() error {
	if c == nil || c.closer == nil {
		return nil
	}
	closeErr := c.closer.Close()
	c.closer = nil
	return closeErr
}

func canonicalizeDefaultFSDestination(fs vfs.FS, dirname string) (string, error) {
	if reflect.TypeOf(fs) != reflect.TypeOf(vfs.Default) {
		return dirname, nil
	}
	return canonicalDefaultFSDestination(dirname)
}

func canonicalDefaultFSDestination(dirname string) (string, error) {
	candidate := dirname
	missing := make([]string, 0)
	for {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", err
		}
		missing = append(missing, filepath.Base(candidate))
		candidate = parent
	}
}

func inspectPebbleTopLevel(fs vfs.FS, dirname string) (bool, map[string]struct{}, error) {
	entries := make(map[string]struct{})
	info, err := fs.Stat(dirname)
	if crdberrors.IsNotExist(err) {
		return false, entries, nil
	}
	if err != nil {
		return false, nil, err
	}
	if !info.IsDir() {
		return true, entries, nil
	}
	names, err := fs.List(dirname)
	if err != nil {
		return false, nil, err
	}
	for _, name := range names {
		entries[name] = struct{}{}
	}
	return true, entries, nil
}

func isPebbleStorageMarker(name string) bool {
	return name == "CURRENT" ||
		name == "LOCK" ||
		strings.HasPrefix(name, "MANIFEST-") ||
		strings.HasPrefix(name, "OPTIONS-") ||
		strings.HasPrefix(name, "marker.") ||
		isPebbleNumberedArtifact(name) ||
		strings.HasPrefix(name, "REMOTE-OBJ-CATALOG")
}

func isPebbleNumberedArtifact(name string) bool {
	if strings.HasPrefix(name, "CURRENT.") && strings.HasSuffix(name, ".dbtmp") {
		return isDecimal(strings.TrimSuffix(strings.TrimPrefix(name, "CURRENT."), ".dbtmp"))
	}
	if strings.HasPrefix(name, "temporary.") && strings.HasSuffix(name, ".dbtmp") {
		return isDecimal(strings.TrimSuffix(strings.TrimPrefix(name, "temporary."), ".dbtmp"))
	}
	if strings.HasSuffix(name, ".sst") {
		return isDecimal(strings.TrimSuffix(name, ".sst"))
	}
	if strings.HasSuffix(name, ".blob") {
		return isDecimal(strings.TrimSuffix(name, ".blob"))
	}
	if strings.HasSuffix(name, ".log") {
		base := strings.TrimSuffix(name, ".log")
		parts := strings.Split(base, "-")
		return (len(parts) == 1 && isDecimal(parts[0])) ||
			(len(parts) == 2 && isDecimal(parts[0]) && isDecimal(parts[1]))
	}
	parts := strings.Split(name, ".")
	return len(parts) == 3 && isDecimal(parts[0]) && parts[1] == "blobmeta" && isDecimal(parts[2])
}

func isDecimal(value string) bool {
	if value == "" {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}
