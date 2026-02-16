package backup

import (
	"fmt"
	"os"
	"path/filepath"
)

const restoreIncompleteFileName = ".incomplete"

// restoreIncompleteFilePath returns the path to the .incomplete marker inside dir.
func restoreIncompleteFilePath(dir string) string {
	return filepath.Join(dir, restoreIncompleteFileName)
}

// HasIncompleteRestore reports whether dir contains a .incomplete marker,
// indicating a previously interrupted restore.
func HasIncompleteRestore(dir string) bool {
	_, err := os.Stat(restoreIncompleteFilePath(dir))
	return err == nil
}

// writeRestoreIncompleteMarker creates the .incomplete marker file in dir.
// The directory must already exist.
func writeRestoreIncompleteMarker(dir string) error {
	f, err := os.Create(restoreIncompleteFilePath(dir))
	if err != nil {
		return fmt.Errorf("create restore incomplete marker: %w", err)
	}
	return f.Close()
}

// removeRestoreIncompleteMarker removes the .incomplete marker from dir.
// Returns nil if the marker does not exist.
func removeRestoreIncompleteMarker(dir string) error {
	err := os.Remove(restoreIncompleteFilePath(dir))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove restore incomplete marker: %w", err)
	}
	return nil
}

// cleanRestoreDir removes all contents of dir (but not dir itself).
func cleanRestoreDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read restore dir for cleanup: %w", err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("remove %s during restore cleanup: %w", e.Name(), err)
		}
	}
	return nil
}
