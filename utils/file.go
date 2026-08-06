package utils

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrFileContentConflict reports that an atomically-published file already
// exists with content other than the requested immutable content.
var ErrFileContentConflict = errors.New("file content conflict")

type filePublicationHooks struct {
	beforePublish func(string) error
	afterPublish  func(string) error
	syncDirectory func(string) error
}

// WriteFileWithSync publishes immutable file content without ever truncating,
// overwriting, or unlinking the destination. An identical destination is an
// idempotent success; a different destination is a conflict.
func WriteFileWithSync(path string, data []byte, mode os.FileMode) error {
	return writeFileWithSync(path, data, mode, filePublicationHooks{})
}

func writeFileWithSync(path string, data []byte, mode os.FileMode, hooks filePublicationHooks) error {
	dir := filepath.Dir(path)
	identical, err := identicalPublishedFile(path, data)
	if err != nil {
		return err
	}
	if identical {
		return syncPublishedFileDirectory(dir, hooks)
	}

	temporary, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		// CreateTemp gives this invocation exclusive ownership of this path.
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("failed to write temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}
	if hooks.beforePublish != nil {
		if err := hooks.beforePublish(temporaryPath); err != nil {
			return err
		}
	}

	if err := os.Link(temporaryPath, path); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("publish file without replacement: %w", err)
		}
		identical, validationErr := identicalPublishedFile(path, data)
		if validationErr != nil {
			return validationErr
		}
		if !identical {
			return fileContentConflict(path)
		}
		return syncPublishedFileDirectory(dir, hooks)
	}
	if hooks.afterPublish != nil {
		if err := hooks.afterPublish(path); err != nil {
			return err
		}
	}
	// First make the no-replace publication durable. The uniquely-owned
	// temporary link may then be removed and that directory update persisted.
	if err := syncPublishedFileDirectory(dir, hooks); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove published temporary link: %w", err)
	}
	if err := syncPublishedFileDirectory(dir, hooks); err != nil {
		return err
	}
	return nil
}

func identicalPublishedFile(path string, data []byte) (bool, error) {
	existing, err := os.ReadFile(path)
	if err == nil {
		if bytes.Equal(existing, data) {
			return true, nil
		}
		return false, fileContentConflict(path)
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func fileContentConflict(path string) error {
	return fmt.Errorf("%w at %q", ErrFileContentConflict, path)
}

func syncPublishedFileDirectory(dir string, hooks filePublicationHooks) error {
	if hooks.syncDirectory != nil {
		return hooks.syncDirectory(dir)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
