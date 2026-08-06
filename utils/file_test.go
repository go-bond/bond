package utils

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFileWithSyncFailureRemovesOnlyUniqueTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")
	err := writeFileWithSync(path, []byte("published"), 0o644, filePublicationHooks{
		beforePublish: func(string) error { return errors.New("injected publication failure") },
	})
	require.ErrorContains(t, err, "injected publication failure")
	require.NoFileExists(t, path)
	temporary, err := filepath.Glob(filepath.Join(dir, ".metadata.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temporary)
}

func TestWriteFileWithSyncCollisionPreservesDifferentDestination(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")
	independent := []byte("independent")
	err := writeFileWithSync(path, []byte("published"), 0o644, filePublicationHooks{
		beforePublish: func(string) error {
			return os.WriteFile(path, independent, 0o644)
		},
	})
	require.ErrorIs(t, err, ErrFileContentConflict)
	stored, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, independent, stored)
}

func TestWriteFileWithSyncDoesNotRemoveReplacementAfterPublication(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")
	independent := []byte("replacement")
	err := writeFileWithSync(path, []byte("published"), 0o644, filePublicationHooks{
		afterPublish: func(publishedPath string) error {
			require.NoError(t, os.Remove(publishedPath))
			require.NoError(t, os.WriteFile(publishedPath, independent, 0o644))
			return errors.New("injected failure after replacement")
		},
	})
	require.ErrorContains(t, err, "injected failure after replacement")
	stored, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, independent, stored)
	temporary, globErr := filepath.Glob(filepath.Join(dir, ".metadata.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, temporary)
}

func TestWriteFileWithSyncAcceptsOnlyIdenticalExistingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")
	require.NoError(t, WriteFileWithSync(path, []byte("same"), 0o644))
	before, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, WriteFileWithSync(path, []byte("same"), 0o600))
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	err = WriteFileWithSync(path, []byte("different"), 0o644)
	require.ErrorIs(t, err, ErrFileContentConflict)
	stored, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("same"), stored)
}

func TestReplaceFileWithSyncPreservesTargetBeforeRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	err := replaceFileWithSync(path, []byte("new"), 0o600, fileReplacementHooks{
		beforeRename: func(temporaryPath string) error {
			replacement, readErr := os.ReadFile(temporaryPath)
			require.NoError(t, readErr)
			require.Equal(t, []byte("new"), replacement)
			return errors.New("injected pre-rename failure")
		},
	})
	require.ErrorContains(t, err, "injected pre-rename failure")
	stored, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("old"), stored)
	temporary, globErr := filepath.Glob(filepath.Join(dir, ".metadata.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, temporary)
}

func TestReplaceFileWithSyncReplacesMutableContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata")
	require.NoError(t, ReplaceFileWithSync(path, []byte("old"), 0o644))
	require.NoError(t, ReplaceFileWithSync(path, []byte("new"), 0o600))
	stored, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), stored)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}
