package backup

import "os"

// HasCheckpoint reports whether a checkpoint directory exists at dir.
func HasCheckpoint(dir string) (bool, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

// RemoveCheckpoint removes the checkpoint directory at dir.
// Returns nil if the directory does not exist.
func RemoveCheckpoint(dir string) error {
	return os.RemoveAll(dir)
}
