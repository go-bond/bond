package filters

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"

	"github.com/go-bond/bond"
)

type FilterFSStorer struct {
	path          string
	noASCIIRegexp *regexp.Regexp
}

func NewFSFilterStorer(path string) *FilterFSStorer {
	return &FilterFSStorer{path: path, noASCIIRegexp: regexp.MustCompile("[^A-Za-z0-9_.]")}
}

func (f *FilterFSStorer) Get(key []byte, batch ...bond.Batch) (data []byte, closer io.Closer, err error) {
	filePath := f.prepareFilePath(f.path, string(key))

	data, err = os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}
	return data, io.NopCloser(nil), nil
}

func (f *FilterFSStorer) Set(key []byte, value []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	filePath := f.prepareFilePath(f.path, string(key))

	// check if directoru exists
	if _, err := os.Stat(path.Dir(filePath)); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(path.Dir(filePath), 0770)
			if err != nil {
				return err
			}
		}
	}

	err := os.WriteFile(filePath, value, 0660)
	if err != nil {
		return err
	}
	return nil
}

func (f *FilterFSStorer) DeleteRange(start []byte, end []byte, opt bond.WriteOptions, batch ...bond.Batch) error {
	startFile := []byte(f.prepareFilePath("", string(start)))

	dirFS := os.DirFS(f.path)
	err := fs.WalkDir(dirFS, ".", func(filePath string, d fs.DirEntry, err error) error {
		if bytes.Compare([]byte(filePath), startFile) >= 0 {
			return os.Remove(path.Join(f.path, filePath))
		}
		return nil
	})

	return err
}

func (f *FilterFSStorer) prepareFilePath(root, fileName string) string {
	return path.Join(
		root,
		f.noASCIIRegexp.ReplaceAllLiteralString(fileName, ""),
	)
}

var _ bond.FilterStorer = &FilterFSStorer{}
