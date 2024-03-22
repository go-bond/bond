package utils

import (
	"os"
	"path"
	"strings"
)

func PathExpand(p string) (string, error) {
	if !strings.HasPrefix(p, "/") {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		p = path.Clean(path.Join(wd, p))
	}
	return p, nil
}
