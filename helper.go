package dbunit

import (
	"path/filepath"
	"strings"
)

func extractFileName(fileName string) string {
	return strings.Replace(fileName, filepath.Ext(fileName), "", 1)
}
