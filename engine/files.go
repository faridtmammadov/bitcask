package engine

import (
	"errors"
	"io/fs"
	"os"
	"regexp"
	"strconv"
)

func isFileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func createFilenameId(filename string) string {
	if filename == "" {
		return "1000000000"
	}
	pattern := regexp.MustCompile(`(\d+)\.bitcask`)
	matches := pattern.FindStringSubmatch(filename)

	filenameId, _ := strconv.Atoi(matches[1])

	return strconv.Itoa(filenameId + 1)
}
