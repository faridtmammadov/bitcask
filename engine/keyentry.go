package engine

import (
	"os"
)

type KeyEntry struct {
	timestamp uint32
	file      *os.File
	position  uint32
	totalSize uint32
}

func NewKeyEntry(timestamp uint32, file *os.File, position uint32, totalSize uint32) KeyEntry {
	return KeyEntry{timestamp, file, position, totalSize}
}
