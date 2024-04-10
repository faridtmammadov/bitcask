package engine

import (
	"os"
)

type KeyEntry struct {
	timestamp     uint32
	file          *os.File
	valuePosition uint32
	valueSize     uint32
}

func NewKeyEntry(timestamp uint32, file *os.File, valuePosition uint32, valueSize uint32) KeyEntry {
	return KeyEntry{timestamp, file, valuePosition, valueSize}
}
