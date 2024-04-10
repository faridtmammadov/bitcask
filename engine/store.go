package engine

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MaxFileSize
// while writing to file if max file size reached, new file will be created
var MaxFileSize int64 = 1 << 30

type DiskStore struct {
	// directory name that contains all data files
	dir string
	// dataFile object pointing the file_name
	dataFile *os.File
	// current cursor position in the file where the data can be written
	writePosition int
	// keyDir is a map of key and KeyEntry being the value. KeyEntry contains the position
	// of the byte offset in the file where the value exists. key_dir map acts as in-memory
	// index to fetch the values quickly from the disk
	keyDir map[string]KeyEntry
	mux    *sync.RWMutex
}

func Open(directoryPath string) (*DiskStore, error) {
	if _, err := os.Stat(directoryPath); os.IsNotExist(err) {
		return nil, err
	}

	diskStore := &DiskStore{
		dir:    directoryPath,
		keyDir: make(map[string]KeyEntry),
		mux:    &sync.RWMutex{},
	}

	err := diskStore.initKeyDir(directoryPath)

	if err != nil {
		log.Fatalf("error while loading the keys from disk: %v", err)
		return nil, err
	}

	return diskStore, nil
}

func (d *DiskStore) Get(key string) (string, error) {
	d.mux.RLock()
	defer d.mux.RUnlock()
	keyEntry, ok := d.keyDir[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	// move the current pointer to the right offset
	_, err := keyEntry.file.Seek(int64(keyEntry.valuePosition), 0)
	if err != nil {
		return "", ErrSeekFailed
	}

	data := make([]byte, keyEntry.valueSize)
	_, err = io.ReadFull(keyEntry.file, data)
	if err != nil {
		return "", ErrReadFailed
	}

	value := string(data)

	return value, nil
}

func (d *DiskStore) Set(key string, value string) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	if err := validateKV(key, value); err != nil {
		return err
	}

	timestamp := uint32(time.Now().Unix())

	r := NewRecord(timestamp, key, value)

	if err := d.checkMaxFileSizeReached(r.RecordSize); err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	err := r.EncodeKV(buf)
	if err != nil {
		return ErrEncodingFailed
	}
	d.writeData(buf.Bytes())

	d.keyDir[key] = NewKeyEntry(timestamp, d.dataFile, uint32(headerSize+r.Header.KeySize), r.Header.ValueSize)
	// update last write position, so that next record can be written from this point
	d.writePosition += int(r.RecordSize)

	return nil
}

func (d *DiskStore) Close() bool {
	// before we close the file, we need to safely write the contents in the buffers
	// to the disk. Check documentation of DiskStore.write() to understand
	// following the operations
	// TODO: handle errors
	d.dataFile.Sync()
	if err := d.dataFile.Close(); err != nil {
		// TODO: log the error
		return false
	}
	for _, v := range d.keyDir {
		v.file.Close()
	}
	return true
}

func (d *DiskStore) checkMaxFileSizeReached(size uint32) error {
	stat, _ := d.dataFile.Stat()
	nextSize := stat.Size() + int64(size)
	if nextSize > MaxFileSize {
		activeFile := createFilenameId(d.dataFile.Name()) + ".bitcask.data"
		file, err := os.Create(filepath.Join(d.dir, activeFile))
		if err != nil {
			return err
		}
		d.dataFile = file
		d.writePosition = 0
	}

	return nil
}

func (d *DiskStore) writeData(data []byte) {
	// saving stuff to a file reliably is hard!
	// if you would like to explore and learn more, then
	// start from here: https://danluu.com/file-consistency/
	// and read this too: https://lwn.net/Articles/457667/
	if _, err := d.dataFile.Write(data); err != nil {
		panic(err)
	}
	// calling fsync after every write is important, this assures that our writes
	// are actually persisted to the disk
	if err := d.dataFile.Sync(); err != nil {
		panic(err)
	}
}

func (d *DiskStore) initKeyDir(directoryName string) error {
	dirEntries, err := os.ReadDir(directoryName)
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}

		err = initKeyDirInternal(d.keyDir, filepath.Join(d.dir, entry.Name()))
		if err != nil {
			return err
		}
	}

	filenameId := createFilenameId("")
	filename := filenameId + ".bitcask.data"

	if len(dirEntries) > 0 {
		filenameId = createFilenameId(dirEntries[len(dirEntries)-1].Name())
		filename = filenameId + ".bitcask.data"
	}

	file, err := os.Create(filepath.Join(d.dir, filename))
	if err != nil {
		return err
	}
	d.dataFile = file
	d.writePosition = 0

	return nil
}

func initKeyDirInternal(keyDir map[string]KeyEntry, filepath string) error {
	// we will initialise the keyDir by reading the contents of the file, record by
	// record. As we read each record, we will also update our keyDir with the
	// corresponding KeyEntry
	//
	// NOTE: this method is a blocking one, if the DB size is yuge then it will take
	// a lot of time to startup
	file, _ := os.Open(filepath)
	writePosition := 0

	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(file, header)

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		h, err := NewHeader(header)
		if err != nil {
			return err
		}

		key := make([]byte, h.KeySize)
		value := make([]byte, h.ValueSize)

		_, err = io.ReadFull(file, key)
		if err != nil {
			return err
		}

		_, err = io.ReadFull(file, value)
		if err != nil {
			return err
		}

		data := append(header, key...)
		data = append(data, value...)

		record, err := DecodeKV(data)

		if err != nil {
			return err
		}

		checksumCorrect := record.VerifyChecksum(data)
		if !checksumCorrect {
			return ErrChecksumMismatch
		}

		keyDir[string(key)] = NewKeyEntry(h.TimeStamp, file, uint32(writePosition)+headerSize+h.KeySize, h.ValueSize)
		writePosition += int(record.RecordSize)
	}

	return nil
}

// returns a list of the current keys
func (d *DiskStore) ListKeys() []string {
	result := make([]string, 0, len(d.keyDir))

	for k := range d.keyDir {
		result = append(result, k)
	}

	return result
}

const (
	MaxKeySize   = 1<<32 - 1
	MaxValueSize = 1<<32 - 1
)

func validateKV(key string, value string) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if len(key) > MaxKeySize {
		return ErrLargeKey
	}

	if len(value) > MaxValueSize {
		return ErrLargeValue
	}

	return nil
}
