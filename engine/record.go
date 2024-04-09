package engine

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type Record struct {
	Header     Header
	Key        string
	Value      string
	RecordSize uint32
}

func NewRecord(timestamp uint32, key string, value string) *Record {
	header := Header{TimeStamp: timestamp, KeySize: uint32(len(key)), ValueSize: uint32(len(value))}
	record := &Record{
		Header:     header,
		Key:        key,
		Value:      value,
		RecordSize: header.KeySize + header.ValueSize + headerSize,
	}

	record.Header.Checksum = calculateChecksum(record)

	return record
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	r.Header.EncodeHeader(buf)
	buf.WriteString(r.Key)
	_, err := buf.WriteString(r.Value)
	return err
}

func DecodeKV(buf []byte) (*Record, error) {
	r := new(Record)
	err := r.Header.DecodeHeader(buf[:headerSize])
	r.Key = string(buf[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(buf[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.RecordSize = headerSize + r.Header.KeySize + r.Header.ValueSize

	if err != nil {
		return nil, err
	}

	return r, nil
}

func calculateChecksum(r *Record) uint32 {
	// encode header
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.TimeStamp)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.KeySize)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.ValueSize)

	// encode kv
	kvBuf := append([]byte(r.Key), []byte(r.Value)...)

	buf := append(headerBuf.Bytes(), kvBuf...)
	return crc32.ChecksumIEEE(buf)
}

func (r *Record) VerifyChecksum(data []byte) bool {
	return crc32.ChecksumIEEE(data[4:]) == r.Header.Checksum
}
