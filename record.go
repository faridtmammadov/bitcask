package bitcask

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

func NewRecord(header Header, key string, value string) *Record {
	record := &Record{
		Header:     header,
		Key:        key,
		Value:      value,
		RecordSize: header.KeySize + header.ValueSize + headerSize,
	}

	header.Checksum = record.CalculateCheckSum()

	return record
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	r.Header.EncodeHeader(buf)
	buf.WriteString(r.Key)
	_, err := buf.WriteString(r.Value)
	return err
}

func (r *Record) DecodeKV(buf []byte) error {
	err := r.Header.DecodeHeader(buf[:headerSize])
	r.Key = string(buf[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(buf[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.RecordSize = headerSize + r.Header.KeySize + r.Header.ValueSize
	return err
}

func (r *Record) CalculateCheckSum() uint32 {
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
