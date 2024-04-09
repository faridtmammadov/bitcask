package engine

import (
	"bytes"
	"encoding/binary"
)

const headerSize = 16

type Header struct {
	Checksum  uint32
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
}

func (h *Header) EncodeHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, &h.Checksum)
	binary.Write(buf, binary.LittleEndian, &h.TimeStamp)
	binary.Write(buf, binary.LittleEndian, &h.KeySize)
	binary.Write(buf, binary.LittleEndian, &h.ValueSize)
	return err
}

func (h *Header) DecodeHeader(buf []byte) error {
	err := binary.Read(bytes.NewReader(buf[0:4]), binary.LittleEndian, &h.Checksum)
	binary.Read(bytes.NewReader(buf[4:8]), binary.LittleEndian, &h.TimeStamp)
	binary.Read(bytes.NewReader(buf[8:12]), binary.LittleEndian, &h.KeySize)
	binary.Read(bytes.NewReader(buf[12:16]), binary.LittleEndian, &h.ValueSize)
	return err
}

func NewHeader(buf []byte) (*Header, error) {
	h := &Header{}
	err := h.DecodeHeader(buf)
	if err != nil {
		return nil, err
	}
	return h, nil
}
