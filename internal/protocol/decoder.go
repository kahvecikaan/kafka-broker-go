package protocol

import (
	"encoding/binary"
	"io"
)

type Decoder struct {
	buf []byte
	pos int
	err error
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{buf: buf, pos: 0, err: nil}
}

func (d *Decoder) ReadInt16() int16 {
	if d.err != nil {
		return 0
	}

	if d.pos+2 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}

	v := binary.BigEndian.Uint16(d.buf[d.pos : d.pos+2])
	d.pos += 2
	return int16(v)
}

func (d *Decoder) ReadInt32() int32 {
	if d.err != nil {
		return 0
	}

	if d.pos+4 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}

	v := binary.BigEndian.Uint32(d.buf[d.pos : d.pos+4])
	d.pos += 4
	return int32(v)
}

func (d *Decoder) Err() string {
	return d.Err()
}
