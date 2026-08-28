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

func (d *Decoder) ReadUvarint() uint64 {
	if d.err != nil {
		return 0
	}

	v, n := binary.Uvarint(d.buf[d.pos:])
	if n <= 0 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}

	d.pos += n
	return v
}
func (d *Decoder) ReadNullableString() string {
	n := d.ReadInt16()
	if d.err != nil || n == -1 {
		return ""
	}

	if d.pos+int(n) > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return ""
	}

	s := string(d.buf[d.pos : d.pos+int(n)])
	d.pos += int(n)
	return s
}

func (d *Decoder) ReadCompactString() string {
	if d.err != nil {
		return ""
	}

	l := d.ReadUvarint()
	if d.err != nil || l == 0 {
		return ""
	}

	n := int(l - 1)
	if d.pos+n > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return ""
	}

	s := string(d.buf[d.pos : d.pos+n])
	d.pos += n
	return s
}

func (d *Decoder) Err() error {
	return d.err
}
