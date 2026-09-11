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

func (d *Decoder) ReadInt8() int8 {
	if d.err != nil {
		return 0
	}

	if d.pos+1 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}

	v := d.buf[d.pos]
	d.pos++
	return int8(v)
}

func (d *Decoder) ReadInt64() int64 {
	if d.err != nil {
		return 0
	}

	if d.pos+8 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}

	v := binary.BigEndian.Uint64(d.buf[d.pos : d.pos+8])
	d.pos += 8
	return int64(v)
}

func (d *Decoder) ReadVarint() int64 {
	if d.err != nil {
		return 0
	}

	v, n := binary.Varint(d.buf[d.pos:])

	if n <= 0 {
		d.err = io.ErrUnexpectedEOF
		return 0
	}

	d.pos += n
	return v
}

func (d *Decoder) ReadRawBytes(n int) []byte {
	if d.err != nil {
		return nil
	}

	if n < 0 || d.pos+n > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}

	b := make([]byte, n)
	copy(b, d.buf[d.pos:d.pos+n])
	d.pos += n
	return b
}

func (d *Decoder) ReadCompactBytes() []byte {
	l := d.ReadUvarint()
	if d.err != nil || l == 0 {
		return nil // null (0), or a failed length read: no bytes
	}

	return d.ReadRawBytes(int(l - 1)) // COMPACT_BYTES: length N+1, then N bytes
}

func (d *Decoder) Remaining() int {
	return len(d.buf) - d.pos
}

func (d *Decoder) Err() error {
	return d.err
}
