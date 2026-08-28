package protocol

import (
	"encoding/binary"
)

type Encoder struct {
	buf []byte
}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) PutInt16(v int16) {
	e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(v))
}

func (e *Encoder) PutInt32(v int32) {
	e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(v))
}

func (e *Encoder) PutUvarint(v uint64) {
	e.buf = binary.AppendUvarint(e.buf, v)
}

func (e *Encoder) PutInt8(v int8) {
	e.buf = append(e.buf, byte(v))
}

func (e *Encoder) PutRawBytes(b []byte) {
	e.buf = append(e.buf, b...)
}

func (e *Encoder) PutCompactString(s string) {
	e.PutUvarint(uint64(len(s) + 1)) // COMPACT_STRING length is N+1, uvarint-encoded
	e.buf = append(e.buf, s...)
}

func (e *Encoder) PutBool(b bool) {
	if b {
		e.PutInt8(1)
	} else {
		e.PutInt8(0)
	}
}

func (e *Encoder) Bytes() []byte {
	return e.buf
}

func Frame(payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(payload)))
	copy(out[4:], payload)
	return out
}
