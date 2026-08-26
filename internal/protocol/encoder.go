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

func (e *Encoder) Bytes() []byte {
	return e.buf
}

func Frame(payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(payload)))
	copy(out[4:], payload)
	return out
}
