package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientId      string
}

type ResponseHeader struct {
	CorrelationID int32
}

func ParseRequestHeader(d *protocol.Decoder) RequestHeader {
	h := RequestHeader{
		APIKey:        d.ReadInt16(),
		APIVersion:    d.ReadInt16(),
		CorrelationID: d.ReadInt32(),
		ClientId:      d.ReadNullableString(),
	}
	d.ReadUvarint() // consume header TAG_BUFFER
	return h
}

func (h ResponseHeader) EncodeV0(e *protocol.Encoder) {
	e.PutInt32(h.CorrelationID) // vO header = ONLY correlation_id
}

func (h ResponseHeader) EncodeV1(e *protocol.Encoder) {
	h.EncodeV0(e)
	e.PutUvarint(0) // v0 header + TAG_BUFFER
}

func NewResponseHeader(reqHeader RequestHeader) ResponseHeader {
	return ResponseHeader{CorrelationID: reqHeader.CorrelationID}
}
