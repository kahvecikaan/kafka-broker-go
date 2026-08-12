package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
}

type ResponseHeader struct {
	CorrelationID int32
}

func ParseRequestHeader(d *protocol.Decoder) RequestHeader {
	return RequestHeader{
		APIKey:        d.ReadInt16(),
		APIVersion:    d.ReadInt16(),
		CorrelationID: d.ReadInt32(),
	}
}

func (h ResponseHeader) Encode(e *protocol.Encoder) {
	e.PutInt32(h.CorrelationID) // vO header = ONLY correlation_id
}

func NewResponseHeader(reqHeader RequestHeader) ResponseHeader {
	return ResponseHeader{CorrelationID: reqHeader.CorrelationID}
}
