package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

func HandleRequest(msg []byte) []byte {
	d := protocol.NewDecoder(msg)

	reqHeader := ParseRequestHeader(d)

	e := protocol.NewEncoder()
	respHeader := NewResponseHeader(reqHeader)
	respHeader.Encode(e)

	switch reqHeader.APIKey {
	case APIVersionsKey:
		resp := HandleApiVersions(reqHeader)
		resp.Encode(e)
	}

	return protocol.Frame(e.Bytes())
}
