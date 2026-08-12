package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

const (
	APIVersionsKey        int16 = 18
	errNone               int16 = 0
	errUnsupportedVersion int16 = 35
)

type ApiVersionsResponse struct {
	ErrorCode int16
}

func (r ApiVersionsResponse) Encode(e *protocol.Encoder) {
	e.PutInt16(r.ErrorCode)
}

func HandleApiVersions(header RequestHeader) ApiVersionsResponse {
	if header.APIVersion > 4 {
		return ApiVersionsResponse{errUnsupportedVersion}
	}

	return ApiVersionsResponse{errNone}
}
