package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      ErrorCode
	SessionID      int32
	Responses      []FetchableTopicResponse // empty this stage
}

type FetchableTopicResponse struct {
	// placeholder for now
}

func (r FetchableTopicResponse) Encode(e *protocol.Encoder) {
	// placeholder for now
}

func HandleFetch() FetchResponse {
	return FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      errNone,
		SessionID:      0,
		Responses:      nil,
	}
}

func (r FetchResponse) Encode(e *protocol.Encoder) {
	e.PutInt32(r.ThrottleTimeMs)
	e.PutInt16(int16(r.ErrorCode))
	e.PutInt32(r.SessionID)
	e.PutUvarint(uint64(len(r.Responses) + 1)) // responses COMPACT_ARRAY (empty -> 1)
	for _, resp := range r.Responses {
		resp.Encode(e)
	}
	e.PutUvarint(0) // TAG_BUFFER
}
