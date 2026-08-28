package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

type ApiKeyEntry struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func (k ApiKeyEntry) Encode(e *protocol.Encoder) {
	e.PutInt16(k.APIKey)
	e.PutInt16(k.MinVersion)
	e.PutInt16(k.MaxVersion)
	e.PutUvarint(0) // TAG_BUFFER
}

type ApiVersionsResponse struct {
	ErrorCode      ErrorCode
	Keys           []ApiKeyEntry
	ThrottleTimeMs int32
}

func (r ApiVersionsResponse) Encode(e *protocol.Encoder) {
	e.PutInt16(int16(r.ErrorCode))
	e.PutUvarint(uint64(len(r.Keys) + 1))
	for _, key := range r.Keys {
		key.Encode(e)
	}
	e.PutInt32(r.ThrottleTimeMs)
	e.PutUvarint(0) // TAG_BUFFER
}

func HandleApiVersions(header RequestHeader) ApiVersionsResponse {
	if header.APIVersion > 4 {
		return ApiVersionsResponse{
			ErrorCode:      errUnsupportedVersion,
			Keys:           supportedApiKeys(),
			ThrottleTimeMs: 0,
		}
	}

	return ApiVersionsResponse{
		ErrorCode:      errNone,
		Keys:           supportedApiKeys(),
		ThrottleTimeMs: 0,
	}
}

func supportedApiKeys() []ApiKeyEntry {
	return []ApiKeyEntry{
		{APIKey: APIVersionsKey, MinVersion: 0, MaxVersion: 4},
		{APIKey: DescribeTopicPartitionsKey, MinVersion: 0, MaxVersion: 0},
	}
}
