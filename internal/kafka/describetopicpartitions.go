package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32
	Topics         []TopicResponse
	NextCursor     int8 // -1 means null (no pagination cursor)
}

type TopicResponse struct {
	ErrorCode            ErrorCode
	Name                 string
	TopicID              [16]byte // UUID; zero value = 00000000-...-000000000000
	IsInternal           bool
	Partitions           []PartitionResponse
	AuthorizedOperations int32
}

type PartitionResponse struct {
	// placeholder for now; partition metadata arrives in a later stage
}

// HandleDescribeTopicPartitions parses the topics from the request body and,
// treating every topic as unknown, echoes each name back with an
// UNKNOWN_TOPIC_OR_PARTITION error. The decoder's cursor is already positioned
// past the request header.
func HandleDescribeTopicPartitions(d *protocol.Decoder) DescribeTopicPartitionsResponse {
	topicCount := int(d.ReadUvarint()) - 1 // topics COMPACT_ARRAY length is N+1

	var topics []TopicResponse
	for i := 0; i < topicCount; i++ {
		name := d.ReadCompactString()
		d.ReadUvarint() // per-topic TAG_BUFFER

		topics = append(topics, TopicResponse{
			ErrorCode:            errUnknownTopic,
			Name:                 name,
			TopicID:              [16]byte{}, // all zeros
			IsInternal:           false,
			Partitions:           nil,
			AuthorizedOperations: 0,
		})
	}

	return DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		NextCursor:     -1, // null
	}
}

func (r DescribeTopicPartitionsResponse) Encode(e *protocol.Encoder) {
	e.PutInt32(r.ThrottleTimeMs)
	e.PutUvarint(uint64(len(r.Topics) + 1)) // topics COMPACT_ARRAY
	for _, t := range r.Topics {
		t.Encode(e)
	}
	e.PutInt8(r.NextCursor) // next_cursor (-1 = null)
	e.PutUvarint(0)         // TAG_BUFFER
}

func (t TopicResponse) Encode(e *protocol.Encoder) {
	e.PutInt16(int16(t.ErrorCode))
	e.PutCompactString(t.Name)
	e.PutRawBytes(t.TopicID[:])
	e.PutBool(t.IsInternal)
	e.PutUvarint(uint64(len(t.Partitions) + 1)) // partitions COMPACT_ARRAY (empty)
	e.PutInt32(t.AuthorizedOperations)
	e.PutUvarint(0) // TAG_BUFFER
}
