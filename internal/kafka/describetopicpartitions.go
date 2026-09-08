package kafka

import (
	"github.com/kahvecikaan/kafka-broker-go/internal/metadata"
	"github.com/kahvecikaan/kafka-broker-go/internal/protocol"
)

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
	ErrorCode    ErrorCode
	Index        int32
	LeaderID     int32
	LeaderEpoch  int32
	ReplicaNodes []int32
	IsrNodes     []int32
}

// HandleDescribeTopicPartitions parses the requested topic names from the body
// and answers each from cluster metadata: real data if the topic exists,
// otherwise UNKNOWN_TOPIC_OR_PARTITION. The decoder's cursor is already
// positioned past the request header.
func HandleDescribeTopicPartitions(d *protocol.Decoder, store *metadata.Store) DescribeTopicPartitionsResponse {
	topicCount := int(d.ReadUvarint()) - 1 // topics COMPACT_ARRAY length is N+1

	var topics []TopicResponse
	for i := 0; i < topicCount; i++ {
		name := d.ReadCompactString()
		d.ReadUvarint() // per-topic TAG_BUFFER

		topics = append(topics, describeTopic(name, store))
	}

	return DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics:         topics,
		NextCursor:     -1, // null
	}
}

// describeTopic builds one topic entry: real metadata if the topic exists,
// otherwise an unknown-topic placeholder.
func describeTopic(name string, store *metadata.Store) TopicResponse {
	t, ok := store.FindTopic(name)
	if !ok {
		return TopicResponse{
			ErrorCode:  errUnknownTopic,
			Name:       name,
			TopicID:    [16]byte{}, // all zeros
			IsInternal: false,
			Partitions: nil,
		}
	}

	partitions := make([]PartitionResponse, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		partitions = append(partitions, PartitionResponse{
			ErrorCode:    errNone,
			Index:        p.ID,
			LeaderID:     p.LeaderID,
			LeaderEpoch:  p.LeaderEpoch,
			ReplicaNodes: p.Replicas,
			IsrNodes:     p.ISR,
		})
	}

	return TopicResponse{
		ErrorCode:  errNone,
		Name:       t.Name,
		TopicID:    t.ID,
		IsInternal: false,
		Partitions: partitions,
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
	e.PutUvarint(uint64(len(t.Partitions) + 1)) // partitions COMPACT_ARRAY
	for _, p := range t.Partitions {
		p.Encode(e)
	}
	e.PutInt32(t.AuthorizedOperations)
	e.PutUvarint(0) // TAG_BUFFER
}

func (p PartitionResponse) Encode(e *protocol.Encoder) {
	e.PutInt16(int16(p.ErrorCode))
	e.PutInt32(p.Index)
	e.PutInt32(p.LeaderID)
	e.PutInt32(p.LeaderEpoch)
	putCompactInt32Array(e, p.ReplicaNodes)
	putCompactInt32Array(e, p.IsrNodes)
	putCompactInt32Array(e, nil) // eligible_leader_replicas (empty)
	putCompactInt32Array(e, nil) // last_known_elr (empty)
	putCompactInt32Array(e, nil) // offline_replicas (empty)
	e.PutUvarint(0)              // TAG_BUFFER
}

func putCompactInt32Array(e *protocol.Encoder, arr []int32) {
	e.PutUvarint(uint64(len(arr) + 1)) // COMPACT_ARRAY length is N+1
	for _, v := range arr {
		e.PutInt32(v)
	}
}
