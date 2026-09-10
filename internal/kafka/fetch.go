package kafka

import (
	"github.com/kahvecikaan/kafka-broker-go/internal/metadata"
	"github.com/kahvecikaan/kafka-broker-go/internal/protocol"
)

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      ErrorCode
	SessionID      int32
	Responses      []FetchableTopicResponse // empty this stage
}

type FetchPartitionResponse struct {
	Index                int32
	ErrorCode            ErrorCode
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	PreferredReadReplica int32
}

type FetchableTopicResponse struct {
	TopicID    metadata.UUID
	Partitions []FetchPartitionResponse
}

func (p FetchPartitionResponse) Encode(e *protocol.Encoder) {
	e.PutInt32(p.Index)
	e.PutInt16(int16(p.ErrorCode))
	e.PutInt64(p.HighWatermark)
	e.PutInt64(p.LastStableOffset)
	e.PutInt64(p.LogStartOffset)
	e.PutUvarint(1) // aborted_transactions: empty compact array
	e.PutInt32(p.PreferredReadReplica)
	e.PutUvarint(0) // records: COMPACT_RECORDS null (0 = null)
	e.PutUvarint(0) // TAG_BUFFER

}

func (t FetchableTopicResponse) Encode(e *protocol.Encoder) {
	e.PutRawBytes(t.TopicID[:])
	e.PutUvarint(uint64(len(t.Partitions) + 1))
	for _, p := range t.Partitions {
		p.Encode(e)
	}
	e.PutUvarint(0) // TAG_BUFFER
}

func HandleFetch(d *protocol.Decoder) FetchResponse {
	d.ReadInt32() // skip max_wait_ms
	d.ReadInt32() // skip min_bytes
	d.ReadInt32() // skip max_bytes
	d.ReadInt8()  // skip isolation_level
	d.ReadInt32() // skip session_id
	d.ReadInt32() // skip session_epoch

	topicCount := int(d.ReadUvarint()) - 1 // topics COMPACT_ARRAY is N+1
	responses := make([]FetchableTopicResponse, 0, max(topicCount, 0))
	for i := 0; i < topicCount; i++ {
		responses = append(responses, fetchTopic(d))
	}

	return FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      errNone, // top-level: no error (per-partition errors carry the detail)
		SessionID:      0,
		Responses:      responses,
	}
}

// fetchTopic reads one requested topic and, treating it as unknown, returns an
// entry with UNKNOWN_TOPIC_ID for each requested partition.
func fetchTopic(d *protocol.Decoder) FetchableTopicResponse {
	var topicID metadata.UUID
	copy(topicID[:], d.ReadRawBytes(16))

	partitionCount := int(d.ReadUvarint()) - 1 // partitions COMPACT_ARRAY is N+1
	partitions := make([]FetchPartitionResponse, 0, max(partitionCount, 0))
	for i := 0; i < partitionCount; i++ {
		partitions = append(partitions, FetchPartitionResponse{
			Index:                readPartitionIndex(d),
			ErrorCode:            errUnknownTopicID,
			PreferredReadReplica: -1,
		})
	}
	d.ReadUvarint() // topic-level TAG_BUFFER

	return FetchableTopicResponse{
		TopicID:    topicID,
		Partitions: partitions,
	}
}

// readPartitionIndex consumes one requested-partition entry and returns its
// partition index. The other fields (fetch_offset, etc.) aren't needed yet, so
// they're read only to advance the cursor to the next entry.
func readPartitionIndex(d *protocol.Decoder) int32 {
	partitionIdx := d.ReadInt32()
	d.ReadInt32()   // skip current_leader_epoch
	d.ReadInt64()   // skip fetch_offset
	d.ReadInt32()   // skip last_fetched_epoch
	d.ReadInt64()   // skip log_start_offset
	d.ReadInt32()   // skip partition_max_bytes
	d.ReadUvarint() // skip TAG_BUFFER

	return partitionIdx
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
