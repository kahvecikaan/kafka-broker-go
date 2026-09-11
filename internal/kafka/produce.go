package kafka

import (
	"github.com/kahvecikaan/kafka-broker-go/internal/metadata"
	"github.com/kahvecikaan/kafka-broker-go/internal/protocol"
	"github.com/kahvecikaan/kafka-broker-go/internal/storage"
)

type ProduceRequest struct {
	TransactionalID string
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceTopicData
}

type ProduceTopicData struct {
	Name       string
	Partitions []ProducePartitionData
}

type ProducePartitionData struct {
	Index   int32
	Records []byte
}

func (r *ProduceRequest) Decode(d *protocol.Decoder) {
	r.TransactionalID = d.ReadCompactString() // COMPACT_NULLABLE_STRING; "" when null
	r.Acks = d.ReadInt16()
	r.TimeoutMs = d.ReadInt32()

	topicCount := int(d.ReadUvarint()) - 1 // COMPACT_ARRAY len is N+1
	r.Topics = make([]ProduceTopicData, 0, max(topicCount, 0))
	for i := 0; i < topicCount; i++ {
		var t ProduceTopicData
		t.Decode(d)
		r.Topics = append(r.Topics, t)
	}
	d.ReadUvarint() // skip TAG_BUFFER
}

func (t *ProduceTopicData) Decode(d *protocol.Decoder) {
	t.Name = d.ReadCompactString()

	partitionCount := int(d.ReadUvarint()) - 1
	t.Partitions = make([]ProducePartitionData, 0, max(partitionCount, 0))
	for i := 0; i < partitionCount; i++ {
		var p ProducePartitionData
		p.Decode(d)
		t.Partitions = append(t.Partitions, p)
	}
	d.ReadUvarint() // skip TAG_BUFFER
}

func (p *ProducePartitionData) Decode(d *protocol.Decoder) {
	p.Index = d.ReadInt32()
	p.Records = d.ReadCompactBytes()
	d.ReadUvarint() // skip TAG_BUFFER
}

type ProduceResponse struct {
	Topics         []ProduceTopicResponse
	ThrottleTimeMs int32
}

type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Index           int32
	ErrorCode       ErrorCode
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
}

func (r ProduceResponse) Encode(e *protocol.Encoder) {
	e.PutUvarint(uint64(len(r.Topics)) + 1)
	for i := 0; i < len(r.Topics); i++ {
		r.Topics[i].Encode(e)
	}

	e.PutInt32(r.ThrottleTimeMs)
	e.PutUvarint(0) // TAG_BUFFER
}

func (t ProduceTopicResponse) Encode(e *protocol.Encoder) {
	e.PutCompactString(t.Name)
	e.PutUvarint(uint64(len(t.Partitions)) + 1)
	for i := 0; i < len(t.Partitions); i++ {
		t.Partitions[i].Encode(e)
	}
	e.PutUvarint(0) // TAG_BUFFER
}

func (p ProducePartitionResponse) Encode(e *protocol.Encoder) {
	e.PutInt32(p.Index)
	e.PutInt16(int16(p.ErrorCode))
	e.PutInt64(p.BaseOffset)
	e.PutInt64(p.LogAppendTimeMs)
	e.PutInt64(p.LogStartOffset)
	e.PutUvarint(1) // empty COMPACT_ARRAY for record_errors
	e.PutUvarint(0) // COMPACT_NULLABLE_STRING for error_message (null)
	e.PutUvarint(0) // TAG_BUFFER
}

func HandleProduce(d *protocol.Decoder, store *metadata.Store, logDir string) (ProduceResponse, error) {
	var req ProduceRequest
	req.Decode(d)

	topics := make([]ProduceTopicResponse, 0, len(req.Topics))
	for _, t := range req.Topics {
		partitions := make([]ProducePartitionResponse, 0, len(t.Partitions))
		topic, ok := store.FindTopic(t.Name)

		for _, p := range t.Partitions {
			if !ok || !topic.HasPartition(p.Index) {
				partitions = append(partitions, ProducePartitionResponse{
					Index:           p.Index,
					ErrorCode:       errUnknownTopic,
					BaseOffset:      -1,
					LogAppendTimeMs: -1,
					LogStartOffset:  -1,
				})
			} else {
				if err := storage.WritePartition(logDir, t.Name, p.Index, p.Records); err != nil {
					return ProduceResponse{}, err
				}

				partitions = append(partitions, ProducePartitionResponse{
					Index:           p.Index,
					ErrorCode:       errNone,
					BaseOffset:      0,
					LogAppendTimeMs: -1,
					LogStartOffset:  0,
				})
			}
		}

		topics = append(topics, ProduceTopicResponse{
			Name:       t.Name,
			Partitions: partitions,
		})
	}

	return ProduceResponse{
		Topics:         topics,
		ThrottleTimeMs: 0,
	}, nil
}
