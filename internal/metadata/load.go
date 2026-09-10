package metadata

import (
	"errors"
	"io/fs"
	"os"

	"github.com/kahvecikaan/kafka-broker-go/internal/protocol"
)

const (
	topicRecordType     int8 = 2
	partitionRecordType int8 = 3
)

func Load(path string) (*Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// No metadata log on disk: a valid empty cluster.
			return &Store{byName: map[string]*Topic{}, byID: map[UUID]*Topic{}}, nil
		}
		return nil, err
	}
	d := protocol.NewDecoder(data)

	topics := map[UUID]*Topic{}
	for d.Remaining() > 0 && d.Err() == nil {
		parseRecordBatch(d, topics)
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return buildStore(topics), nil
}

func parseRecordBatch(d *protocol.Decoder, topics map[UUID]*Topic) {
	d.ReadInt64() // skip base offset
	d.ReadInt32() // skip batch length
	d.ReadInt32() // skip partition leader epoch
	d.ReadInt8()  // skip magic byte
	d.ReadInt32() // skip CRC
	d.ReadInt16() // skip attributes
	d.ReadInt32() // skip last offset delta
	d.ReadInt64() // skip base timestamp
	d.ReadInt64() // skip max timestamp
	d.ReadInt64() // skip producer ID
	d.ReadInt16() // skip producer epoch
	d.ReadInt32() // skip base seq.

	n := d.ReadInt32() // records len
	for i := int32(0); i < n; i++ {
		parseRecord(d, topics)
	}
}

func parseRecord(d *protocol.Decoder, topics map[UUID]*Topic) {
	d.ReadVarint()           // skip length
	d.ReadInt8()             // skip attributes
	d.ReadVarint()           // skip timestamp delta
	d.ReadVarint()           // skip offset delta
	keyLen := d.ReadVarint() // -1 if null
	if keyLen > 0 {
		d.ReadRawBytes(int(keyLen)) // skip key if present
	}

	valueLen := d.ReadVarint()
	value := d.ReadRawBytes(int(valueLen))

	d.ReadVarint() // skip header count

	// value parsing with a sub-decoder
	vd := protocol.NewDecoder(value)
	vd.ReadInt8() // skip frame version
	recordType := vd.ReadInt8()
	vd.ReadInt8() // skip version

	switch recordType {
	case topicRecordType:
		parseTopicRecord(vd, topics)
	case partitionRecordType:
		parsePartitionRecord(vd, topics)
	}
}

func parseTopicRecord(vd *protocol.Decoder, topics map[UUID]*Topic) {
	name := vd.ReadCompactString()
	id := readUUID(vd)
	topics[id] = &Topic{
		Name:       name,
		ID:         id,
		Partitions: nil,
	}
}

func parsePartitionRecord(vd *protocol.Decoder, topics map[UUID]*Topic) {
	partitionID := vd.ReadInt32()
	topicID := readUUID(vd)
	replicas := readCompactInt32Array(vd)
	isr := readCompactInt32Array(vd)
	readCompactInt32Array(vd) // skip removing replicas
	readCompactInt32Array(vd) // skip adding replicas
	leaderID := vd.ReadInt32()
	leaderEpoch := vd.ReadInt32()

	part := Partition{
		ID:          partitionID,
		LeaderID:    leaderID,
		LeaderEpoch: leaderEpoch,
		Replicas:    replicas,
		ISR:         isr,
	}

	// Attach the partition to its topic, found by the shared topic UUID.
	if t, ok := topics[topicID]; ok {
		t.Partitions = append(t.Partitions, part)
	}
}

func readCompactInt32Array(d *protocol.Decoder) []int32 {
	n := int(d.ReadUvarint()) - 1 // COMPACT_ARRAY length is N+1
	out := make([]int32, 0, max(n, 0))
	for i := 0; i < n; i++ {
		out = append(out, d.ReadInt32())
	}

	return out
}

func readUUID(d *protocol.Decoder) UUID {
	var u UUID
	copy(u[:], d.ReadRawBytes(16))
	return u
}

func buildStore(topics map[UUID]*Topic) *Store {
	byName := make(map[string]*Topic)
	byID := make(map[UUID]*Topic)
	for _, t := range topics {
		byName[t.Name] = t
		byID[t.ID] = t
	}

	return &Store{byName: byName, byID: byID}
}
