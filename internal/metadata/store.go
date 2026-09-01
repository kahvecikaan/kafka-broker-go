package metadata

// UUID is a Kafka identifier: a fixed 16-byte value.
type UUID [16]byte

type Topic struct {
	Name       string
	ID         UUID
	Partitions []Partition
}

type Partition struct {
	ID          int32
	LeaderID    int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

type Store struct {
	byName map[string]*Topic
}

func (s *Store) FindTopic(name string) (*Topic, bool) {
	t, ok := s.byName[name]
	return t, ok
}
