package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/metadata"

// Broker holds the shared state a request handler needs. It is built once at
// startup and read concurrently by every connection goroutine.
type Broker struct {
	logDir   string
	metadata *metadata.Store
}

func NewBroker(logDir string, store *metadata.Store) *Broker {
	return &Broker{
		logDir:   logDir,
		metadata: store,
	}
}
