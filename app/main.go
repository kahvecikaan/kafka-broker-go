package main

import (
	"log"

	"github.com/kahvecikaan/kafka-broker-go/internal/kafka"
	"github.com/kahvecikaan/kafka-broker-go/internal/metadata"
	"github.com/kahvecikaan/kafka-broker-go/internal/server"
)

const logDir = "/tmp/kraft-combined-logs/"
const metadataLogPath = logDir + "__cluster_metadata-0/00000000000000000000.log"

func main() {
	store, err := metadata.Load(metadataLogPath)
	if err != nil {
		log.Fatal("Failed to load cluster metadata: ", err)
	}

	broker := kafka.NewBroker(logDir, store)

	if err := server.Run("0.0.0.0:9092", broker); err != nil {
		log.Fatal("Failed to run the server: ", err)
	}
}
