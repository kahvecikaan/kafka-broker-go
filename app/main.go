package main

import (
	"log"

	"github.com/kahvecikaan/kafka-broker-go/internal/server"
)

func main() {
	err := server.Run("0.0.0.0:9092")
	if err != nil {
		log.Fatal("Failed to run the server: ", err)
	}
}
