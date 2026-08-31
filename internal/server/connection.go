package server

import (
	"encoding/binary"
	"io"
	"log"
	"net"

	"github.com/kahvecikaan/kafka-broker-go/internal/kafka"
)

// maxRequestSize caps how large a single request may claim to be, so a client
// can't force an unbounded allocation via the message_size field. Mirrors
// Kafka's socket.request.max.bytes default (100 MiB).
const maxRequestSize = 100 << 20

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// read the 4-byte message_size
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			return
		}

		// read exactly 'size' bytes - the header+body
		size := binary.BigEndian.Uint32(sizeBuf)
		if size > maxRequestSize {
			return
		}
		msg := make([]byte, size)
		if _, err := io.ReadFull(conn, msg); err != nil {
			return
		}

		out, err := kafka.HandleRequest(msg)
		if err != nil {
			log.Println("malformed request, closing connection:", err)
			return
		}

		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

func Run(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}
