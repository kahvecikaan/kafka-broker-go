package server

import (
	"encoding/binary"
	"io"
	"log"
	"net"

	"github.com/kahvecikaan/kafka-broker-go/internal/kafka"
)

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
		msg := make([]byte, size)
		if _, err := io.ReadFull(conn, msg); err != nil {
			return
		}

		out := kafka.HandleRequest(msg)

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
