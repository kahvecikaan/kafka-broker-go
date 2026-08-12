package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buff := make([]byte, 1024)
	n, err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}

	if n < 12 {
		fmt.Println("Received request is too short: ", n)
		return
	}

	// Extract the correlation id from the request
	// 0-3 [message_size - 4 bytes]
	// 4-5 [request_api_key - 2 bytes]
	// 6-7 [request_api_version - 2 bytes]
	// 8-11 [correlation_id - 4 bytes]
	apiVer := binary.BigEndian.Uint32(buff[6:7])
	correlationId := binary.BigEndian.Uint32(buff[8:12])

	// 10 byte response
	response := make([]byte, 10)
	binary.BigEndian.PutUint32(response[0:4], 0)
	binary.BigEndian.PutUint32(response[4:8], correlationId)

	if apiVer > 4 {
		binary.BigEndian.PutUint16(response[8:10], 35)
	} else {
		binary.BigEndian.PutUint16(response[8:10], 0)
	}

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		return
	}
}
