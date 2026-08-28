package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

func HandleRequest(msg []byte) []byte {
	d := protocol.NewDecoder(msg)

	reqHeader := ParseRequestHeader(d)

	e := protocol.NewEncoder()
	respHeader := NewResponseHeader(reqHeader)

	switch reqHeader.APIKey {
	case APIVersionsKey:
		respHeader.EncodeV0(e) // ApiVersions uses response header v0
		HandleApiVersions(reqHeader).Encode(e)
	case DescribeTopicPartitionsKey:
		respHeader.EncodeV1(e) // DescribeTopicPartitions uses response header v1
		HandleDescribeTopicPartitions(d).Encode(e)
	}

	return protocol.Frame(e.Bytes())
}
