package kafka

import "github.com/kahvecikaan/kafka-broker-go/internal/protocol"

func HandleRequest(msg []byte) ([]byte, error) {
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

	if err := d.Err(); err != nil {
		return nil, err // malformed/truncated request: don't send partial garbage
	}

	return protocol.Frame(e.Bytes()), nil
}
