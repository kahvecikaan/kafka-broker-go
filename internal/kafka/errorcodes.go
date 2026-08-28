package kafka

type ErrorCode int16

const (
	errNone               ErrorCode = 0
	errUnsupportedVersion ErrorCode = 35
	errUnknownTopic       ErrorCode = 3
)
