package protocol

const (
	ErrorNone               uint16 = 0
	ErrorUnknownAPI         uint16 = 1
	ErrorInvalidRequest     uint16 = 2
	ErrorInternal           uint16 = 3
	ErrorInvalidAckMode     uint16 = 4
	ErrorMessageTooLarge    uint16 = 5
	ErrorUnknownTopic       uint16 = 6
	ErrorPartitionNotFound  uint16 = 7
	ErrorOffsetOutOfRange   uint16 = 8
	ErrorOffsetBeforeStart  uint16 = 9
	ErrorInvalidFetchLimit  uint16 = 10
	ErrorUnsupportedVersion uint16 = 11
	ErrorRateLimited        uint16 = 12
)

type Response struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	ErrorCode     uint16
	Payload       []byte
}
