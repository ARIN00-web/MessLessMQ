package protocol

const (
	ErrorNone           uint16 = 0
	ErrorUnknownAPI     uint16 = 1
	ErrorInvalidRequest uint16 = 2
	ErrorInternal       uint16 = 3
)

type Response struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	ErrorCode     uint16
	Payload       []byte
}