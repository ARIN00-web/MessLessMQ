package protocol

const (
	APIPRODUCE uint16 = 1
	APICONSUME uint16 = 2
)

type Request struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	Payload       []byte
}