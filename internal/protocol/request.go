package protocol

const (
	APIPRODUCE uint16 = 1
	APICONSUME uint16 = 2
	APIGROUP   uint16 = 4
	APIAUTH    uint16 = 5
)

type Request struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	Payload       []byte
}
