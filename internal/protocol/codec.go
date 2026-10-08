package protocol

import (
	"encoding/binary"
	"fmt"
)

const headerSize = 8 

func EncodeRequest(req Request) ([]byte, error) {
	if req.APIKey == 0 {
		return nil, fmt.Errorf("invalid API key: 0")
	}

	data := make([]byte, headerSize+len(req.Payload))

	binary.BigEndian.PutUint16(data[0:2], req.APIKey)
	binary.BigEndian.PutUint16(data[2:4], req.APIVersion)
	binary.BigEndian.PutUint32(data[4:8], req.CorrelationID)

	copy(data[8:], req.Payload)

	return data, nil
}

func DecodeRequest(data []byte) (Request, error) {
	if len(data) < headerSize {
		return Request{}, fmt.Errorf(
			"request too short: got %d bytes, need at least %d",
			len(data),
			headerSize,
		)
	}

	req := Request{
		APIKey:        binary.BigEndian.Uint16(data[0:2]),
		APIVersion:    binary.BigEndian.Uint16(data[2:4]),
		CorrelationID: binary.BigEndian.Uint32(data[4:8]),
		Payload:       append([]byte(nil), data[8:]...),
	}

	if req.APIKey == 0 {
		return Request{}, fmt.Errorf("invalid API key: 0")
	}

	return req, nil
}
