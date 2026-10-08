package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	requestHeaderSize  = 8
	responseHeaderSize = 10
)

func EncodeRequest(req Request) ([]byte, error) {
	if req.APIKey == 0 {
		return nil, fmt.Errorf("invalid API key: 0")
	}

	data := make([]byte, requestHeaderSize+len(req.Payload))

	binary.BigEndian.PutUint16(data[0:2], req.APIKey)
	binary.BigEndian.PutUint16(data[2:4], req.APIVersion)
	binary.BigEndian.PutUint32(data[4:8], req.CorrelationID)

	copy(data[requestHeaderSize:], req.Payload)

	return data, nil
}

func DecodeRequest(data []byte) (Request, error) {
	if len(data) < requestHeaderSize {
		return Request{}, fmt.Errorf(
			"request too short: got %d bytes, need at least %d",
			len(data),
			requestHeaderSize,
		)
	}

	req := Request{
		APIKey:        binary.BigEndian.Uint16(data[0:2]),
		APIVersion:    binary.BigEndian.Uint16(data[2:4]),
		CorrelationID: binary.BigEndian.Uint32(data[4:8]),
		Payload:       append([]byte(nil), data[requestHeaderSize:]...),
	}

	if req.APIKey == 0 {
		return Request{}, fmt.Errorf("invalid API key: 0")
	}

	return req, nil
}

func EncodeResponse(resp Response) ([]byte, error) {
	if resp.APIKey == 0 {
		return nil, fmt.Errorf("invalid API key: 0")
	}

	data := make([]byte, responseHeaderSize+len(resp.Payload))

	binary.BigEndian.PutUint16(data[0:2], resp.APIKey)
	binary.BigEndian.PutUint16(data[2:4], resp.APIVersion)
	binary.BigEndian.PutUint32(data[4:8], resp.CorrelationID)
	binary.BigEndian.PutUint16(data[8:10], resp.ErrorCode)

	copy(data[responseHeaderSize:], resp.Payload)

	return data, nil
}

func DecodeResponse(data []byte) (Response, error) {
	if len(data) < responseHeaderSize {
		return Response{}, fmt.Errorf(
			"response too short: got %d bytes, need at least %d",
			len(data),
			responseHeaderSize,
		)
	}

	resp := Response{
		APIKey:        binary.BigEndian.Uint16(data[0:2]),
		APIVersion:    binary.BigEndian.Uint16(data[2:4]),
		CorrelationID: binary.BigEndian.Uint32(data[4:8]),
		ErrorCode:     binary.BigEndian.Uint16(data[8:10]),
		Payload:       append([]byte(nil), data[responseHeaderSize:]...),
	}

	return resp, nil
}
