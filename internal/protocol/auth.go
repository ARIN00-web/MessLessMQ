package protocol

import (
	"encoding/binary"
	"fmt"
)

const AuthAPIVersion uint16 = 1

// AuthRequest establishes a principal for the current TCP connection. Tokens
// are intentionally carried only in this request and are never echoed.
type AuthRequest struct{ Token string }
type AuthResponse struct{ PrincipalID string }

func EncodeAuthRequest(request AuthRequest) ([]byte, error) {
	if request.Token == "" || len(request.Token) > 4096 {
		return nil, ErrInvalidRequest
	}
	data := make([]byte, 2+len(request.Token))
	binary.BigEndian.PutUint16(data[:2], uint16(len(request.Token)))
	copy(data[2:], request.Token)
	return data, nil
}
func DecodeAuthRequest(data []byte) (AuthRequest, error) {
	if len(data) < 2 {
		return AuthRequest{}, ErrInvalidRequest
	}
	length := int(binary.BigEndian.Uint16(data[:2]))
	if length == 0 || length > 4096 || len(data) != 2+length {
		return AuthRequest{}, ErrInvalidRequest
	}
	return AuthRequest{Token: string(data[2:])}, nil
}
func EncodeAuthResponse(response AuthResponse) ([]byte, error) {
	if response.PrincipalID == "" || len(response.PrincipalID) > 128 {
		return nil, ErrInvalidRequest
	}
	return EncodeAuthRequest(AuthRequest{Token: response.PrincipalID})
}
func DecodeAuthResponse(data []byte) (AuthResponse, error) {
	request, err := DecodeAuthRequest(data)
	if err != nil {
		return AuthResponse{}, err
	}
	if request.Token == "" {
		return AuthResponse{}, fmt.Errorf("empty principal: %w", ErrInvalidRequest)
	}
	return AuthResponse{PrincipalID: request.Token}, nil
}
