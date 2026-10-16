package protocol

import "errors"

var (
	ErrInvalidRequest  = errors.New("invalid request")
	ErrInvalidAckMode  = errors.New("invalid acknowledgement mode")
	ErrMessageTooLarge = errors.New("message or batch too large")
)
