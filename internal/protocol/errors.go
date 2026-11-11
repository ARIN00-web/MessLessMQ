package protocol

import "errors"

var (
	ErrInvalidRequest     = errors.New("invalid request")
	ErrInvalidAckMode     = errors.New("invalid acknowledgement mode")
	ErrMessageTooLarge    = errors.New("message or batch too large")
	ErrInvalidFetchLimit  = errors.New("invalid fetch limit")
	ErrUnsupportedVersion = errors.New("unsupported API version")
	ErrRateLimited        = errors.New("request rate limit exceeded")
)
