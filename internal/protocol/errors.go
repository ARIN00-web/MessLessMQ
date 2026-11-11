package protocol

import "errors"

var (
	ErrInvalidRequest     = errors.New("invalid request")
	ErrInvalidAckMode     = errors.New("invalid acknowledgement mode")
	ErrMessageTooLarge    = errors.New("message or batch too large")
	ErrInvalidFetchLimit  = errors.New("invalid fetch limit")
	ErrUnsupportedVersion = errors.New("unsupported API version")
	ErrRateLimited        = errors.New("request rate limit exceeded")
	ErrNotPartitionLeader = errors.New("request reached a broker that is not the partition leader")
	ErrInsufficientISR    = errors.New("not enough in-sync replicas acknowledged the append")
	ErrCommitUncertain    = errors.New("append committed on the leader but commit-marker quorum was not confirmed; outcome is uncertain")
)
