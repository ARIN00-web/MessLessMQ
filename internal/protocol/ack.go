package protocol

import "fmt"

// AckMode specifies the producer's requested acknowledgement policy.
type AckMode int16

const (
	AckNone   AckMode = 0
	AckLeader AckMode = 1
	AckAll    AckMode = -1
)

func (a AckMode) Validate() error {
	switch a {
	case AckNone, AckLeader, AckAll:
		return nil
	default:
		return fmt.Errorf("invalid acknowledgement mode: %d", a)
	}
}

func (a AckMode) String() string {
	switch a {
	case AckNone:
		return "none"
	case AckLeader:
		return "leader"
	case AckAll:
		return "all"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}
