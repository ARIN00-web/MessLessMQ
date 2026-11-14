package protocol

import (
	"encoding/json"
	"fmt"
)

const GroupAPIVersion uint16 = 1

const (
	GroupJoin        = "join"
	GroupHeartbeat   = "heartbeat"
	GroupLeave       = "leave"
	GroupFetch       = "fetch"
	GroupCommit      = "commit_offset"
	GroupFetchOffset = "fetch_offset"
)

// GroupRequest is carried inside the versioned binary TCP frame. JSON is used
// only for this administrative-style payload so fields can evolve without
// changing the outer transport framing; operation and API version are always
// validated before dispatch.
type GroupRequest struct {
	Operation   string   `json:"operation"`
	GroupID     string   `json:"group_id"`
	MemberID    string   `json:"member_id,omitempty"`
	Generation  uint64   `json:"generation,omitempty"`
	Topics      []string `json:"topics,omitempty"`
	Topic       string   `json:"topic,omitempty"`
	PartitionID uint32   `json:"partition_id,omitempty"`
	Offset      uint64   `json:"offset,omitempty"`
	MaxMessages uint32   `json:"max_messages,omitempty"`
	Metadata    string   `json:"metadata,omitempty"`
}

type GroupAssignment struct {
	Topic       string `json:"topic"`
	PartitionID uint32 `json:"partition_id"`
}
type GroupResponse struct {
	GroupID     string            `json:"group_id"`
	MemberID    string            `json:"member_id,omitempty"`
	Generation  uint64            `json:"generation"`
	Assignments []GroupAssignment `json:"assignments,omitempty"`
	Offset      uint64            `json:"offset,omitempty"`
	HasOffset   bool              `json:"has_offset,omitempty"`
	Fetch       *ConsumeResponse  `json:"fetch,omitempty"`
}

func EncodeGroupRequest(request GroupRequest) ([]byte, error) {
	if err := validateGroupRequest(request); err != nil {
		return nil, err
	}
	return json.Marshal(request)
}
func DecodeGroupRequest(data []byte) (GroupRequest, error) {
	var request GroupRequest
	if len(data) == 0 || json.Unmarshal(data, &request) != nil {
		return GroupRequest{}, ErrInvalidRequest
	}
	return request, validateGroupRequest(request)
}
func EncodeGroupResponse(response GroupResponse) ([]byte, error) {
	if response.GroupID == "" {
		return nil, ErrInvalidRequest
	}
	return json.Marshal(response)
}
func DecodeGroupResponse(data []byte) (GroupResponse, error) {
	var response GroupResponse
	if len(data) == 0 || json.Unmarshal(data, &response) != nil || response.GroupID == "" {
		return GroupResponse{}, ErrInvalidRequest
	}
	return response, nil
}

func validateGroupRequest(request GroupRequest) error {
	if request.GroupID == "" || len(request.GroupID) > 128 || len(request.MemberID) > 128 || len(request.Topics) > 1024 || len(request.Metadata) > 1024 {
		return ErrInvalidRequest
	}
	switch request.Operation {
	case GroupJoin:
		if request.MemberID == "" || len(request.Topics) == 0 {
			return ErrInvalidRequest
		}
	case GroupHeartbeat, GroupLeave:
		if request.MemberID == "" || request.Generation == 0 {
			return ErrInvalidRequest
		}
	case GroupFetch:
		if request.MemberID == "" || request.Generation == 0 || request.Topic == "" || request.MaxMessages == 0 || request.MaxMessages > MaxConsumeMessages {
			return ErrInvalidRequest
		}
	case GroupCommit:
		if request.MemberID == "" || request.Generation == 0 || request.Topic == "" {
			return ErrInvalidRequest
		}
	case GroupFetchOffset:
		if request.Topic == "" {
			return ErrInvalidRequest
		}
	default:
		return fmt.Errorf("unknown group operation: %w", ErrInvalidRequest)
	}
	return nil
}
