package broker

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/group"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

type GroupHandler struct{ coordinator *group.Coordinator }

func NewGroupHandler(coordinator *group.Coordinator) *GroupHandler {
	return &GroupHandler{coordinator: coordinator}
}

func (h *GroupHandler) Handle(request protocol.Request) ([]byte, error) {
	if h == nil || h.coordinator == nil {
		return nil, ErrHandlerUnavailable
	}
	if request.APIKey != protocol.APIGROUP || request.APIVersion != protocol.GroupAPIVersion {
		return nil, protocol.ErrUnsupportedVersion
	}
	req, err := protocol.DecodeGroupRequest(request.Payload)
	if err != nil {
		return nil, err
	}
	response := protocol.GroupResponse{GroupID: req.GroupID, MemberID: req.MemberID}
	switch req.Operation {
	case protocol.GroupJoin:
		joined, err := h.coordinator.JoinGroup(req.GroupID, req.MemberID, req.Topics)
		if err != nil {
			return nil, err
		}
		response.Generation, response.Assignments = joined.Generation, groupAssignments(joined.Assignments)
	case protocol.GroupHeartbeat:
		if err := h.coordinator.Heartbeat(req.GroupID, req.MemberID, req.Generation); err != nil {
			return nil, err
		}
		response.Generation = req.Generation
	case protocol.GroupLeave:
		generation, err := h.coordinator.LeaveGroup(req.GroupID, req.MemberID)
		if err != nil {
			return nil, err
		}
		response.Generation = generation
	case protocol.GroupCommit:
		if err := h.coordinator.CommitOffsetWithMetadata(req.GroupID, req.MemberID, req.Generation, req.Topic, req.PartitionID, req.Offset, req.Metadata); err != nil {
			return nil, err
		}
		response.Generation, response.Offset, response.HasOffset = req.Generation, req.Offset, true
	case protocol.GroupFetchOffset:
		commit, exists, err := h.coordinator.CommittedOffset(req.GroupID, req.Topic, req.PartitionID)
		if err != nil {
			return nil, err
		}
		response.Offset, response.HasOffset = commit.NextOffset, exists
	case protocol.GroupFetch:
		fetched, err := h.coordinator.Fetch(req.GroupID, req.MemberID, req.Generation, req.Topic, req.PartitionID, req.MaxMessages)
		if err != nil {
			return nil, err
		}
		response.Generation = req.Generation
		response.Fetch = &protocol.ConsumeResponse{StartOffset: fetched.StartOffset, LogEndOffset: fetched.LogEndOffset, NextOffset: fetched.NextOffset, Records: make([]protocol.ConsumeRecord, len(fetched.Records))}
		for i, record := range fetched.Records {
			response.Fetch.Records[i] = protocol.ConsumeRecord{Offset: record.Offset, Timestamp: record.Timestamp, Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...)}
		}
	default:
		return nil, fmt.Errorf("unknown group operation: %w", protocol.ErrInvalidRequest)
	}
	return protocol.EncodeGroupResponse(response)
}

func groupAssignments(assignments []group.TopicPartition) []protocol.GroupAssignment {
	out := make([]protocol.GroupAssignment, len(assignments))
	for i, assignment := range assignments {
		out[i] = protocol.GroupAssignment{Topic: assignment.Topic, PartitionID: assignment.PartitionID}
	}
	return out
}
