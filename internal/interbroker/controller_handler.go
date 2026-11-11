package interbroker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

// ControllerHandler exposes the Phase 15 controller's Raft request handlers
// over the Phase 16 peer transport. Persistent Raft state and elections are
// still managed by the controller implementation.
type ControllerHandler struct{ Node *controller.Node }

func NewControllerHandler(node *controller.Node) (*ControllerHandler, error) {
	if node == nil {
		return nil, controller.ErrInvalidConfig
	}
	return &ControllerHandler{Node: node}, nil
}

func (h *ControllerHandler) Handle(_ context.Context, request Envelope) (Envelope, error) {
	if h == nil || h.Node == nil {
		return Envelope{}, controller.ErrInvalidConfig
	}
	if request.IsResponse || request.TargetID != h.Node.ID() {
		return Envelope{}, ErrInvalidEnvelope
	}
	var payload any
	switch request.Type {
	case MessageVoteRequest:
		var vote controller.VoteRequest
		if err := json.Unmarshal(request.Payload, &vote); err != nil {
			return Envelope{}, err
		}
		if vote.CandidateID != request.SourceID {
			return Envelope{}, fmt.Errorf("vote candidate does not match peer identity")
		}
		payload = h.Node.HandleVoteRequest(vote)
	case MessageAppendEntries:
		var appendRequest controller.AppendEntriesRequest
		if err := json.Unmarshal(request.Payload, &appendRequest); err != nil {
			return Envelope{}, err
		}
		if appendRequest.LeaderID != request.SourceID {
			return Envelope{}, fmt.Errorf("append leader does not match peer identity")
		}
		payload = h.Node.HandleAppendEntries(appendRequest)
	default:
		return Envelope{}, fmt.Errorf("unsupported controller message type %d", request.Type)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Payload: encoded}, nil
}

func EncodeControllerPayload(value any) ([]byte, error)    { return json.Marshal(value) }
func DecodeControllerPayload(data []byte, value any) error { return json.Unmarshal(data, value) }

// DurableControllerHandler persists term, vote, log, and commit state before
// returning Raft RPC responses. OnLeaderContact is called after a valid
// AppendEntries has been accepted, allowing the runtime to reset its election
// timer.
type DurableControllerHandler struct {
	Node            *controller.DurableNode
	OnLeaderContact func()
}

func NewDurableControllerHandler(node *controller.DurableNode, onLeaderContact func()) (*DurableControllerHandler, error) {
	if node == nil {
		return nil, controller.ErrInvalidConfig
	}
	return &DurableControllerHandler{Node: node, OnLeaderContact: onLeaderContact}, nil
}

func (h *DurableControllerHandler) Handle(_ context.Context, request Envelope) (Envelope, error) {
	if h == nil || h.Node == nil || request.IsResponse || request.TargetID != h.Node.ID() {
		return Envelope{}, ErrInvalidEnvelope
	}
	var payload any
	switch request.Type {
	case MessageVoteRequest:
		var vote controller.VoteRequest
		if err := json.Unmarshal(request.Payload, &vote); err != nil {
			return Envelope{}, err
		}
		if vote.CandidateID != request.SourceID {
			return Envelope{}, fmt.Errorf("vote candidate does not match peer identity")
		}
		response, err := h.Node.HandleVoteRequest(vote)
		if err != nil {
			return Envelope{}, err
		}
		payload = response
	case MessageAppendEntries:
		var appendRequest controller.AppendEntriesRequest
		if err := json.Unmarshal(request.Payload, &appendRequest); err != nil {
			return Envelope{}, err
		}
		if appendRequest.LeaderID != request.SourceID {
			return Envelope{}, fmt.Errorf("append leader does not match peer identity")
		}
		response, err := h.Node.HandleAppendEntries(appendRequest)
		if err != nil {
			return Envelope{}, err
		}
		if response.Success && h.OnLeaderContact != nil {
			h.OnLeaderContact()
		}
		payload = response
	default:
		return Envelope{}, fmt.Errorf("unsupported controller message type %d", request.Type)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Payload: encoded}, nil
}
