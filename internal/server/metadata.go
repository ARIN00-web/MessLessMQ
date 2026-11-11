package server

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/membership"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

type localMetadataHandler struct {
	brokerID string
	address  string
	registry *topic.Registry
	members  *membership.Registry
}

func (h *localMetadataHandler) Handle(request protocol.Request) ([]byte, error) {
	if h == nil || h.registry == nil || h.brokerID == "" || h.address == "" {
		return nil, fmt.Errorf("local metadata handler is not initialized: %w", protocol.ErrInvalidRequest)
	}
	if request.APIKey != protocol.APIMETADATA {
		return nil, fmt.Errorf("expected METADATA API key: %w", protocol.ErrInvalidRequest)
	}
	if request.APIVersion != protocol.MetadataAPIVersion {
		return nil, protocol.ErrUnsupportedVersion
	}
	metadataRequest, err := protocol.DecodeMetadataRequest(request.Payload)
	if err != nil {
		return nil, err
	}
	response := protocol.MetadataResponse{
		Version: 1,
		Brokers: []protocol.BrokerMetadata{{ID: h.brokerID, Address: h.address}},
	}
	if h.members != nil {
		snapshot := h.members.Snapshot()
		response.Brokers = response.Brokers[:0]
		for _, member := range snapshot.Brokers {
			response.Brokers = append(response.Brokers, protocol.BrokerMetadata{
				ID: member.ID, Address: member.ClientAddress,
			})
		}
	}
	for _, topicName := range h.registry.List() {
		if metadataRequest.Topic != "" && metadataRequest.Topic != topicName {
			continue
		}
		registeredTopic, err := h.registry.Get(topicName)
		if err != nil {
			return nil, err
		}
		for _, partition := range registeredTopic.Partitions() {
			response.Partitions = append(response.Partitions, protocol.PartitionMetadata{
				Topic: topicName, PartitionID: partition.ID(),
				Replicas: []string{h.brokerID}, Leader: h.brokerID, LeaderEpoch: 1,
			})
		}
	}
	return protocol.EncodeMetadataResponse(response)
}
