package server

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/membership"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

type localMetadataHandler struct {
	brokerID    string
	address     string
	registry    *topic.Registry
	members     *membership.Registry
	assignments map[partitionKey]partitionReplicaAssignment
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
	knownBrokers := map[string]struct{}{h.brokerID: {}}
	if h.members != nil {
		snapshot := h.members.Snapshot()
		response.Version = snapshot.Version
		response.Brokers = response.Brokers[:0]
		for _, member := range snapshot.Brokers {
			knownBrokers[member.ID] = struct{}{}
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
			key := partitionKey{Topic: topicName, PartitionID: partition.ID()}
			assignment, assigned := h.assignments[key]
			if !assigned {
				assignment = partitionReplicaAssignment{Topic: topicName, PartitionID: partition.ID(), Replicas: []string{h.brokerID}, LeaderID: h.brokerID, LeaderEpoch: 1}
			}
			// Metadata must not advertise a leader/replica whose broker address is
			// not yet known locally. A later metadata request sees the converged view.
			allKnown := true
			for _, replicaID := range assignment.Replicas {
				if _, ok := knownBrokers[replicaID]; !ok {
					allKnown = false
					break
				}
			}
			if !allKnown {
				continue
			}
			response.Partitions = append(response.Partitions, protocol.PartitionMetadata{
				Topic: topicName, PartitionID: partition.ID(),
				Replicas: append([]string(nil), assignment.Replicas...), Leader: assignment.LeaderID, LeaderEpoch: assignment.LeaderEpoch,
			})
		}
	}
	return protocol.EncodeMetadataResponse(response)
}
