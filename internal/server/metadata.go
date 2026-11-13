package server

import (
	"fmt"
	"sort"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/membership"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

type localMetadataHandler struct {
	brokerID    string
	address     string
	registry    *topic.Registry
	members     *membership.Registry
	assignments *assignmentRegistry
	controller  *controller.DurableNode
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

	response := protocol.MetadataResponse{Version: 1}
	knownBrokers := make(map[string]struct{})
	committedAssignments := make(map[partitionKey]partitionReplicaAssignment)

	if h.controller != nil {
		// In controller mode, only committed Raft metadata is authoritative.
		// Membership discovery alone must not cause clients to route to a
		// broker or partition leader that the controller has not committed.
		snapshot := h.controller.Metadata()
		response.Version = snapshot.Version
		brokerIDs := make([]string, 0, len(snapshot.Brokers))
		for id := range snapshot.Brokers {
			brokerIDs = append(brokerIDs, id)
		}
		sort.Strings(brokerIDs)
		for _, id := range brokerIDs {
			broker := snapshot.Brokers[id]
			response.Brokers = append(response.Brokers, protocol.BrokerMetadata{ID: broker.ID, Address: broker.Address})
			knownBrokers[id] = struct{}{}
		}
		for key, assignment := range snapshot.Partitions {
			committedAssignments[partitionKey{Topic: key.Topic, PartitionID: uint32(key.Partition)}] = partitionReplicaAssignment{
				Topic: key.Topic, PartitionID: uint32(key.Partition),
				Replicas: append([]string(nil), assignment.Replicas...),
				LeaderID: assignment.Leader, LeaderEpoch: assignment.LeaderEpoch,
			}
		}
	} else {
		response.Brokers = []protocol.BrokerMetadata{{ID: h.brokerID, Address: h.address}}
		knownBrokers[h.brokerID] = struct{}{}
		if h.members != nil {
			membershipSnapshot := h.members.Snapshot()
			response.Version = membershipSnapshot.Version
			response.Brokers = response.Brokers[:0]
			for _, member := range membershipSnapshot.Brokers {
				knownBrokers[member.ID] = struct{}{}
				response.Brokers = append(response.Brokers, protocol.BrokerMetadata{ID: member.ID, Address: member.ClientAddress})
			}
		}
		if h.assignments != nil {
			committedAssignments = h.assignments.snapshot()
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
			assignment, assigned := committedAssignments[key]
			if !assigned && h.controller == nil {
				assignment = partitionReplicaAssignment{Topic: topicName, PartitionID: partition.ID(), Replicas: []string{h.brokerID}, LeaderID: h.brokerID, LeaderEpoch: 1}
				assigned = true
			}
			if !assigned {
				continue
			}
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
