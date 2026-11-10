package cluster

import (
	"fmt"
	"sort"

	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

// MetadataHandler implements the broker Handler signature for the Metadata
// API. It serves only committed metadata from the selected controller node.
type MetadataHandler struct {
	controllers *controller.Cluster
	nodeID      string
}

func NewMetadataHandler(controllers *controller.Cluster, nodeID string) (*MetadataHandler, error) {
	if controllers == nil || nodeID == "" {
		return nil, controller.ErrInvalidConfig
	}
	if _, err := controllers.Node(nodeID); err != nil {
		return nil, err
	}
	return &MetadataHandler{controllers: controllers, nodeID: nodeID}, nil
}

func (h *MetadataHandler) Handle(request protocol.Request) ([]byte, error) {
	if h == nil || h.controllers == nil {
		return nil, fmt.Errorf("metadata handler is not initialized: %w", protocol.ErrInvalidRequest)
	}
	if request.APIKey != protocol.APIMETADATA {
		return nil, fmt.Errorf("expected METADATA API key: %w", protocol.ErrInvalidRequest)
	}
	if request.APIVersion != protocol.MetadataAPIVersion {
		return nil, fmt.Errorf("metadata API version %d: %w", request.APIVersion, protocol.ErrUnsupportedVersion)
	}
	metadataRequest, err := protocol.DecodeMetadataRequest(request.Payload)
	if err != nil {
		return nil, err
	}
	snapshot, err := h.controllers.Metadata(h.nodeID)
	if err != nil {
		return nil, err
	}

	response := protocol.MetadataResponse{Version: snapshot.Version}
	brokerIDs := make([]string, 0, len(snapshot.Brokers))
	for id := range snapshot.Brokers {
		brokerIDs = append(brokerIDs, id)
	}
	sort.Strings(brokerIDs)
	for _, id := range brokerIDs {
		broker := snapshot.Brokers[id]
		response.Brokers = append(response.Brokers, protocol.BrokerMetadata{ID: broker.ID, Address: broker.Address})
	}
	keys := make([]controller.PartitionKey, 0, len(snapshot.Partitions))
	for key := range snapshot.Partitions {
		if metadataRequest.Topic == "" || metadataRequest.Topic == key.Topic {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic == keys[j].Topic {
			return keys[i].Partition < keys[j].Partition
		}
		return keys[i].Topic < keys[j].Topic
	})
	for _, key := range keys {
		assignment := snapshot.Partitions[key]
		if key.Partition < 0 {
			return nil, protocol.ErrInvalidMetadataPayload
		}
		response.Partitions = append(response.Partitions, protocol.PartitionMetadata{
			Topic: key.Topic, PartitionID: uint32(key.Partition),
			Replicas: append([]string(nil), assignment.Replicas...),
			Leader:   assignment.Leader, LeaderEpoch: assignment.LeaderEpoch,
		})
	}
	return protocol.EncodeMetadataResponse(response)
}
