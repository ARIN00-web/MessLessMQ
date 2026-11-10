package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

var (
	ErrMetadataNotLoaded = errors.New("cluster: client metadata has not been loaded")
	ErrStaleMetadata     = errors.New("cluster: refusing stale metadata snapshot")
	ErrRouteNotFound     = errors.New("cluster: route not found in metadata")
)

// ClientRoute is the broker address and assignment selected from a cached
// metadata snapshot.
type ClientRoute struct {
	MetadataVersion uint64
	Broker          protocol.BrokerMetadata
	Partition       protocol.PartitionMetadata
}

// ClientRouter maintains a detached metadata cache. Clients refresh it through
// the Metadata API and use Resolve to select a partition leader locally.
type ClientRouter struct {
	mu       sync.RWMutex
	metadata *protocol.MetadataResponse
}

func NewClientRouter() *ClientRouter { return &ClientRouter{} }

// Update replaces the cache only with a structurally valid snapshot whose
// metadata version is not older than the currently cached version.
func (r *ClientRouter) Update(snapshot protocol.MetadataResponse) error {
	if r == nil {
		return ErrMetadataNotLoaded
	}
	if _, err := protocol.EncodeMetadataResponse(snapshot); err != nil {
		return err
	}
	copySnapshot := cloneProtocolMetadata(snapshot)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.metadata != nil && copySnapshot.Version < r.metadata.Version {
		return fmt.Errorf("received=%d cached=%d: %w", copySnapshot.Version, r.metadata.Version, ErrStaleMetadata)
	}
	r.metadata = &copySnapshot
	return nil
}

func (r *ClientRouter) MetadataVersion() (uint64, bool) {
	if r == nil {
		return 0, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.metadata == nil {
		return 0, false
	}
	return r.metadata.Version, true
}

// Resolve selects the advertised leader from the cached metadata. It does not
// prove that the broker is reachable; network errors trigger metadata refresh
// or retry at the client layer.
func (r *ClientRouter) Resolve(topic string, partitionID uint32) (ClientRoute, error) {
	if r == nil {
		return ClientRoute{}, ErrMetadataNotLoaded
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.metadata == nil {
		return ClientRoute{}, ErrMetadataNotLoaded
	}
	for _, partition := range r.metadata.Partitions {
		if partition.Topic != topic || partition.PartitionID != partitionID {
			continue
		}
		for _, broker := range r.metadata.Brokers {
			if broker.ID == partition.Leader {
				return ClientRoute{MetadataVersion: r.metadata.Version, Broker: broker, Partition: cloneProtocolPartition(partition)}, nil
			}
		}
		return ClientRoute{}, ErrBrokerUnavailable
	}
	return ClientRoute{}, ErrRouteNotFound
}

func cloneProtocolMetadata(snapshot protocol.MetadataResponse) protocol.MetadataResponse {
	out := protocol.MetadataResponse{
		Version:    snapshot.Version,
		Brokers:    append([]protocol.BrokerMetadata(nil), snapshot.Brokers...),
		Partitions: make([]protocol.PartitionMetadata, len(snapshot.Partitions)),
	}
	for i, partition := range snapshot.Partitions {
		out.Partitions[i] = cloneProtocolPartition(partition)
	}
	return out
}

func cloneProtocolPartition(partition protocol.PartitionMetadata) protocol.PartitionMetadata {
	partition.Replicas = append([]string(nil), partition.Replicas...)
	return partition
}
