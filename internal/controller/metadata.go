package controller

import "strings"

// MetadataSnapshot is a detached, read-only-by-convention view of committed
// cluster metadata. Maps and replica slices are copied when a snapshot is made.
type MetadataSnapshot struct {
	Version    uint64
	Brokers    map[string]Broker
	Partitions map[PartitionKey]PartitionAssignment
}

type metadataState struct {
	version    uint64
	brokers    map[string]Broker
	partitions map[PartitionKey]PartitionAssignment
}

func newMetadataState() metadataState {
	return metadataState{
		brokers:    make(map[string]Broker),
		partitions: make(map[PartitionKey]PartitionAssignment),
	}
}

func (m *metadataState) clone() metadataState {
	copyState := newMetadataState()
	copyState.version = m.version
	for id, broker := range m.brokers {
		copyState.brokers[id] = broker
	}
	for key, assignment := range m.partitions {
		assignment.Replicas = append([]string(nil), assignment.Replicas...)
		copyState.partitions[key] = assignment
	}
	return copyState
}

func (m *metadataState) snapshot() MetadataSnapshot {
	out := MetadataSnapshot{
		Version:    m.version,
		Brokers:    make(map[string]Broker, len(m.brokers)),
		Partitions: make(map[PartitionKey]PartitionAssignment, len(m.partitions)),
	}
	for id, broker := range m.brokers {
		out.Brokers[id] = broker
	}
	for key, assignment := range m.partitions {
		assignment.Replicas = append([]string(nil), assignment.Replicas...)
		out.Partitions[key] = assignment
	}
	return out
}

func validateCommandShape(command Command) error {
	switch command.Type {
	case CommandRegisterBroker:
		if strings.TrimSpace(command.Broker.ID) == "" || strings.TrimSpace(command.Broker.Address) == "" {
			return ErrInvalidCommand
		}
	case CommandAssignPartition:
		a := command.Assignment
		if strings.TrimSpace(a.Key.Topic) == "" || a.Key.Partition < 0 || len(a.Replicas) == 0 || a.LeaderEpoch == 0 {
			return ErrInvalidCommand
		}
		if strings.TrimSpace(a.Leader) == "" {
			return ErrInvalidCommand
		}
		seen := make(map[string]struct{}, len(a.Replicas))
		leaderFound := false
		for _, id := range a.Replicas {
			if strings.TrimSpace(id) == "" {
				return ErrInvalidCommand
			}
			if _, exists := seen[id]; exists {
				return ErrInvalidCommand
			}
			seen[id] = struct{}{}
			if id == a.Leader {
				leaderFound = true
			}
		}
		if !leaderFound {
			return ErrLeaderNotInReplicas
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

func (m *metadataState) validate(command Command) error {
	if err := validateCommandShape(command); err != nil {
		return err
	}
	switch command.Type {
	case CommandRegisterBroker:
		if _, exists := m.brokers[command.Broker.ID]; exists {
			return ErrAlreadyExists
		}
	case CommandAssignPartition:
		a := command.Assignment
		for _, id := range a.Replicas {
			if _, exists := m.brokers[id]; !exists {
				return ErrBrokerNotRegistered
			}
		}
		current, exists := m.partitions[a.Key]
		if exists && a.LeaderEpoch <= current.LeaderEpoch {
			return ErrMetadataConflict
		}
	}
	return nil
}

func (m *metadataState) apply(command Command) error {
	if err := m.validate(command); err != nil {
		return err
	}
	switch command.Type {
	case CommandRegisterBroker:
		m.brokers[command.Broker.ID] = command.Broker
	case CommandAssignPartition:
		a := command.Assignment
		a.Replicas = append([]string(nil), a.Replicas...)
		m.partitions[a.Key] = a
	}
	m.version++
	return nil
}
