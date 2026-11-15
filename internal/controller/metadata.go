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
		assignment.ISR = append([]string(nil), assignment.ISR...)
		assignment.ReassignmentTarget = append([]string(nil), assignment.ReassignmentTarget...)
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
		assignment.ISR = append([]string(nil), assignment.ISR...)
		assignment.ReassignmentTarget = append([]string(nil), assignment.ReassignmentTarget...)
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
		// Older durable logs predate ISR metadata. An omitted ISR means the
		// complete replica set when the assignment is applied.
		if len(a.ISR) == 0 {
			a.ISR = a.Replicas
		}
		isrSeen := make(map[string]struct{}, len(a.ISR))
		leaderInISR := false
		for _, id := range a.ISR {
			if _, replica := seen[id]; !replica || id == "" {
				return ErrInvalidCommand
			}
			if _, duplicate := isrSeen[id]; duplicate {
				return ErrInvalidCommand
			}
			isrSeen[id] = struct{}{}
			leaderInISR = leaderInISR || id == a.Leader
		}
		if !leaderInISR {
			return ErrInvalidCommand
		}
		targetSeen := make(map[string]struct{}, len(a.ReassignmentTarget))
		for _, id := range a.ReassignmentTarget {
			if _, replica := seen[id]; !replica || id == "" {
				return ErrInvalidCommand
			}
			if _, duplicate := targetSeen[id]; duplicate {
				return ErrInvalidCommand
			}
			targetSeen[id] = struct{}{}
		}
	case CommandUpdatePartitionISR:
		a := command.Assignment
		if strings.TrimSpace(a.Key.Topic) == "" || a.Key.Partition < 0 || len(a.ISR) == 0 {
			return ErrInvalidCommand
		}
	case CommandNoop:
		return nil
	default:
		return ErrInvalidCommand

	case CommandUpdateBrokerAddress:
		if strings.TrimSpace(command.Broker.ID) == "" ||
			strings.TrimSpace(command.Broker.Address) == "" {
			return ErrInvalidCommand
		}
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
	case CommandUpdatePartitionISR:
		current, exists := m.partitions[command.Assignment.Key]
		if !exists || command.Assignment.Leader != current.Leader || command.Assignment.LeaderEpoch != current.LeaderEpoch {
			return ErrMetadataConflict
		}
		seen := make(map[string]struct{}, len(current.Replicas))
		for _, id := range current.Replicas {
			seen[id] = struct{}{}
		}
		isrSeen := make(map[string]struct{}, len(command.Assignment.ISR))
		leaderInISR := false
		for _, id := range command.Assignment.ISR {
			if _, replica := seen[id]; !replica {
				return ErrInvalidCommand
			}
			if _, duplicate := isrSeen[id]; duplicate {
				return ErrInvalidCommand
			}
			isrSeen[id] = struct{}{}
			leaderInISR = leaderInISR || id == current.Leader
		}
		if !leaderInISR {
			return ErrInvalidCommand
		}
	case CommandUpdateBrokerAddress:
		if _, exists := m.brokers[command.Broker.ID]; !exists {
			return ErrBrokerNotRegistered
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
		if len(a.ISR) == 0 {
			a.ISR = append([]string(nil), a.Replicas...)
		}
		a.ISR = append([]string(nil), a.ISR...)
		a.ReassignmentTarget = append([]string(nil), a.ReassignmentTarget...)
		m.partitions[a.Key] = a
	case CommandUpdatePartitionISR:
		current := m.partitions[command.Assignment.Key]
		current.ISR = append([]string(nil), command.Assignment.ISR...)
		m.partitions[command.Assignment.Key] = current
	case CommandNoop:
		return nil
	case CommandUpdateBrokerAddress:
		m.brokers[command.Broker.ID] = command.Broker
	}
	m.version++
	return nil
}
