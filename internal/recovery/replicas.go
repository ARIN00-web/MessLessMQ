package recovery

import (
	"errors"
	"fmt"
)

// ReplicaRecoveryTask adapts one broker-owned replica to the recovery
// workflow. Callbacks let the broker wire its actual replication.Manager
// without coupling the control-plane package to physical partition objects.
type ReplicaRecoveryTask struct {
	BrokerID        string
	Topic           string
	PartitionID     uint32
	MarkAvailable   func() error
	CatchUp         func() error
	Inspect         func() (ReplicaReadiness, error)
	MarkUnavailable func() error
}

type ReplicaRecoveryResult struct {
	BrokerID      string
	Topic         string
	PartitionID   uint32
	MatchOffset   uint64
	HighWatermark uint64
	LeaderEpoch   uint64
	InSync        bool
}

// RecoverReplica brings an offline replica back only after it has caught up
// and the data plane reports it as an in-sync replica. Any failed recovery
// attempts to fence the replica out again.
func RecoverReplica(task ReplicaRecoveryTask) (ReplicaRecoveryResult, error) {
	if task.BrokerID == "" || task.Topic == "" || task.MarkAvailable == nil || task.CatchUp == nil || task.Inspect == nil {
		return ReplicaRecoveryResult{}, ErrInvalidConfig
	}
	fail := func(cause error) (ReplicaRecoveryResult, error) {
		if task.MarkUnavailable != nil {
			if cleanupErr := task.MarkUnavailable(); cleanupErr != nil {
				cause = errors.Join(cause, fmt.Errorf("fence failed replica: %w", cleanupErr))
			}
		}
		return ReplicaRecoveryResult{}, cause
	}
	if err := task.MarkAvailable(); err != nil {
		return fail(fmt.Errorf("mark replica available: %w", err))
	}
	if err := task.CatchUp(); err != nil {
		return fail(fmt.Errorf("catch up replica: %w", err))
	}
	progress, err := task.Inspect()
	if err != nil {
		return fail(fmt.Errorf("inspect recovered replica: %w", err))
	}
	if progress.BrokerID != task.BrokerID {
		return fail(ErrInvalidReplicaProgress)
	}
	if err := ValidateReplicaReadiness(progress); err != nil {
		return fail(err)
	}
	return ReplicaRecoveryResult{
		BrokerID: task.BrokerID, Topic: task.Topic, PartitionID: task.PartitionID,
		MatchOffset: progress.MatchOffset, HighWatermark: progress.HighWatermark,
		LeaderEpoch: progress.LeaderEpoch, InSync: progress.InSync,
	}, nil
}

type BrokerRecoveryResult struct {
	BrokerID  string
	Recovered []ReplicaRecoveryResult
	Failed    []PartitionRecoveryFailure
}

type PartitionRecoveryFailure struct {
	Topic       string
	PartitionID uint32
	Err         error
}

// RecoverBroker attempts every partition assigned to a broker. It returns a
// complete report instead of stopping at the first failed partition. A broker
// is considered recovered only when every supplied partition task succeeds.
func RecoverBroker(brokerID string, tasks []ReplicaRecoveryTask) (BrokerRecoveryResult, error) {
	if brokerID == "" || len(tasks) == 0 {
		return BrokerRecoveryResult{}, ErrInvalidConfig
	}
	result := BrokerRecoveryResult{BrokerID: brokerID}
	var failures []error
	seen := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		if task.BrokerID != brokerID {
			return result, fmt.Errorf("task broker %q does not match %q: %w", task.BrokerID, brokerID, ErrInvalidConfig)
		}
		key := fmt.Sprintf("%s/%d", task.Topic, task.PartitionID)
		if _, duplicate := seen[key]; duplicate {
			return result, fmt.Errorf("duplicate partition task %s: %w", key, ErrInvalidConfig)
		}
		seen[key] = struct{}{}
		recovered, err := RecoverReplica(task)
		if err != nil {
			result.Failed = append(result.Failed, PartitionRecoveryFailure{Topic: task.Topic, PartitionID: task.PartitionID, Err: err})
			failures = append(failures, fmt.Errorf("%s-%d: %w", task.Topic, task.PartitionID, err))
			continue
		}
		result.Recovered = append(result.Recovered, recovered)
	}
	if len(failures) > 0 {
		return result, errors.Join(append([]error{ErrBrokerRecoveryIncomplete}, failures...)...)
	}
	return result, nil
}
