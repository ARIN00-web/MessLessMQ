package recovery

import "errors"

var (
	ErrInvalidConfig               = errors.New("recovery: invalid configuration")
	ErrPlanNotFound                = errors.New("recovery: reassignment plan not found")
	ErrPlanExists                  = errors.New("recovery: reassignment already active for partition")
	ErrPlanState                   = errors.New("recovery: invalid reassignment state")
	ErrStaleAssignment             = errors.New("recovery: partition assignment changed during reassignment")
	ErrReplicaNotReady             = errors.New("recovery: target replica is not caught up")
	ErrNoControllerLeader          = errors.New("recovery: no active controller leader")
	ErrInvalidReplicaProgress      = errors.New("recovery: invalid replica progress evidence")
	ErrInvalidGroupSnapshot        = errors.New("recovery: invalid consumer group snapshot")
	ErrDuplicatePartitionOwner     = errors.New("recovery: partition has multiple group owners")
	ErrControllerStateInconsistent = errors.New("recovery: controller state is inconsistent")
	ErrBrokerRecoveryIncomplete    = errors.New("recovery: broker recovery incomplete")
)
