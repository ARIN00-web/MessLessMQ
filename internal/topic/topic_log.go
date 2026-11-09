package topic

import (
	"errors"
	"fmt"
	"path/filepath"
)

// partitionLogDirectory constructs the deterministic directory for a
// partition's storage log.
func partitionLogDirectory(
	rootDir string,
	topicName string,
	partitionID uint32,
) string {
	return filepath.Join(
		rootDir,
		topicName,
		fmt.Sprintf("partition-%010d", partitionID),
	)
}

// OpenLogs opens the persistent storage log for every partition in the topic.
//
// Directory layout:
//
//	rootDir/topicName/partition-0000000000/
//	rootDir/topicName/partition-0000000001/
func (t *Topic) OpenLogs(rootDir string, maxSegmentBytes int64) error {
	if t == nil || t.name == "" || rootDir == "" || maxSegmentBytes <= 0 {
		return ErrInvalidPartitionLogConfig
	}

	t.logMu.Lock()
	defer t.logMu.Unlock()

	if t.logsOpen {
		return ErrPartitionLogAlreadyOpen
	}

	t.partitionMu.RLock()
	defer t.partitionMu.RUnlock()

	if len(t.partitions) == 0 {
		return ErrInvalidPartitionCount
	}

	opened := make([]*Partition, 0, len(t.partitions))

	for _, partition := range t.partitions {
		if partition == nil {
			return errors.Join(
				ErrInvalidPartitionLogConfig,
				closeOpenedPartitions(opened),
			)
		}

		dir := partitionLogDirectory(
			rootDir,
			t.name,
			partition.id,
		)

		if err := partition.openLog(dir, maxSegmentBytes); err != nil {
			openErr := fmt.Errorf(
				"open topic %q partition %d: %w",
				t.name,
				partition.id,
				err,
			)

			return errors.Join(
				openErr,
				closeOpenedPartitions(opened),
			)
		}

		opened = append(opened, partition)
	}

	// Save the configuration only after every partition opens successfully.
	t.logRootDir = rootDir
	t.maxSegmentBytes = maxSegmentBytes
	t.logsOpen = true

	return nil
}

// CloseLogs closes all partition logs belonging to the topic.
//
// All partitions are attempted even if one close operation fails.
func (t *Topic) CloseLogs() error {
	if t == nil {
		return nil
	}

	t.logMu.Lock()
	defer t.logMu.Unlock()

	t.partitionMu.RLock()
	defer t.partitionMu.RUnlock()

	var closeErrors []error

	for _, partition := range t.partitions {
		if partition == nil {
			continue
		}

		if err := partition.Close(); err != nil {
			closeErrors = append(
				closeErrors,
				fmt.Errorf(
					"close topic %q partition %d: %w",
					t.name,
					partition.id,
					err,
				),
			)
		}
	}

	t.logsOpen = false
	t.logRootDir = ""
	t.maxSegmentBytes = 0

	return errors.Join(closeErrors...)
}

// closeOpenedPartitions rolls back logs opened during a failed OpenLogs call.
func closeOpenedPartitions(partitions []*Partition) error {
	var closeErrors []error

	for i := len(partitions) - 1; i >= 0; i-- {
		if err := partitions[i].Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}

	return errors.Join(closeErrors...)
}
