package topic

import (
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

// openLog opens the persistent storage log for this partition.
func (p *Partition) openLog(path string, maxSegmentBytes int64) error {
	if p == nil || path == "" || maxSegmentBytes <= 0 {
		return ErrInvalidPartitionLogConfig
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.log != nil {
		return fmt.Errorf(
			"%w: topic=%q partition=%d",
			ErrPartitionLogAlreadyOpen,
			p.topicName,
			p.id,
		)
	}

	log, err := storage.OpenLogWithSegmentSize(path, maxSegmentBytes)
	if err != nil {
		return fmt.Errorf("open partition log: %w", err)
	}

	p.log = log
	return nil
}

// storageLog returns the current storage log.
func (p *Partition) storageLog() (*storage.Log, error) {
	if p == nil {
		return nil, ErrPartitionLogNotOpen
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.log == nil {
		return nil, ErrPartitionLogNotOpen
	}

	return p.log, nil
}

// Append appends a message to this partition.
func (p *Partition) Append(data []byte) (uint64, error) {
	log, err := p.storageLog()
	if err != nil {
		return 0, err
	}

	return log.Append(data)
}

// AppendAt appends a message with an explicit Unix-millisecond timestamp.
func (p *Partition) AppendAt(
	data []byte,
	timestamp int64,
) (uint64, error) {
	log, err := p.storageLog()
	if err != nil {
		return 0, err
	}

	return log.AppendAt(data, timestamp)
}

// AppendSync appends a message and synchronizes the log to storage.
func (p *Partition) AppendSync(data []byte) (uint64, error) {
	log, err := p.storageLog()
	if err != nil {
		return 0, err
	}

	return log.AppendSync(data)
}

// Read retrieves a message using its partition-local offset.
func (p *Partition) Read(offset uint64) ([]byte, error) {
	log, err := p.storageLog()
	if err != nil {
		return nil, err
	}

	return log.Read(offset)
}

// ReadWithTimestamp retrieves a message and its timestamp.
func (p *Partition) ReadWithTimestamp(
	offset uint64,
) ([]byte, int64, error) {
	log, err := p.storageLog()
	if err != nil {
		return nil, 0, err
	}

	return log.ReadWithTimestamp(offset)
}

// ReadAtOrAfterTimestamp retrieves the earliest-offset record whose
// timestamp is greater than or equal to the requested timestamp.
func (p *Partition) ReadAtOrAfterTimestamp(
	timestamp int64,
) (uint64, []byte, int64, error) {
	log, err := p.storageLog()
	if err != nil {
		return 0, nil, 0, err
	}

	return log.ReadAtOrAfterTimestamp(timestamp)
}

// NextOffset returns the next partition-local offset.
func (p *Partition) NextOffset() (uint64, error) {
	log, err := p.storageLog()
	if err != nil {
		return 0, err
	}

	return log.NextOffset(), nil
}

// Sync synchronizes this partition's log and indexes.
func (p *Partition) Sync() error {
	log, err := p.storageLog()
	if err != nil {
		return err
	}

	return log.Sync()
}

// Close closes this partition's storage log.
// It is safe to call Close more than once.
func (p *Partition) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.log == nil {
		return nil
	}

	err := p.log.Close()
	p.log = nil

	return err
}
