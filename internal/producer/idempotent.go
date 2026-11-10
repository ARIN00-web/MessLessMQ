// Package producer implements partition-scoped idempotent producer sessions.
package producer

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/recordmeta"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const recentSequenceWindow = 256

var (
	ErrInvalidProducerID          = errors.New("invalid producer ID")
	ErrProducerFenced             = errors.New("producer session is fenced by a newer epoch")
	ErrSequenceGap                = errors.New("producer sequence has a gap")
	ErrSequenceReuse              = errors.New("producer sequence was reused with different payload")
	ErrSequenceTooOld             = errors.New("producer sequence is older than the duplicate-detection window")
	ErrPartitionNotRegistered     = errors.New("partition is not registered with idempotency manager")
	ErrPartitionAlreadyRegistered = errors.New("a different partition object is already registered for this identity")
	ErrProducerLogCorrupt         = errors.New("producer sequence metadata in partition log is inconsistent")
)

type partitionKey struct {
	topic string
	id    uint32
}

type producerPartitionKey struct {
	producerID string
	partition  partitionKey
}

type receipt struct {
	offset      uint64
	payloadHash [32]byte
}

type sequenceState struct {
	epoch        uint64
	nextSequence uint64
	recent       map[uint64]receipt
	recentOrder  []uint64
}

// Manager coordinates producer epochs and per-producer/per-partition
// sequences. It must be shared by producers that write to the same broker
// process; otherwise epoch fencing is not global to that process.
type Manager struct {
	mu         sync.Mutex
	partitions map[partitionKey]*topic.Partition
	maxEpoch   map[string]uint64
	hasEpoch   map[string]bool
	states     map[producerPartitionKey]*sequenceState
}

// Session is a producer identity plus a fencing epoch.
type Session struct {
	manager    *Manager
	producerID string
	epoch      uint64
}

type AppendResult struct {
	Topic       string
	PartitionID uint32
	Offset      uint64
	ProducerID  string
	Epoch       uint64
	Sequence    uint64
	Duplicate   bool
}

// NewManager creates an idempotency manager and rebuilds its producer
// sequence state from the supplied, already-open partition logs.
func NewManager(partitions ...*topic.Partition) (*Manager, error) {
	m := &Manager{
		partitions: make(map[partitionKey]*topic.Partition),
		maxEpoch:   make(map[string]uint64),
		hasEpoch:   make(map[string]bool),
		states:     make(map[producerPartitionKey]*sequenceState),
	}
	for _, partition := range partitions {
		if err := m.RegisterPartition(partition); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// RegisterPartition adds a partition to the manager and scans its retained
// records to recover producer sequence state. Call this for new partitions
// created after the manager itself has been constructed.
func (m *Manager) RegisterPartition(partition *topic.Partition) error {
	if m == nil || partition == nil {
		return ErrPartitionNotRegistered
	}
	key := partitionKey{topic: partition.TopicName(), id: partition.ID()}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.partitions[key]; ok {
		if existing != partition {
			return fmt.Errorf("%w: %s/%d", ErrPartitionAlreadyRegistered, key.topic, key.id)
		}
		return nil
	}
	if err := m.rebuildPartitionLocked(partition); err != nil {
		return fmt.Errorf("recover producer state for %s/%d: %w", key.topic, key.id, err)
	}
	m.partitions[key] = partition
	return nil
}

// BeginProducer starts a new epoch for producerID. Starting another session
// with the same ID fences all earlier sessions managed by this Manager.
func (m *Manager) BeginProducer(producerID string) (*Session, error) {
	if m == nil || !validProducerID(producerID) {
		return nil, ErrInvalidProducerID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var epoch uint64
	if m.hasEpoch[producerID] {
		if m.maxEpoch[producerID] == math.MaxUint64 {
			return nil, ErrProducerFenced
		}
		epoch = m.maxEpoch[producerID] + 1
	}
	m.maxEpoch[producerID] = epoch
	m.hasEpoch[producerID] = true
	return &Session{manager: m, producerID: producerID, epoch: epoch}, nil
}

// OpenProducer resumes a caller-supplied epoch after reconnect/restart.
// The epoch must match the latest durable epoch observed in registered logs,
// or be greater than it. A lower epoch is fenced.
func (m *Manager) OpenProducer(producerID string, epoch uint64) (*Session, error) {
	if m == nil || !validProducerID(producerID) {
		return nil, ErrInvalidProducerID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hasEpoch[producerID] && epoch < m.maxEpoch[producerID] {
		return nil, ErrProducerFenced
	}
	if !m.hasEpoch[producerID] || epoch > m.maxEpoch[producerID] {
		m.maxEpoch[producerID] = epoch
		m.hasEpoch[producerID] = true
	}
	return &Session{manager: m, producerID: producerID, epoch: epoch}, nil
}

func (s *Session) ProducerID() string {
	if s == nil {
		return ""
	}
	return s.producerID
}

func (s *Session) Epoch() uint64 {
	if s == nil {
		return 0
	}
	return s.epoch
}

// Append appends one idempotent record. The sequence starts at zero for each
// producer/partition pair. Retrying the same recent sequence with identical
// payload returns the original offset and does not append another record.
func (s *Session) Append(
	partition *topic.Partition,
	sequence uint64,
	payload []byte,
) (AppendResult, error) {
	if s == nil || s.manager == nil || !validProducerID(s.producerID) {
		return AppendResult{}, ErrInvalidProducerID
	}
	if partition == nil {
		return AppendResult{}, ErrPartitionNotRegistered
	}

	m := s.manager
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.hasEpoch[s.producerID] || m.maxEpoch[s.producerID] != s.epoch {
		return AppendResult{}, ErrProducerFenced
	}

	pkey := partitionKey{topic: partition.TopicName(), id: partition.ID()}
	registered, exists := m.partitions[pkey]
	if !exists || registered != partition {
		return AppendResult{}, fmt.Errorf("%w: %s/%d", ErrPartitionNotRegistered, pkey.topic, pkey.id)
	}

	key := producerPartitionKey{producerID: s.producerID, partition: pkey}
	state := m.states[key]
	if state != nil && s.epoch < state.epoch {
		return AppendResult{}, ErrProducerFenced
	}
	if state == nil || state.epoch < s.epoch {
		state = &sequenceState{
			epoch:  s.epoch,
			recent: make(map[uint64]receipt),
		}
		m.states[key] = state
	}

	payloadHash := sha256.Sum256(payload)
	if sequence < state.nextSequence {
		if previous, found := state.recent[sequence]; found {
			if previous.payloadHash != payloadHash {
				return AppendResult{}, ErrSequenceReuse
			}
			return AppendResult{
				Topic: pkey.topic, PartitionID: pkey.id, Offset: previous.offset,
				ProducerID: s.producerID, Epoch: s.epoch, Sequence: sequence,
				Duplicate: true,
			}, nil
		}
		return AppendResult{}, ErrSequenceTooOld
	}
	if sequence > state.nextSequence {
		return AppendResult{}, fmt.Errorf(
			"%w: expected=%d received=%d",
			ErrSequenceGap,
			state.nextSequence,
			sequence,
		)
	}
	if sequence == math.MaxUint64 {
		return AppendResult{}, ErrSequenceGap
	}

	encoded, err := recordmeta.Encode(recordmeta.ProducerMetadata{
		ProducerID: s.producerID,
		Epoch:      s.epoch,
		Sequence:   sequence,
	}, payload)
	if err != nil {
		return AppendResult{}, err
	}

	offset, err := partition.AppendSync(encoded)
	if err != nil {
		// AppendSync may report a sync error after the record reached the OS.
		// Re-scan the authoritative partition log before allowing a retry.
		recoveryErr := m.rebuildPartitionLocked(partition)
		return AppendResult{}, errors.Join(fmt.Errorf("append idempotent record: %w", err), recoveryErr)
	}

	state.remember(sequence, receipt{offset: offset, payloadHash: payloadHash})
	state.nextSequence++

	return AppendResult{
		Topic: pkey.topic, PartitionID: pkey.id, Offset: offset,
		ProducerID: s.producerID, Epoch: s.epoch, Sequence: sequence,
	}, nil
}

func (s *sequenceState) remember(sequence uint64, value receipt) {
	if s.recent == nil {
		s.recent = make(map[uint64]receipt)
	}
	if _, exists := s.recent[sequence]; exists {
		s.recent[sequence] = value
		return
	}
	s.recent[sequence] = value
	s.recentOrder = append(s.recentOrder, sequence)
	if len(s.recentOrder) > recentSequenceWindow {
		oldest := s.recentOrder[0]
		s.recentOrder = s.recentOrder[1:]
		delete(s.recent, oldest)
	}
}

func (m *Manager) rebuildPartitionLocked(partition *topic.Partition) error {
	start, end, err := partition.OffsetRange()
	if err != nil {
		return err
	}
	pkey := partitionKey{topic: partition.TopicName(), id: partition.ID()}

	for key := range m.states {
		if key.partition == pkey {
			delete(m.states, key)
		}
	}

	for offset := start; offset < end; offset++ {
		value, _, err := partition.ReadWithTimestamp(offset)
		if err != nil {
			return fmt.Errorf("read partition offset %d: %w", offset, err)
		}
		meta, payload, isEnvelope, err := recordmeta.Decode(value)
		if err != nil {
			return fmt.Errorf("decode producer envelope at offset %d: %w", offset, err)
		}
		if !isEnvelope {
			continue
		}

		key := producerPartitionKey{producerID: meta.ProducerID, partition: pkey}
		state := m.states[key]
		if state == nil || meta.Epoch > state.epoch {
			state = &sequenceState{
				epoch:  meta.Epoch,
				recent: make(map[uint64]receipt),
			}
			m.states[key] = state
		}
		if meta.Epoch < state.epoch {
			continue
		}
		if meta.Sequence < state.nextSequence {
			return fmt.Errorf("%w: duplicate sequence %d at offset %d", ErrProducerLogCorrupt, meta.Sequence, offset)
		}
		if meta.Sequence > state.nextSequence && state.nextSequence != 0 {
			return fmt.Errorf("%w: expected sequence %d, found %d at offset %d", ErrProducerLogCorrupt, state.nextSequence, meta.Sequence, offset)
		}
		state.nextSequence = meta.Sequence + 1
		state.remember(meta.Sequence, receipt{
			offset:      offset,
			payloadHash: sha256.Sum256(payload),
		})
		if !m.hasEpoch[meta.ProducerID] || meta.Epoch > m.maxEpoch[meta.ProducerID] {
			m.maxEpoch[meta.ProducerID] = meta.Epoch
			m.hasEpoch[meta.ProducerID] = true
		}
	}
	return nil
}

func validProducerID(id string) bool {
	if len(id) == 0 || len(id) > recordmeta.MaxProducerIDLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
