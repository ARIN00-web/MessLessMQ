package replication

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/storage"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

// Manager coordinates replication for one topic partition. Replication is
// modeled in-process in this phase; the binary batch protocol is defined so a
// later inter-broker transport can carry the same messages.
type Manager struct {
	mu                    sync.Mutex
	topicName             string
	partitionID           uint32
	replicas              map[ReplicaID]*Replica
	leaderID              ReplicaID
	leaderEpoch           uint64
	isr                   map[ReplicaID]struct{}
	minISR                int
	highWatermark         uint64 // exclusive: every offset below this is committed
	replicationBatchCount int
	replicationBatchBytes int
}

// NewManager creates a replication group around already-open partition logs.
// The initial ISR consists only of replicas whose retained ranges and records
// match the leader exactly over the leader's retained range.
func NewManager(replicas []*Replica, cfg Config) (*Manager, error) {
	if len(replicas) == 0 || cfg.LeaderID == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.MinISR == 0 {
		cfg.MinISR = 1
	}
	if cfg.MinISR < 1 || cfg.MinISR > len(replicas) {
		return nil, fmt.Errorf("min ISR %d for %d replicas: %w", cfg.MinISR, len(replicas), ErrInvalidConfig)
	}
	if cfg.ReplicationBatchRecords == 0 {
		cfg.ReplicationBatchRecords = DefaultReplicationBatchRecords
	}
	if cfg.ReplicationBatchBytes == 0 {
		cfg.ReplicationBatchBytes = DefaultReplicationBatchBytes
	}
	if cfg.ReplicationBatchRecords < 1 || cfg.ReplicationBatchRecords > MaxReplicationRecords ||
		cfg.ReplicationBatchBytes < 1 || cfg.ReplicationBatchBytes > MaxReplicationFrameBytes {
		return nil, fmt.Errorf("replication batch limits: %w", ErrInvalidConfig)
	}

	m := &Manager{
		replicas:              make(map[ReplicaID]*Replica, len(replicas)),
		leaderID:              cfg.LeaderID,
		leaderEpoch:           1,
		isr:                   make(map[ReplicaID]struct{}, len(replicas)),
		minISR:                cfg.MinISR,
		replicationBatchCount: cfg.ReplicationBatchRecords,
		replicationBatchBytes: cfg.ReplicationBatchBytes,
	}

	var leader *Replica
	for _, replica := range replicas {
		if replica == nil || replica.partition == nil {
			return nil, ErrPartitionMismatch
		}
		if err := validateReplicaID(replica.id); err != nil {
			return nil, err
		}
		if _, exists := m.replicas[replica.id]; exists {
			return nil, fmt.Errorf("%q: %w", replica.id, ErrDuplicateReplica)
		}
		if !replica.Status().Available {
			return nil, fmt.Errorf("replica %q: %w", replica.id, ErrReplicaUnavailable)
		}
		if m.topicName == "" {
			m.topicName = replica.partition.TopicName()
			m.partitionID = replica.partition.ID()
		} else if replica.partition.TopicName() != m.topicName || replica.partition.ID() != m.partitionID {
			return nil, fmt.Errorf("replica %q: %w", replica.id, ErrPartitionMismatch)
		}
		m.replicas[replica.id] = replica
		if replica.id == cfg.LeaderID {
			leader = replica
		}
	}
	if leader == nil {
		return nil, fmt.Errorf("leader %q: %w", cfg.LeaderID, ErrReplicaNotFound)
	}

	leaderStart, leaderEnd, err := leader.partition.OffsetRange()
	if err != nil {
		return nil, fmt.Errorf("inspect leader log: %w", err)
	}

	for _, replica := range replicas {
		start, end, rangeErr := replica.partition.OffsetRange()
		if rangeErr != nil {
			return nil, fmt.Errorf("inspect replica %q: %w", replica.id, rangeErr)
		}
		// A follower may retain older segments than the leader, but it may not
		// have discarded records the leader still retains.
		if start > leaderStart {
			return nil, fmt.Errorf("replica %q start=%d leader start=%d: %w", replica.id, start, leaderStart, ErrReplicaBehindRetention)
		}
		if end != leaderEnd {
			return nil, fmt.Errorf("replica %q end=%d leader end=%d: %w", replica.id, end, leaderEnd, ErrReplicaNotCaughtUp)
		}
		if replica != leader {
			if err := compareLogs(leader.partition, replica.partition, leaderStart, leaderEnd); err != nil {
				return nil, fmt.Errorf("verify replica %q: %w", replica.id, err)
			}
		}
		m.isr[replica.id] = struct{}{}
	}

	m.highWatermark = leaderEnd
	for _, replica := range replicas {
		role := RoleFollower
		if replica.id == cfg.LeaderID {
			role = RoleLeader
		}
		replica.setState(role, true, m.leaderEpoch, leaderEnd)
	}
	return m, nil
}

// Append appends a record with the current Unix-millisecond timestamp.
func (m *Manager) Append(value []byte) (AppendResult, error) {
	return m.AppendAt(value, time.Now().UnixMilli())
}

// AppendAt durably appends to the leader and synchronously replicates to the
// current ISR. A successful return means the record is below the returned
// exclusive high watermark. If ISR falls below MinISR after the leader write,
// the leader may contain an uncommitted tail and the result is returned with
// ErrInsufficientISR.
func (m *Manager) AppendAt(value []byte, timestamp int64) (AppendResult, error) {
	if m == nil {
		return AppendResult{}, ErrNoLeader
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	leader := m.replicas[m.leaderID]
	if leader == nil || !leader.Status().Available {
		return AppendResult{}, ErrNoLeader
	}
	if _, leaderInISR := m.isr[m.leaderID]; !leaderInISR || len(m.isr) < m.minISR {
		return AppendResult{HighWatermark: m.highWatermark, LeaderEpoch: m.leaderEpoch}, ErrInsufficientISR
	}

	offset, err := leader.partition.AppendAtSync(value, timestamp)
	if err != nil {
		// AppendAtSync can report a sync error after the record was appended.
		// Fence this leader until an operator performs recovery/replacement.
		leader.setAvailability(false)
		delete(m.isr, m.leaderID)
		return AppendResult{
			Offset: offset, HighWatermark: m.highWatermark, LeaderEpoch: m.leaderEpoch,
		}, fmt.Errorf("append/sync leader %q: %w", leader.id, err)
	}
	leader.setState(RoleLeader, true, m.leaderEpoch, offset+1)

	acknowledged := 1
	var replicationErrors []error
	for _, id := range m.sortedISRLocked() {
		if id == m.leaderID {
			continue
		}
		follower := m.replicas[id]
		if follower == nil || !follower.Status().Available {
			delete(m.isr, id)
			if follower != nil {
				follower.setAvailability(false)
			}
			continue
		}
		record := Record{
			Offset: offset, Timestamp: timestamp,
			Value: append([]byte(nil), value...),
		}
		if applyErr := follower.applyRecord(m.leaderID, m.leaderEpoch, record); applyErr != nil {
			delete(m.isr, id)
			follower.setAvailability(false)
			replicationErrors = append(replicationErrors, fmt.Errorf("replicate to %q: %w", id, applyErr))
			continue
		}
		acknowledged++
	}

	result := AppendResult{
		Offset: offset, HighWatermark: m.highWatermark,
		LeaderEpoch: m.leaderEpoch, AcknowledgedReplicas: acknowledged,
	}
	if len(m.isr) < m.minISR {
		return result, errors.Join(ErrInsufficientISR, errors.Join(replicationErrors...))
	}
	if err := m.advanceHighWatermarkLocked(); err != nil {
		return result, errors.Join(err, errors.Join(replicationErrors...))
	}
	result.HighWatermark = m.highWatermark
	return result, nil
}

func (m *Manager) advanceHighWatermarkLocked() error {
	if len(m.isr) < m.minISR {
		return ErrInsufficientISR
	}
	if _, ok := m.isr[m.leaderID]; !ok {
		return ErrNoLeader
	}

	minimum := ^uint64(0)
	for id := range m.isr {
		replica := m.replicas[id]
		if replica == nil || !replica.Status().Available {
			return ErrReplicaUnavailable
		}
		if status := replica.Status(); status.MatchOffset < minimum {
			minimum = status.MatchOffset
		}
	}
	if minimum < m.highWatermark {
		return fmt.Errorf("computed high watermark %d is below current %d", minimum, m.highWatermark)
	}
	m.highWatermark = minimum
	return nil
}

// MarkReplicaUnavailable removes a replica from the ISR. If it is the leader,
// writes stop until an eligible follower is promoted.
func (m *Manager) MarkReplicaUnavailable(id ReplicaID) error {
	if m == nil {
		return ErrReplicaNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	replica := m.replicas[id]
	if replica == nil {
		return fmt.Errorf("%q: %w", id, ErrReplicaNotFound)
	}
	replica.setAvailability(false)
	delete(m.isr, id)
	// A failed leader leaves no active leader from which to compute a new
	// watermark. Preserve the last committed watermark until promotion.
	if id == m.leaderID {
		return nil
	}
	if len(m.isr) >= m.minISR {
		return m.advanceHighWatermarkLocked()
	}
	return nil
}

// MarkReplicaAvailable makes a failed follower eligible for catch-up. It is
// not re-added to the ISR until CatchUpReplica succeeds.
func (m *Manager) MarkReplicaAvailable(id ReplicaID) error {
	if m == nil {
		return ErrReplicaNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	replica := m.replicas[id]
	if replica == nil {
		return fmt.Errorf("%q: %w", id, ErrReplicaNotFound)
	}
	if id == m.leaderID && replica.Status().Role == RoleOffline {
		return fmt.Errorf("failed leader must be replaced, not restarted as leader: %w", ErrNoLeader)
	}
	replica.setAvailability(true)
	return nil
}

// CatchUpReplica copies missing records from the leader in bounded batches.
// A follower joins the ISR only after its complete retained range and log end
// have been verified against the leader.
func (m *Manager) CatchUpReplica(id ReplicaID) error {
	if m == nil {
		return ErrNoLeader
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	leader := m.replicas[m.leaderID]
	if leader == nil || !leader.Status().Available {
		return ErrNoLeader
	}
	follower := m.replicas[id]
	if follower == nil {
		return fmt.Errorf("%q: %w", id, ErrReplicaNotFound)
	}
	if id == m.leaderID {
		return ErrNotLeader
	}
	if !follower.Status().Available {
		return ErrReplicaUnavailable
	}

	leaderStart, leaderEnd, err := leader.partition.OffsetRange()
	if err != nil {
		return err
	}
	followerStart, followerEnd, err := follower.partition.OffsetRange()
	if err != nil {
		return err
	}
	if followerStart > leaderStart || followerEnd < leaderStart {
		return fmt.Errorf("follower range [%d,%d), leader start %d: %w", followerStart, followerEnd, leaderStart, ErrReplicaBehindRetention)
	}
	if followerEnd > leaderEnd {
		return fmt.Errorf("follower end %d, leader end %d: %w", followerEnd, leaderEnd, ErrReplicaAheadOfLeader)
	}
	if err := compareLogs(leader.partition, follower.partition, leaderStart, followerEnd); err != nil {
		return fmt.Errorf("verify follower %q before catch-up: %w", id, err)
	}

	for followerEnd < leaderEnd {
		batch, err := m.buildReplicationBatchLocked(id, followerEnd, m.replicationBatchCount, m.replicationBatchBytes)
		if err != nil {
			return err
		}
		if len(batch.Records) == 0 {
			return fmt.Errorf("leader returned an empty batch before log end: %w", ErrInvalidReplicationBatch)
		}
		if err := m.applyReplicationBatchLocked(id, batch); err != nil {
			return err
		}
		followerEnd, err = follower.partition.NextOffset()
		if err != nil {
			return err
		}
	}

	if err := compareLogs(leader.partition, follower.partition, leaderStart, leaderEnd); err != nil {
		return fmt.Errorf("verify caught-up follower %q: %w", id, err)
	}
	follower.setState(RoleFollower, true, m.leaderEpoch, leaderEnd)
	m.isr[id] = struct{}{}
	if len(m.isr) >= m.minISR {
		return m.advanceHighWatermarkLocked()
	}
	return nil
}

// FetchReplicationBatch builds a bounded replication batch for a registered
// follower. The returned epoch and leader identity fence stale replication
// traffic after a leadership change.
func (m *Manager) FetchReplicationBatch(
	followerID ReplicaID,
	fromOffset uint64,
	maxRecords int,
	maxBytes int,
) (ReplicationBatch, error) {
	if m == nil {
		return ReplicationBatch{}, ErrNoLeader
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buildReplicationBatchLocked(followerID, fromOffset, maxRecords, maxBytes)
}

func (m *Manager) buildReplicationBatchLocked(
	followerID ReplicaID,
	fromOffset uint64,
	maxRecords int,
	maxBytes int,
) (ReplicationBatch, error) {
	follower := m.replicas[followerID]
	if follower == nil {
		return ReplicationBatch{}, ErrReplicaNotFound
	}
	if followerID == m.leaderID {
		return ReplicationBatch{}, ErrNotLeader
	}
	if !follower.Status().Available {
		return ReplicationBatch{}, ErrReplicaUnavailable
	}
	if maxRecords <= 0 || maxRecords > MaxReplicationRecords || maxBytes <= 0 || maxBytes > MaxReplicationFrameBytes {
		return ReplicationBatch{}, ErrInvalidConfig
	}
	leader := m.replicas[m.leaderID]
	if leader == nil || !leader.Status().Available {
		return ReplicationBatch{}, ErrNoLeader
	}
	start, end, err := leader.partition.OffsetRange()
	if err != nil {
		return ReplicationBatch{}, err
	}
	if fromOffset < start {
		return ReplicationBatch{}, ErrReplicaBehindRetention
	}
	if fromOffset > end {
		return ReplicationBatch{}, ErrReplicationGap
	}

	batch := ReplicationBatch{
		Version: ProtocolVersion, Topic: m.topicName, PartitionID: m.partitionID,
		LeaderID: m.leaderID, LeaderEpoch: m.leaderEpoch,
		Records: make([]Record, 0, min(maxRecords, 64)),
	}
	totalBytes := 0
	for offset := fromOffset; offset < end && len(batch.Records) < maxRecords; offset++ {
		value, timestamp, readErr := leader.partition.ReadWithTimestamp(offset)
		if readErr != nil {
			return ReplicationBatch{}, fmt.Errorf("read leader offset %d: %w", offset, readErr)
		}
		if len(value) > maxBytes && len(batch.Records) == 0 {
			return ReplicationBatch{}, fmt.Errorf("offset %d payload %d exceeds batch byte limit %d: %w", offset, len(value), maxBytes, ErrInvalidReplicationBatch)
		}
		if totalBytes+len(value) > maxBytes {
			break
		}
		batch.Records = append(batch.Records, Record{
			Offset: offset, Timestamp: timestamp, Value: append([]byte(nil), value...),
		})
		totalBytes += len(value)
	}
	return batch, nil
}

// ApplyReplicationBatch validates the source partition, leader, epoch, and
// contiguous offsets before durably applying records to a follower.
func (m *Manager) ApplyReplicationBatch(followerID ReplicaID, batch ReplicationBatch) error {
	if m == nil {
		return ErrNoLeader
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyReplicationBatchLocked(followerID, batch)
}

func (m *Manager) applyReplicationBatchLocked(followerID ReplicaID, batch ReplicationBatch) error {
	if err := validateReplicationBatch(batch); err != nil {
		return err
	}
	if batch.Topic != m.topicName || batch.PartitionID != m.partitionID {
		return ErrPartitionMismatch
	}
	if batch.LeaderID != m.leaderID || batch.LeaderEpoch != m.leaderEpoch {
		return fmt.Errorf("batch leader=%q epoch=%d current leader=%q epoch=%d: %w", batch.LeaderID, batch.LeaderEpoch, m.leaderID, m.leaderEpoch, ErrStaleLeaderEpoch)
	}
	leader := m.replicas[m.leaderID]
	if leader == nil || !leader.Status().Available {
		return ErrNoLeader
	}
	follower := m.replicas[followerID]
	if follower == nil {
		return ErrReplicaNotFound
	}
	if followerID == m.leaderID {
		return ErrNotLeader
	}
	if !follower.Status().Available {
		return ErrReplicaUnavailable
	}

	leaderStart, leaderEnd, err := leader.partition.OffsetRange()
	if err != nil {
		return err
	}
	followerStart, _, err := follower.partition.OffsetRange()
	if err != nil {
		return err
	}
	if followerStart > leaderStart {
		return ErrReplicaBehindRetention
	}
	for _, record := range batch.Records {
		if record.Offset < leaderStart || record.Offset >= leaderEnd {
			return fmt.Errorf("record offset %d outside leader range [%d,%d): %w", record.Offset, leaderStart, leaderEnd, ErrInvalidReplicationBatch)
		}
		if err := follower.applyRecord(m.leaderID, m.leaderEpoch, record); err != nil {
			return fmt.Errorf("apply follower %q offset %d: %w", followerID, record.Offset, err)
		}
	}

	_, followerEnd, err := follower.partition.OffsetRange()
	if err != nil {
		return err
	}
	follower.setState(RoleFollower, true, m.leaderEpoch, followerEnd)
	if followerEnd == leaderEnd {
		if err := compareLogs(leader.partition, follower.partition, leaderStart, leaderEnd); err != nil {
			return err
		}
		m.isr[followerID] = struct{}{}
	}
	if _, inISR := m.isr[followerID]; inISR && len(m.isr) >= m.minISR {
		return m.advanceHighWatermarkLocked()
	}
	return nil
}

// ElectLeader promotes an available ISR follower after the existing leader is
// unavailable. The candidate must end exactly at the high watermark; any
// uncommitted tail is therefore not promoted. This is an explicit local
// promotion API, not a distributed consensus/election protocol.
func (m *Manager) ElectLeader(candidateID ReplicaID) error {
	if m == nil {
		return ErrNoEligibleLeader
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	oldLeader := m.replicas[m.leaderID]
	if oldLeader != nil && oldLeader.Status().Available {
		return fmt.Errorf("current leader is still available: %w", ErrNoEligibleLeader)
	}
	candidate := m.replicas[candidateID]
	if candidate == nil {
		return fmt.Errorf("%q: %w", candidateID, ErrReplicaNotFound)
	}
	if !candidate.Status().Available {
		return ErrReplicaUnavailable
	}
	if _, inISR := m.isr[candidateID]; !inISR {
		return ErrNoEligibleLeader
	}
	candidateEnd, err := candidate.partition.NextOffset()
	if err != nil {
		return err
	}
	if candidateEnd != m.highWatermark {
		return fmt.Errorf("candidate end=%d high watermark=%d: %w", candidateEnd, m.highWatermark, ErrReplicaNotCaughtUp)
	}
	if m.leaderEpoch == ^uint64(0) {
		return ErrInvalidConfig
	}

	m.leaderEpoch++
	if oldLeader != nil {
		oldLeader.setAvailability(false)
		delete(m.isr, oldLeader.id)
	}
	m.leaderID = candidateID
	for id := range m.isr {
		replica := m.replicas[id]
		role := RoleFollower
		if id == candidateID {
			role = RoleLeader
		}
		status := replica.Status()
		replica.setState(role, status.Available, m.leaderEpoch, status.MatchOffset)
	}
	// Promotion may succeed even when the remaining ISR is below MinISR. In
	// that case the high watermark remains unchanged and writes stay blocked.
	if len(m.isr) >= m.minISR {
		return m.advanceHighWatermarkLocked()
	}
	return nil
}

// ReadCommitted reads only records below the exclusive high watermark.
func (m *Manager) ReadCommitted(offset uint64, maxRecords, maxBytes int) (ReadResult, error) {
	if m == nil {
		return ReadResult{}, ErrNoLeader
	}
	if maxRecords <= 0 || maxRecords > MaxReplicationRecords || maxBytes <= 0 || maxBytes > MaxReplicationFrameBytes {
		return ReadResult{}, ErrInvalidConfig
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	leader := m.replicas[m.leaderID]
	if leader == nil || !leader.Status().Available {
		return ReadResult{}, ErrNoLeader
	}
	start, _, err := leader.partition.OffsetRange()
	if err != nil {
		return ReadResult{}, err
	}
	if offset < start {
		return ReadResult{}, ErrOffsetBeforeHighWatermark
	}
	if offset > m.highWatermark {
		return ReadResult{}, ErrHighWatermarkRange
	}

	result := ReadResult{NextOffset: offset, HighWatermark: m.highWatermark, Records: make([]Record, 0, min(maxRecords, 64))}
	totalBytes := 0
	for current := offset; current < m.highWatermark && len(result.Records) < maxRecords; current++ {
		value, timestamp, readErr := leader.partition.ReadWithTimestamp(current)
		if readErr != nil {
			return ReadResult{}, fmt.Errorf("read committed offset %d: %w", current, readErr)
		}
		if len(value) > maxBytes && len(result.Records) == 0 {
			return ReadResult{}, ErrRecordTooLarge
		}
		if totalBytes+len(value) > maxBytes {
			break
		}
		result.Records = append(result.Records, Record{
			Offset: current, Timestamp: timestamp, Value: append([]byte(nil), value...),
		})
		totalBytes += len(value)
		result.NextOffset = current + 1
	}
	return result, nil
}

// ReadResult is a bounded read from the committed prefix of a partition log.
type ReadResult struct {
	Records       []Record
	NextOffset    uint64
	HighWatermark uint64
}

// HighWatermark returns the exclusive committed offset.
func (m *Manager) HighWatermark() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.highWatermark
}

// InSyncReplicas returns a sorted snapshot of current ISR member IDs.
func (m *Manager) InSyncReplicas() []ReplicaID {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sortedISRLocked()
}

func (m *Manager) sortedISRLocked() []ReplicaID {
	ids := make([]ReplicaID, 0, len(m.isr))
	for id := range m.isr {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Describe returns a deterministic snapshot of replication state.
func (m *Manager) Describe() Status {
	if m == nil {
		return Status{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	status := Status{
		Topic: m.topicName, PartitionID: m.partitionID, LeaderID: m.leaderID,
		LeaderEpoch: m.leaderEpoch, HighWatermark: m.highWatermark,
		MinISR: m.minISR, ISR: m.sortedISRLocked(),
	}
	ids := make([]ReplicaID, 0, len(m.replicas))
	for id := range m.replicas {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		status.Replicas = append(status.Replicas, m.replicas[id].Status())
	}
	return status
}

// Replica returns a replica handle for diagnostics and transport setup.
func (m *Manager) Replica(id ReplicaID) (*Replica, error) {
	if m == nil {
		return nil, ErrReplicaNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	replica := m.replicas[id]
	if replica == nil {
		return nil, fmt.Errorf("%q: %w", id, ErrReplicaNotFound)
	}
	return replica, nil
}

// TopicPartition returns the topic-partition identity managed here.
func (m *Manager) TopicPartition() (string, uint32) {
	if m == nil {
		return "", 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.topicName, m.partitionID
}

// AssertPartition checks whether a partition has the identity managed here.
func (m *Manager) AssertPartition(partition *topic.Partition) error {
	if m == nil || partition == nil {
		return ErrPartitionMismatch
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if partition.TopicName() != m.topicName || partition.ID() != m.partitionID {
		return ErrPartitionMismatch
	}
	return nil
}

func compareLogs(leader, follower *topic.Partition, start, end uint64) error {
	for offset := start; offset < end; offset++ {
		leaderValue, leaderTS, err := leader.ReadWithTimestamp(offset)
		if err != nil {
			return fmt.Errorf("read leader offset %d: %w", offset, err)
		}
		followerValue, followerTS, err := follower.ReadWithTimestamp(offset)
		if err != nil {
			return fmt.Errorf("read follower offset %d: %w", offset, err)
		}
		if leaderTS != followerTS || string(leaderValue) != string(followerValue) {
			return fmt.Errorf("offset %d differs: %w", offset, ErrDivergentReplica)
		}
	}
	return nil
}

func validateReplicationBatch(batch ReplicationBatch) error {
	if batch.Version != ProtocolVersion || batch.Topic == "" || validateReplicaID(batch.LeaderID) != nil || len(batch.Records) > MaxReplicationRecords {
		return ErrInvalidReplicationBatch
	}
	if err := topic.ValidateTopicName(batch.Topic); err != nil {
		return fmt.Errorf("topic: %w", ErrInvalidReplicationBatch)
	}
	var total int
	for i, record := range batch.Records {
		if len(record.Value) > storage.MaxRecordSize {
			return ErrInvalidReplicationBatch
		}
		if i > 0 && record.Offset != batch.Records[i-1].Offset+1 {
			return fmt.Errorf("non-contiguous record offsets: %w", ErrInvalidReplicationBatch)
		}
		total += len(record.Value)
		if total > MaxReplicationValueBytes {
			return ErrInvalidReplicationBatch
		}
	}
	return nil
}
