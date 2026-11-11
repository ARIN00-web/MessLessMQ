package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/interbroker"
	"github.com/ARIN00-web/messlessmq/internal/membership"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/replication"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const (
	replicationBatchRecords = 128
	replicationBatchBytes   = 16 * 1024 * 1024
)

type partitionKey struct {
	Topic       string
	PartitionID uint32
}

type partitionReplicaAssignment struct {
	Topic       string
	PartitionID uint32
	Replicas    []string
	LeaderID    string
	LeaderEpoch uint64
}

// ParseReplicaAssignments parses entries in the form
// topic:partition=leader,follower[,follower];topic:partition=leader,follower.
// The first replica is the statically configured leader for this milestone.
func parseReplicaAssignments(raw string) (map[partitionKey]partitionReplicaAssignment, error) {
	assignments := make(map[partitionKey]partitionReplicaAssignment)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return assignments, nil
	}
	for _, entry := range strings.Split(raw, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("empty replica assignment entry")
		}
		left, right, ok := strings.Cut(entry, "=")
		if !ok || strings.Contains(right, "=") {
			return nil, fmt.Errorf("invalid replica assignment %q: expected topic:partition=broker,broker", entry)
		}
		topicName, partitionText, ok := strings.Cut(strings.TrimSpace(left), ":")
		if !ok || strings.TrimSpace(topicName) != topicName || topic.ValidateTopicName(topicName) != nil {
			return nil, fmt.Errorf("invalid topic in replica assignment %q", entry)
		}
		partitionNumber, err := strconv.ParseUint(strings.TrimSpace(partitionText), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid partition in replica assignment %q", entry)
		}
		key := partitionKey{Topic: topicName, PartitionID: uint32(partitionNumber)}
		if _, exists := assignments[key]; exists {
			return nil, fmt.Errorf("duplicate replica assignment for %s:%d", key.Topic, key.PartitionID)
		}
		parts := strings.Split(right, ",")
		if len(parts) == 0 {
			return nil, fmt.Errorf("replica assignment %s:%d has no replicas", key.Topic, key.PartitionID)
		}
		seen := make(map[string]struct{}, len(parts))
		replicas := make([]string, 0, len(parts))
		for _, part := range parts {
			id := strings.TrimSpace(part)
			if id == "" || len(id) > 128 {
				return nil, fmt.Errorf("invalid broker ID in replica assignment %s:%d", key.Topic, key.PartitionID)
			}
			for _, r := range id {
				if r < 0x21 || r > 0x7e || r == ',' || r == ';' || r == '=' || r == '@' {
					return nil, fmt.Errorf("invalid broker ID %q in replica assignment", id)
				}
			}
			if _, exists := seen[id]; exists {
				return nil, fmt.Errorf("duplicate broker %q in replica assignment %s:%d", id, key.Topic, key.PartitionID)
			}
			seen[id] = struct{}{}
			replicas = append(replicas, id)
		}
		assignments[key] = partitionReplicaAssignment{
			Topic: key.Topic, PartitionID: key.PartitionID,
			Replicas: replicas, LeaderID: replicas[0], LeaderEpoch: 1,
		}
	}
	return assignments, nil
}

func validateReplicaAssignments(definitions []TopicDefinition, assignments map[partitionKey]partitionReplicaAssignment, minISR int) error {
	if len(assignments) == 0 {
		return nil
	}
	if minISR < 1 {
		return fmt.Errorf("minimum ISR must be positive")
	}
	expected := make(map[partitionKey]struct{})
	for _, definition := range definitions {
		for partitionID := uint32(0); partitionID < definition.Partitions; partitionID++ {
			key := partitionKey{Topic: definition.Name, PartitionID: partitionID}
			expected[key] = struct{}{}
			assignment, exists := assignments[key]
			if !exists {
				return fmt.Errorf("missing replica assignment for %s:%d", key.Topic, key.PartitionID)
			}
			if minISR > len(assignment.Replicas) {
				return fmt.Errorf("min ISR %d exceeds replica count %d for %s:%d", minISR, len(assignment.Replicas), key.Topic, key.PartitionID)
			}
		}
	}
	for key := range assignments {
		if _, exists := expected[key]; !exists {
			return fmt.Errorf("replica assignment references unknown topic partition %s:%d", key.Topic, key.PartitionID)
		}
	}
	return nil
}

type distributedAppender struct {
	brokerID    string
	members     *membership.Registry
	assignments map[partitionKey]partitionReplicaAssignment
	minISR      int
	commits     *replication.CommitStore
	transport   *interbroker.Transport
	requestID   atomic.Uint64
	locksMu     sync.Mutex
	locks       map[partitionKey]*sync.Mutex
}

func newDistributedAppender(brokerID string, members *membership.Registry, assignments map[partitionKey]partitionReplicaAssignment, minISR int, commits *replication.CommitStore) *distributedAppender {
	return &distributedAppender{
		brokerID: brokerID, members: members, assignments: assignments, minISR: minISR, commits: commits,
		transport: interbroker.NewTransport(5 * time.Second), locks: make(map[partitionKey]*sync.Mutex),
	}
}

func (a *distributedAppender) partitionLock(key partitionKey) *sync.Mutex {
	a.locksMu.Lock()
	defer a.locksMu.Unlock()
	lock := a.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		a.locks[key] = lock
	}
	return lock
}

func (a *distributedAppender) Append(partition *topic.Partition, topicName string, value []byte, timestamp int64, ack protocol.AckMode) (uint64, error) {
	if a == nil || partition == nil {
		return 0, errors.New("distributed appender is not initialized")
	}
	key := partitionKey{Topic: topicName, PartitionID: partition.ID()}
	assignment, assigned := a.assignments[key]
	if assigned && assignment.LeaderID != a.brokerID {
		return 0, fmt.Errorf("partition %s:%d leader is %s, this broker is %s: %w", topicName, partition.ID(), assignment.LeaderID, a.brokerID, protocol.ErrNotPartitionLeader)
	}
	lock := a.partitionLock(key)
	lock.Lock()
	defer lock.Unlock()

	if !assigned {
		return a.appendAndCommitLocally(partition, topicName, value, timestamp)
	}
	switch ack {
	case protocol.AckNone:
		return partition.AppendAt(value, timestamp)
	case protocol.AckLeader:
		return partition.AppendAtSync(value, timestamp)
	case protocol.AckAll:
		return a.appendAndReplicate(partition, assignment, value, timestamp)
	default:
		return 0, protocol.ErrInvalidAckMode
	}
}

func (a *distributedAppender) appendAndCommitLocally(partition *topic.Partition, topicName string, value []byte, timestamp int64) (uint64, error) {
	offset, err := partition.AppendAtSync(value, timestamp)
	if err != nil {
		return offset, err
	}
	_, logEnd, err := partition.OffsetRange()
	if err != nil {
		return offset, err
	}
	state, ok := a.commits.Get(topicName, partition.ID())
	if !ok {
		return offset, replication.ErrInvalidCommitState
	}
	if _, err := a.commits.Advance(topicName, partition.ID(), logEnd, state.LeaderID, state.LeaderEpoch, logEnd); err != nil {
		return offset, err
	}
	return offset, nil
}

func (a *distributedAppender) appendAndReplicate(partition *topic.Partition, assignment partitionReplicaAssignment, value []byte, timestamp int64) (uint64, error) {
	offset, err := partition.AppendAtSync(value, timestamp)
	if err != nil {
		return offset, err
	}
	acknowledgedFollowers := make([]string, 0, len(assignment.Replicas)-1)
	var replicaErrors []error
	for _, followerID := range assignment.Replicas {
		if followerID == a.brokerID {
			continue
		}
		if err := a.replicateFollower(partition, assignment, followerID); err != nil {
			replicaErrors = append(replicaErrors, fmt.Errorf("replica %s: %w", followerID, err))
			continue
		}
		acknowledgedFollowers = append(acknowledgedFollowers, followerID)
	}
	acknowledged := 1 + len(acknowledgedFollowers) // include the leader's durable append
	if acknowledged < a.minISR {
		return offset, errors.Join(append([]error{fmt.Errorf("acknowledged %d replicas, need %d: %w", acknowledged, a.minISR, protocol.ErrInsufficientISR)}, replicaErrors...)...)
	}
	_, logEnd, err := partition.OffsetRange()
	if err != nil {
		return offset, err
	}
	if _, err := a.commits.Advance(assignment.Topic, assignment.PartitionID, logEnd, assignment.LeaderID, assignment.LeaderEpoch, logEnd); err != nil {
		return offset, fmt.Errorf("persist leader high watermark: %w", err)
	}

	// The leader's commit point is durable before followers are told to expose
	// it. Require the same MinISR to persist the commit marker. If this fails,
	// the append outcome is uncertain: the leader has committed locally, but
	// the marker may have reached fewer than MinISR replicas.
	commitAcks := 1
	var commitErrors []error
	for _, followerID := range acknowledgedFollowers {
		if err := a.sendCommitMarker(assignment, followerID, logEnd); err != nil {
			commitErrors = append(commitErrors, fmt.Errorf("commit marker to %s: %w", followerID, err))
			continue
		}
		commitAcks++
	}
	if commitAcks < a.minISR {
		base := fmt.Errorf("commit marker persisted on %d replicas, need %d: %w", commitAcks, a.minISR, protocol.ErrCommitUncertain)
		return offset, errors.Join(append([]error{base}, commitErrors...)...)
	}
	return offset, nil
}

type replicationStatusRequest struct {
	Topic       string `json:"topic"`
	PartitionID uint32 `json:"partition_id"`
}

type replicationStatusResponse struct {
	BrokerID      string `json:"broker_id"`
	StartOffset   uint64 `json:"start_offset"`
	NextOffset    uint64 `json:"next_offset"`
	HighWatermark uint64 `json:"high_watermark"`
	LeaderEpoch   uint64 `json:"leader_epoch"`
}

type replicationCommitRequest struct {
	Topic         string `json:"topic"`
	PartitionID   uint32 `json:"partition_id"`
	LeaderID      string `json:"leader_id"`
	LeaderEpoch   uint64 `json:"leader_epoch"`
	HighWatermark uint64 `json:"high_watermark"`
}

func (a *distributedAppender) replicateFollower(partition *topic.Partition, assignment partitionReplicaAssignment, followerID string) error {
	if a.members == nil {
		return membership.ErrUnknownPeer
	}
	follower, ok := a.members.Broker(followerID)
	if !ok {
		return fmt.Errorf("broker %s is not currently discovered: %w", followerID, membership.ErrUnknownPeer)
	}
	leaderStart, leaderEnd, err := partition.OffsetRange()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	statusPayload, err := json.Marshal(replicationStatusRequest{Topic: assignment.Topic, PartitionID: assignment.PartitionID})
	if err != nil {
		return err
	}
	statusEnvelope, err := a.transport.RoundTrip(ctx, follower.PeerAddress, interbroker.Envelope{
		Type: interbroker.MessageReplicationStatus, RequestID: a.requestID.Add(1),
		SourceID: a.brokerID, TargetID: followerID, Payload: statusPayload,
	})
	if err != nil {
		return err
	}
	var status replicationStatusResponse
	if err := json.Unmarshal(statusEnvelope.Payload, &status); err != nil {
		return fmt.Errorf("decode follower status: %w", err)
	}
	if status.BrokerID != followerID {
		return fmt.Errorf("follower status identifies %q, expected %q", status.BrokerID, followerID)
	}
	if status.LeaderEpoch != assignment.LeaderEpoch {
		return replication.ErrStaleLeaderEpoch
	}
	leaderCommit, exists := a.commits.Get(assignment.Topic, assignment.PartitionID)
	if !exists {
		return replication.ErrInvalidCommitState
	}
	if status.HighWatermark > leaderCommit.HighWatermark {
		return replication.ErrFollowerCommitAhead
	}
	if status.StartOffset > leaderStart || status.NextOffset < leaderStart {
		return replication.ErrReplicaBehindRetention
	}
	if status.NextOffset > leaderEnd {
		return replication.ErrReplicaAheadOfLeader
	}

	// Verify the follower's most recent retained record before extending its log.
	// The follower rejects a duplicate offset whose bytes or timestamp differ.
	if status.NextOffset > leaderStart {
		verifyOffset := status.NextOffset - 1
		if err := a.sendReplicationBatch(ctx, followerID, follower.PeerAddress, assignment, partition, verifyOffset, verifyOffset+1); err != nil {
			return fmt.Errorf("verify follower prefix: %w", err)
		}
	}

	for from := status.NextOffset; from < leaderEnd; {
		_, next, err := buildReplicationBatch(partition, assignment, from, leaderEnd)
		if err != nil {
			return err
		}
		if next <= from {
			return fmt.Errorf("replication made no progress at offset %d", from)
		}
		if err := a.sendReplicationBatch(ctx, followerID, follower.PeerAddress, assignment, partition, from, next); err != nil {
			return err
		}
		from = next
	}
	return nil
}

func (a *distributedAppender) sendCommitMarker(assignment partitionReplicaAssignment, followerID string, highWatermark uint64) error {
	if a.members == nil {
		return membership.ErrUnknownPeer
	}
	follower, ok := a.members.Broker(followerID)
	if !ok {
		return fmt.Errorf("broker %s is not currently discovered: %w", followerID, membership.ErrUnknownPeer)
	}
	payload, err := json.Marshal(replicationCommitRequest{
		Topic: assignment.Topic, PartitionID: assignment.PartitionID,
		LeaderID: assignment.LeaderID, LeaderEpoch: assignment.LeaderEpoch,
		HighWatermark: highWatermark,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := a.transport.RoundTrip(ctx, follower.PeerAddress, interbroker.Envelope{
		Type: interbroker.MessageReplicationCommit, RequestID: a.requestID.Add(1),
		SourceID: a.brokerID, TargetID: followerID, Payload: payload,
	})
	if err != nil {
		return err
	}
	var ack replicationStatusResponse
	if err := json.Unmarshal(response.Payload, &ack); err != nil {
		return fmt.Errorf("decode commit acknowledgement: %w", err)
	}
	if ack.BrokerID != followerID || ack.HighWatermark < highWatermark || ack.LeaderEpoch != assignment.LeaderEpoch {
		return fmt.Errorf("follower %s commit acknowledgement is stale or incomplete: %+v", followerID, ack)
	}
	return nil
}

func buildReplicationBatch(partition *topic.Partition, assignment partitionReplicaAssignment, from, end uint64) (replication.ReplicationBatch, uint64, error) {
	batch := replication.ReplicationBatch{
		Version: replication.ProtocolVersion, Topic: assignment.Topic,
		PartitionID: assignment.PartitionID, LeaderID: replication.ReplicaID(assignment.LeaderID),
		LeaderEpoch: assignment.LeaderEpoch, Records: make([]replication.Record, 0, replicationBatchRecords),
	}
	totalBytes := 0
	next := from
	for next < end && len(batch.Records) < replicationBatchRecords {
		value, timestamp, err := partition.ReadWithTimestamp(next)
		if err != nil {
			return replication.ReplicationBatch{}, from, fmt.Errorf("read leader offset %d: %w", next, err)
		}
		if len(value) > replicationBatchBytes {
			return replication.ReplicationBatch{}, from, replication.ErrInvalidReplicationBatch
		}
		if len(batch.Records) > 0 && totalBytes+len(value) > replicationBatchBytes {
			break
		}
		batch.Records = append(batch.Records, replication.Record{Offset: next, Timestamp: timestamp, Value: value})
		totalBytes += len(value)
		next++
	}
	return batch, next, nil
}

func (a *distributedAppender) sendReplicationBatch(ctx context.Context, followerID, address string, assignment partitionReplicaAssignment, partition *topic.Partition, from, end uint64) error {
	batch, next, err := buildReplicationBatch(partition, assignment, from, end)
	if err != nil {
		return err
	}
	if next != end {
		return fmt.Errorf("internal replication batch range mismatch: built through %d, expected %d", next, end)
	}
	payload, err := replication.EncodeReplicationBatch(batch)
	if err != nil {
		return err
	}
	response, err := a.transport.RoundTrip(ctx, address, interbroker.Envelope{
		Type: interbroker.MessageReplicationBatch, RequestID: a.requestID.Add(1),
		SourceID: a.brokerID, TargetID: followerID, Payload: payload,
	})
	if err != nil {
		return err
	}
	var ack replicationStatusResponse
	if err := json.Unmarshal(response.Payload, &ack); err != nil {
		return fmt.Errorf("decode replication acknowledgement: %w", err)
	}
	if ack.BrokerID != followerID || ack.NextOffset < end {
		return fmt.Errorf("follower %s acknowledged next offset %d, expected at least %d", followerID, ack.NextOffset, end)
	}
	return nil
}

type replicationPeerHandler struct {
	brokerID    string
	members     *membership.Registry
	registry    *topic.Registry
	assignments map[partitionKey]partitionReplicaAssignment
	commits     *replication.CommitStore
}

func newReplicationPeerHandler(brokerID string, members *membership.Registry, registry *topic.Registry, assignments map[partitionKey]partitionReplicaAssignment, commits *replication.CommitStore) (*replicationPeerHandler, error) {
	if brokerID == "" || members == nil || registry == nil || len(assignments) == 0 || commits == nil {
		return nil, errors.New("replication peer handler requires broker identity, membership, assignments and commit store")
	}
	return &replicationPeerHandler{brokerID: brokerID, members: members, registry: registry, assignments: assignments, commits: commits}, nil
}

func (h *replicationPeerHandler) validatePeer(sourceID, targetID string, assignment partitionReplicaAssignment) error {
	if sourceID == "" || sourceID == h.brokerID || targetID != h.brokerID || sourceID != assignment.LeaderID {
		return fmt.Errorf("replication request is not from the configured partition leader")
	}
	if !containsReplica(assignment.Replicas, h.brokerID) {
		return fmt.Errorf("broker %s is not assigned as a replica", h.brokerID)
	}
	if _, ok := h.members.Broker(sourceID); !ok {
		return membership.ErrUnknownPeer
	}
	return nil
}

func containsReplica(replicas []string, id string) bool {
	for _, replica := range replicas {
		if replica == id {
			return true
		}
	}
	return false
}

func (h *replicationPeerHandler) Handle(ctx context.Context, request interbroker.Envelope) (interbroker.Envelope, error) {
	if h == nil || h.registry == nil || request.IsResponse || request.TargetID != h.brokerID {
		return interbroker.Envelope{}, interbroker.ErrInvalidEnvelope
	}
	if _, ok := h.members.Broker(request.SourceID); !ok {
		return interbroker.Envelope{}, membership.ErrUnknownPeer
	}
	switch request.Type {
	case interbroker.MessageReplicationStatus:
		var statusRequest replicationStatusRequest
		if err := json.Unmarshal(request.Payload, &statusRequest); err != nil || statusRequest.Topic == "" {
			return interbroker.Envelope{}, replication.ErrInvalidReplicationBatch
		}
		key := partitionKey{Topic: statusRequest.Topic, PartitionID: statusRequest.PartitionID}
		assignment, ok := h.assignments[key]
		if !ok {
			return interbroker.Envelope{}, fmt.Errorf("no replica assignment for %s:%d", key.Topic, key.PartitionID)
		}
		if err := h.validatePeer(request.SourceID, request.TargetID, assignment); err != nil {
			return interbroker.Envelope{}, err
		}
		registeredTopic, err := h.registry.Get(key.Topic)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		partition, err := registeredTopic.Partition(key.PartitionID)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		start, next, err := partition.OffsetRange()
		if err != nil {
			return interbroker.Envelope{}, err
		}
		commitState, exists := h.commits.Get(key.Topic, key.PartitionID)
		if !exists {
			return interbroker.Envelope{}, replication.ErrInvalidCommitState
		}
		payload, err := json.Marshal(replicationStatusResponse{BrokerID: h.brokerID, StartOffset: start, NextOffset: next, HighWatermark: commitState.HighWatermark, LeaderEpoch: commitState.LeaderEpoch})
		if err != nil {
			return interbroker.Envelope{}, err
		}
		return interbroker.Envelope{Payload: payload}, nil
	case interbroker.MessageReplicationBatch:
		batch, err := replication.DecodeReplicationBatch(request.Payload)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		key := partitionKey{Topic: batch.Topic, PartitionID: batch.PartitionID}
		assignment, ok := h.assignments[key]
		if !ok {
			return interbroker.Envelope{}, fmt.Errorf("no replica assignment for %s:%d", key.Topic, key.PartitionID)
		}
		if err := h.validatePeer(request.SourceID, request.TargetID, assignment); err != nil {
			return interbroker.Envelope{}, err
		}
		if string(batch.LeaderID) != assignment.LeaderID || batch.LeaderEpoch != assignment.LeaderEpoch {
			return interbroker.Envelope{}, replication.ErrStaleLeaderEpoch
		}
		registeredTopic, err := h.registry.Get(key.Topic)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		partition, err := registeredTopic.Partition(key.PartitionID)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		if err := applyReplicationBatch(ctx, partition, batch); err != nil {
			return interbroker.Envelope{}, err
		}
		start, next, err := partition.OffsetRange()
		if err != nil {
			return interbroker.Envelope{}, err
		}
		payload, err := json.Marshal(replicationStatusResponse{BrokerID: h.brokerID, StartOffset: start, NextOffset: next})
		if err != nil {
			return interbroker.Envelope{}, err
		}
		return interbroker.Envelope{Payload: payload}, nil
	case interbroker.MessageReplicationCommit:
		var commitRequest replicationCommitRequest
		if err := json.Unmarshal(request.Payload, &commitRequest); err != nil || commitRequest.Topic == "" || commitRequest.LeaderID == "" || commitRequest.LeaderEpoch == 0 {
			return interbroker.Envelope{}, replication.ErrInvalidCommitState
		}
		key := partitionKey{Topic: commitRequest.Topic, PartitionID: commitRequest.PartitionID}
		assignment, ok := h.assignments[key]
		if !ok {
			return interbroker.Envelope{}, fmt.Errorf("no replica assignment for %s:%d", key.Topic, key.PartitionID)
		}
		if err := h.validatePeer(request.SourceID, request.TargetID, assignment); err != nil {
			return interbroker.Envelope{}, err
		}
		if commitRequest.LeaderID != assignment.LeaderID || commitRequest.LeaderEpoch != assignment.LeaderEpoch {
			return interbroker.Envelope{}, replication.ErrStaleLeaderEpoch
		}
		registeredTopic, err := h.registry.Get(key.Topic)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		partition, err := registeredTopic.Partition(key.PartitionID)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		_, logEnd, err := partition.OffsetRange()
		if err != nil {
			return interbroker.Envelope{}, err
		}
		state, err := h.commits.Advance(key.Topic, key.PartitionID, commitRequest.HighWatermark, commitRequest.LeaderID, commitRequest.LeaderEpoch, logEnd)
		if err != nil {
			return interbroker.Envelope{}, err
		}
		startOffset, _, err := partition.OffsetRange()
		if err != nil {
			return interbroker.Envelope{}, err
		}
		payload, err := json.Marshal(replicationStatusResponse{BrokerID: h.brokerID, StartOffset: startOffset, NextOffset: logEnd, HighWatermark: state.HighWatermark, LeaderEpoch: state.LeaderEpoch})
		if err != nil {
			return interbroker.Envelope{}, err
		}
		return interbroker.Envelope{Payload: payload}, nil
	default:
		return interbroker.Envelope{}, fmt.Errorf("unsupported replication message type %d", request.Type)
	}
}

func applyReplicationBatch(ctx context.Context, partition *topic.Partition, batch replication.ReplicationBatch) error {
	if partition == nil || len(batch.Records) == 0 {
		return replication.ErrInvalidReplicationBatch
	}
	start, next, err := partition.OffsetRange()
	if err != nil {
		return err
	}
	if batch.Records[0].Offset < start {
		return replication.ErrReplicaBehindRetention
	}
	if batch.Records[0].Offset > next {
		return replication.ErrReplicationGap
	}
	for _, record := range batch.Records {
		if err := ctx.Err(); err != nil {
			return err
		}
		start, next, err = partition.OffsetRange()
		if err != nil {
			return err
		}
		if record.Offset < start {
			return replication.ErrReplicaBehindRetention
		}
		if record.Offset < next {
			existing, existingTimestamp, readErr := partition.ReadWithTimestamp(record.Offset)
			if readErr != nil {
				return readErr
			}
			if existingTimestamp != record.Timestamp || !bytes.Equal(existing, record.Value) {
				return replication.ErrDivergentReplica
			}
			continue
		}
		if record.Offset != next {
			return replication.ErrReplicationGap
		}
		appended, appendErr := partition.AppendAtSync(record.Value, record.Timestamp)
		if appendErr != nil {
			return appendErr
		}
		if appended != record.Offset {
			return fmt.Errorf("storage appended offset %d, replication expected %d", appended, record.Offset)
		}
	}
	return nil
}
