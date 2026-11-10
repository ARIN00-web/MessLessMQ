// Package offsets implements durable consumer offset storage using the
// broker's internal __consumer_offsets append-only log.
package offsets

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

const (
	// InternalTopicName is the reserved internal log directory used to persist
	// consumer-group offsets. It is not exposed as a user topic.
	InternalTopicName = "__consumer_offsets"

	recordVersion    byte = 1
	recordKindCommit byte = 1
	recordKindReset  byte = 2

	recordHeaderSize  = 21
	maxGroupIDLength  = 128
	maxTopicLength    = 249
	maxMetadataLength = 1024
)

var (
	ErrInvalidKey       = errors.New("invalid offset key")
	ErrInvalidMetadata  = errors.New("offset metadata is too large")
	ErrOffsetRegression = errors.New("committed offset cannot move backwards without reset")
	ErrCorruptRecord    = errors.New("corrupt consumer offset record")
	ErrStoreClosed      = errors.New("consumer offset store is closed")
	ErrInvalidConfig    = errors.New("invalid consumer offset store configuration")
)

// Key identifies one committed position for a consumer group and topic
// partition. Partition IDs are scoped to their topic.
type Key struct {
	GroupID     string
	Topic       string
	PartitionID uint32
}

// Commit stores the next offset the group should consume for a partition.
// NextOffset is the first record that has not yet been acknowledged as
// processed; it is not the offset of the last processed record.
type Commit struct {
	Key         Key
	NextOffset  uint64
	Metadata    string
	CommittedAt time.Time
	WasReset    bool
}

// Store persists offset commit records in an append-only internal log.
// The latest record for each Key is the current committed state.
type Store struct {
	mu        sync.RWMutex
	log       *storage.Log
	directory string
	committed map[Key]Commit
	closed    bool
}

// Open creates or opens the reserved __consumer_offsets log below dataDir.
// For example, Open("./data") stores offsets under ./data/__consumer_offsets.
func Open(dataDir string) (*Store, error) {
	return OpenWithSegmentSize(dataDir, storage.DefaultSegmentSize)
}

// OpenWithSegmentSize opens the internal offsets log with the supplied
// maximum segment size. The value must be positive.
func OpenWithSegmentSize(dataDir string, maxSegmentBytes int64) (*Store, error) {
	if strings.TrimSpace(dataDir) == "" || maxSegmentBytes <= 0 {
		return nil, ErrInvalidConfig
	}

	directory := filepath.Join(dataDir, InternalTopicName)
	log, err := storage.OpenLogWithSegmentSize(directory, maxSegmentBytes)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", InternalTopicName, err)
	}

	store := &Store{
		log:       log,
		directory: directory,
		committed: make(map[Key]Commit),
	}

	if err := store.recover(); err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("recover %s: %w", InternalTopicName, err)
	}

	return store, nil
}

// Directory returns the physical directory containing the internal offsets
// log. It is primarily useful for diagnostics and tests.
func (s *Store) Directory() string {
	if s == nil {
		return ""
	}
	return s.directory
}

// Commit durably stores a next-to-read offset. Normal commits are monotonic;
// use Reset when an administrator intentionally wants to rewind a group.
func (s *Store) Commit(key Key, nextOffset uint64) error {
	return s.CommitWithMetadata(key, nextOffset, "")
}

// CommitWithMetadata durably stores a next-to-read offset and optional
// application metadata. The log append is synchronized before in-memory
// state is updated.
func (s *Store) CommitWithMetadata(key Key, nextOffset uint64, metadata string) error {
	return s.appendState(recordKindCommit, key, nextOffset, metadata)
}

// Reset intentionally replaces the committed position, including moving it
// backwards. The caller must validate the requested offset against the
// partition's retained range before invoking Reset.
func (s *Store) Reset(key Key, nextOffset uint64, reason string) error {
	return s.appendState(recordKindReset, key, nextOffset, reason)
}

func (s *Store) appendState(kind byte, key Key, nextOffset uint64, metadata string) error {
	if s == nil {
		return ErrStoreClosed
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if len(metadata) > maxMetadataLength {
		return ErrInvalidMetadata
	}

	payload, err := encodeRecord(kind, key, nextOffset, metadata)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.log == nil {
		return ErrStoreClosed
	}

	previous, exists := s.committed[key]
	if kind == recordKindCommit && exists {
		if nextOffset < previous.NextOffset {
			return fmt.Errorf(
				"%w: previous=%d requested=%d",
				ErrOffsetRegression,
				previous.NextOffset,
				nextOffset,
			)
		}
		if nextOffset == previous.NextOffset && metadata == previous.Metadata && !previous.WasReset {
			return nil
		}
	}

	timestamp := time.Now().UnixMilli()
	if _, err := s.log.AppendSync(payload); err != nil {
		// A sync error can leave the caller uncertain whether the record reached
		// durable storage. Recovery replays the authoritative log on next open.
		return fmt.Errorf("append durable offset record: %w", err)
	}

	s.committed[key] = Commit{
		Key:         key,
		NextOffset:  nextOffset,
		Metadata:    metadata,
		CommittedAt: time.UnixMilli(timestamp),
		WasReset:    kind == recordKindReset,
	}
	return nil
}

// Get returns the latest committed state for a key.
func (s *Store) Get(key Key) (Commit, bool, error) {
	if s == nil {
		return Commit{}, false, ErrStoreClosed
	}
	if err := validateKey(key); err != nil {
		return Commit{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Commit{}, false, ErrStoreClosed
	}
	commit, exists := s.committed[key]
	return commit, exists, nil
}

// GetOffset returns only the next offset for a key.
func (s *Store) GetOffset(key Key) (uint64, bool, error) {
	commit, exists, err := s.Get(key)
	if err != nil || !exists {
		return 0, exists, err
	}
	return commit.NextOffset, true, nil
}

// ListGroup returns all committed offsets for a group in deterministic
// topic/partition order.
func (s *Store) ListGroup(groupID string) ([]Commit, error) {
	if s == nil {
		return nil, ErrStoreClosed
	}
	if err := validateIdentifier(groupID, maxGroupIDLength); err != nil {
		return nil, fmt.Errorf("%w: group ID: %v", ErrInvalidKey, err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	result := make([]Commit, 0)
	for key, commit := range s.committed {
		if key.GroupID == groupID {
			result = append(result, commit)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Key.Topic != result[j].Key.Topic {
			return result[i].Key.Topic < result[j].Key.Topic
		}
		return result[i].Key.PartitionID < result[j].Key.PartitionID
	})
	return result, nil
}

// Sync flushes the internal log's segment and index files.
func (s *Store) Sync() error {
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.log == nil {
		return ErrStoreClosed
	}
	return s.log.Sync()
}

// Close closes the internal offsets log. It is safe to call more than once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.log == nil {
		return nil
	}
	err := s.log.Close()
	s.log = nil
	return err
}

// recover rebuilds the latest committed state by replaying the authoritative
// append-only __consumer_offsets log from the beginning.
func (s *Store) recover() error {
	end := s.log.NextOffset()
	for offset := uint64(0); offset < end; offset++ {
		payload, timestamp, err := s.log.ReadWithTimestamp(offset)
		if err != nil {
			return fmt.Errorf("read offset record %d: %w", offset, err)
		}

		kind, key, nextOffset, metadata, err := decodeRecord(payload)
		if err != nil {
			return fmt.Errorf("%w at log offset %d: %v", ErrCorruptRecord, offset, err)
		}

		previous, exists := s.committed[key]
		if kind == recordKindCommit && exists && nextOffset < previous.NextOffset {
			return fmt.Errorf(
				"%w at log offset %d: commit regressed from %d to %d without reset",
				ErrCorruptRecord,
				offset,
				previous.NextOffset,
				nextOffset,
			)
		}

		s.committed[key] = Commit{
			Key:         key,
			NextOffset:  nextOffset,
			Metadata:    metadata,
			CommittedAt: time.UnixMilli(timestamp),
			WasReset:    kind == recordKindReset,
		}
	}
	return nil
}

func validateKey(key Key) error {
	if err := validateIdentifier(key.GroupID, maxGroupIDLength); err != nil {
		return fmt.Errorf("%w: group ID: %v", ErrInvalidKey, err)
	}
	if err := validateTopicName(key.Topic); err != nil {
		return fmt.Errorf("%w: topic: %v", ErrInvalidKey, err)
	}
	return nil
}

func validateIdentifier(value string, max int) error {
	if len(value) == 0 || len(value) > max {
		return errors.New("invalid identifier length")
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		valid := (ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '.' || ch == '_' || ch == '-'
		if !valid {
			return fmt.Errorf("invalid identifier character %q", ch)
		}
	}
	return nil
}

func validateTopicName(value string) error {
	return validateIdentifier(value, maxTopicLength)
}

func encodeRecord(kind byte, key Key, nextOffset uint64, metadata string) ([]byte, error) {
	if kind != recordKindCommit && kind != recordKindReset {
		return nil, ErrCorruptRecord
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if len(metadata) > maxMetadataLength {
		return nil, ErrInvalidMetadata
	}

	groupLen := len(key.GroupID)
	topicLen := len(key.Topic)
	data := make([]byte, recordHeaderSize+groupLen+topicLen+len(metadata))
	data[0] = recordVersion
	data[1] = kind
	binary.BigEndian.PutUint16(data[2:4], uint16(groupLen))
	binary.BigEndian.PutUint16(data[4:6], uint16(topicLen))
	binary.BigEndian.PutUint32(data[6:10], key.PartitionID)
	binary.BigEndian.PutUint64(data[10:18], nextOffset)
	binary.BigEndian.PutUint16(data[18:20], uint16(len(metadata)))
	// Byte 20 is reserved for future record-format flags and must remain zero.

	cursor := recordHeaderSize
	copy(data[cursor:], key.GroupID)
	cursor += groupLen
	copy(data[cursor:], key.Topic)
	cursor += topicLen
	copy(data[cursor:], metadata)
	return data, nil
}

func decodeRecord(data []byte) (byte, Key, uint64, string, error) {
	if len(data) < recordHeaderSize {
		return 0, Key{}, 0, "", errors.New("record header is truncated")
	}
	if data[0] != recordVersion {
		return 0, Key{}, 0, "", fmt.Errorf("unsupported record version %d", data[0])
	}
	kind := data[1]
	if kind != recordKindCommit && kind != recordKindReset {
		return 0, Key{}, 0, "", fmt.Errorf("unknown record kind %d", kind)
	}
	if data[20] != 0 {
		return 0, Key{}, 0, "", errors.New("reserved flags byte is non-zero")
	}

	groupLen := int(binary.BigEndian.Uint16(data[2:4]))
	topicLen := int(binary.BigEndian.Uint16(data[4:6]))
	partitionID := binary.BigEndian.Uint32(data[6:10])
	nextOffset := binary.BigEndian.Uint64(data[10:18])
	metadataLen := int(binary.BigEndian.Uint16(data[18:20]))

	if groupLen == 0 || groupLen > maxGroupIDLength || topicLen == 0 || topicLen > maxTopicLength || metadataLen > maxMetadataLength {
		return 0, Key{}, 0, "", errors.New("record contains invalid field length")
	}
	expected := recordHeaderSize + groupLen + topicLen + metadataLen
	if len(data) != expected {
		return 0, Key{}, 0, "", fmt.Errorf("record length mismatch: got=%d want=%d", len(data), expected)
	}

	cursor := recordHeaderSize
	key := Key{GroupID: string(data[cursor : cursor+groupLen])}
	cursor += groupLen
	key.Topic = string(data[cursor : cursor+topicLen])
	cursor += topicLen
	key.PartitionID = partitionID
	metadata := string(data[cursor : cursor+metadataLen])

	if err := validateKey(key); err != nil {
		return 0, Key{}, 0, "", err
	}
	return kind, key, nextOffset, metadata, nil
}

// LogEndOffset returns the next physical record offset in __consumer_offsets.
// It is intended for diagnostics and tests, not as a consumer message offset.
func (s *Store) LogEndOffset() (uint64, error) {
	if s == nil {
		return 0, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.log == nil {
		return 0, ErrStoreClosed
	}
	return s.log.NextOffset(), nil
}
