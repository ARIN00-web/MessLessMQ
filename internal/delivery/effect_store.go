package delivery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/storage"
)

const (
	effectMagic        = "MLMQEFF1"
	effectHeaderSize   = 8 + 2 + 2 + 4 + 8 + 2
	effectChecksumSize = 4
	maxEffectCount     = 256
	maxEffectBytes     = 10 * 1024 * 1024
)

var (
	ErrEffectStoreClosed        = errors.New("effect store is closed")
	ErrInvalidEventID           = errors.New("invalid processed event identity")
	ErrInvalidEffects           = errors.New("invalid or oversized effect list")
	ErrEffectConflict           = errors.New("event was previously recorded with different effects")
	ErrCorruptEffect            = errors.New("corrupt effect record")
	ErrInvalidEffectStoreConfig = errors.New("invalid effect store configuration")
)

// EventID identifies one input record whose effects are being recorded.
// The group is part of the identity so independent groups can process the
// same underlying message with independent outcomes.
type EventID struct {
	GroupID     string
	Topic       string
	PartitionID uint32
	Offset      uint64
}

// EffectRecord is a durable list of effect intents produced for an input event.
// It is not an acknowledgement from an arbitrary external system.
type EffectRecord struct {
	ID      EventID
	Effects [][]byte
}

// EffectStore records one effect-intent entry per EventID in an append-only
// log. The entry and its deduplication marker are the same durable record.
type EffectStore struct {
	mu      sync.RWMutex
	log     *storage.Log
	dir     string
	records map[EventID]EffectRecord
	closed  bool
}

func OpenEffectStore(dataDir string) (*EffectStore, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, ErrInvalidEffectStoreConfig
	}
	return OpenEffectStoreWithSegmentSize(
		filepath.Join(dataDir, "__processed_effects"),
		storage.DefaultSegmentSize,
	)
}

func OpenEffectStoreWithSegmentSize(directory string, maxSegmentBytes int64) (*EffectStore, error) {
	if strings.TrimSpace(directory) == "" || maxSegmentBytes <= 0 {
		return nil, ErrInvalidEffectStoreConfig
	}
	log, err := storage.OpenLogWithSegmentSize(directory, maxSegmentBytes)
	if err != nil {
		return nil, fmt.Errorf("open effect log: %w", err)
	}
	store := &EffectStore{
		log:     log,
		dir:     directory,
		records: make(map[EventID]EffectRecord),
	}
	if err := store.recoverLocked(); err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("recover effect log: %w", err)
	}
	return store, nil
}

func (s *EffectStore) Directory() string {
	if s == nil {
		return ""
	}
	return s.dir
}

// ApplyOnce durably records effects for an event exactly once in this local
// effect ledger. It returns true when inserted and false for an identical
// previously recorded event. Reusing an EventID with different effects fails.
func (s *EffectStore) ApplyOnce(id EventID, effects [][]byte) (bool, error) {
	if s == nil {
		return false, ErrEffectStoreClosed
	}
	if err := validateEventID(id); err != nil {
		return false, err
	}
	if err := validateEffects(effects); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.log == nil {
		return false, ErrEffectStoreClosed
	}
	if previous, exists := s.records[id]; exists {
		if !equalEffects(previous.Effects, effects) {
			return false, ErrEffectConflict
		}
		return false, nil
	}

	encoded, err := encodeEffectRecord(EffectRecord{ID: id, Effects: cloneEffects(effects)})
	if err != nil {
		return false, err
	}
	if _, err := s.log.AppendSync(encoded); err != nil {
		recoveryErr := s.recoverLocked()
		return false, errors.Join(fmt.Errorf("append effect record: %w", err), recoveryErr)
	}
	s.records[id] = EffectRecord{ID: id, Effects: cloneEffects(effects)}
	return true, nil
}

func (s *EffectStore) Get(id EventID) (EffectRecord, bool, error) {
	if s == nil {
		return EffectRecord{}, false, ErrEffectStoreClosed
	}
	if err := validateEventID(id); err != nil {
		return EffectRecord{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return EffectRecord{}, false, ErrEffectStoreClosed
	}
	record, exists := s.records[id]
	if !exists {
		return EffectRecord{}, false, nil
	}
	return EffectRecord{ID: record.ID, Effects: cloneEffects(record.Effects)}, true, nil
}

func (s *EffectStore) Close() error {
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

func (s *EffectStore) recoverLocked() error {
	s.records = make(map[EventID]EffectRecord)
	if s.log == nil {
		return ErrEffectStoreClosed
	}
	end := s.log.NextOffset()
	for offset := uint64(0); offset < end; offset++ {
		payload, err := s.log.Read(offset)
		if err != nil {
			return fmt.Errorf("read effect journal offset %d: %w", offset, err)
		}
		record, err := decodeEffectRecord(payload)
		if err != nil {
			return fmt.Errorf("%w at journal offset %d: %v", ErrCorruptEffect, offset, err)
		}
		if previous, exists := s.records[record.ID]; exists {
			if !equalEffects(previous.Effects, record.Effects) {
				return fmt.Errorf("%w: conflicting event at journal offset %d", ErrCorruptEffect, offset)
			}
			continue
		}
		s.records[record.ID] = record
	}
	return nil
}

func validateEventID(id EventID) error {
	if !validIdentifier(id.GroupID, 128) || !validIdentifier(id.Topic, 249) {
		return ErrInvalidEventID
	}
	return nil
}

func validIdentifier(value string, max int) bool {
	if len(value) == 0 || len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	return true
}

func validateEffects(effects [][]byte) error {
	if len(effects) > maxEffectCount {
		return ErrInvalidEffects
	}
	total := 0
	for _, effect := range effects {
		total += len(effect)
		if total > maxEffectBytes {
			return ErrInvalidEffects
		}
	}
	return nil
}

func encodeEffectRecord(record EffectRecord) ([]byte, error) {
	if err := validateEventID(record.ID); err != nil {
		return nil, err
	}
	if err := validateEffects(record.Effects); err != nil {
		return nil, err
	}
	if len(record.ID.GroupID) > int(^uint16(0)) || len(record.ID.Topic) > int(^uint16(0)) {
		return nil, ErrInvalidEventID
	}

	total := effectHeaderSize + len(record.ID.GroupID) + len(record.ID.Topic) + effectChecksumSize
	for _, effect := range record.Effects {
		if uint64(len(effect)) > uint64(^uint32(0)) {
			return nil, ErrInvalidEffects
		}
		total += 4 + len(effect)
	}
	data := make([]byte, total)
	copy(data[:8], effectMagic)
	binary.BigEndian.PutUint16(data[8:10], uint16(len(record.ID.GroupID)))
	binary.BigEndian.PutUint16(data[10:12], uint16(len(record.ID.Topic)))
	binary.BigEndian.PutUint32(data[12:16], record.ID.PartitionID)
	binary.BigEndian.PutUint64(data[16:24], record.ID.Offset)
	binary.BigEndian.PutUint16(data[24:26], uint16(len(record.Effects)))
	cursor := effectHeaderSize
	copy(data[cursor:], record.ID.GroupID)
	cursor += len(record.ID.GroupID)
	copy(data[cursor:], record.ID.Topic)
	cursor += len(record.ID.Topic)
	for _, effect := range record.Effects {
		binary.BigEndian.PutUint32(data[cursor:cursor+4], uint32(len(effect)))
		cursor += 4
		copy(data[cursor:], effect)
		cursor += len(effect)
	}
	binary.BigEndian.PutUint32(data[cursor:], crc32.ChecksumIEEE(data[:cursor]))
	return data, nil
}

func decodeEffectRecord(data []byte) (EffectRecord, error) {
	if len(data) < effectHeaderSize+effectChecksumSize || string(data[:8]) != effectMagic {
		return EffectRecord{}, ErrCorruptEffect
	}
	groupLength := int(binary.BigEndian.Uint16(data[8:10]))
	topicLength := int(binary.BigEndian.Uint16(data[10:12]))
	partitionID := binary.BigEndian.Uint32(data[12:16])
	offset := binary.BigEndian.Uint64(data[16:24])
	effectCount := int(binary.BigEndian.Uint16(data[24:26]))
	if groupLength == 0 || topicLength == 0 || effectCount > maxEffectCount {
		return EffectRecord{}, ErrCorruptEffect
	}
	cursor := effectHeaderSize
	if groupLength+topicLength > len(data)-cursor-effectChecksumSize {
		return EffectRecord{}, ErrCorruptEffect
	}
	groupID := string(data[cursor : cursor+groupLength])
	cursor += groupLength
	topicName := string(data[cursor : cursor+topicLength])
	cursor += topicLength

	record := EffectRecord{
		ID:      EventID{GroupID: groupID, Topic: topicName, PartitionID: partitionID, Offset: offset},
		Effects: make([][]byte, 0, effectCount),
	}
	totalEffectBytes := 0
	checksumOffset := len(data) - effectChecksumSize
	for i := 0; i < effectCount; i++ {
		if checksumOffset-cursor < 4 {
			return EffectRecord{}, ErrCorruptEffect
		}
		length := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if length < 0 || length > checksumOffset-cursor {
			return EffectRecord{}, ErrCorruptEffect
		}
		totalEffectBytes += length
		if totalEffectBytes > maxEffectBytes {
			return EffectRecord{}, ErrCorruptEffect
		}
		record.Effects = append(record.Effects, append([]byte(nil), data[cursor:cursor+length]...))
		cursor += length
	}
	if cursor != checksumOffset {
		return EffectRecord{}, ErrCorruptEffect
	}
	want := binary.BigEndian.Uint32(data[checksumOffset:])
	if want != crc32.ChecksumIEEE(data[:checksumOffset]) {
		return EffectRecord{}, ErrCorruptEffect
	}
	if err := validateEventID(record.ID); err != nil {
		return EffectRecord{}, err
	}
	return record, nil
}

func cloneEffects(effects [][]byte) [][]byte {
	cloned := make([][]byte, len(effects))
	for i := range effects {
		cloned[i] = append([]byte(nil), effects[i]...)
	}
	return cloned
}

func equalEffects(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
