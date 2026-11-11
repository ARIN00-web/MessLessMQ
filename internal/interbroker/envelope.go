package interbroker

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	ProtocolVersion    uint16 = 1
	MaxPeerPayload            = 32 * 1024 * 1024
	MaxPeerFrame              = MaxPeerPayload + 1024
	peerHeaderSize            = 23
	maxPeerIDLength           = 128
	maxPeerErrorLength        = 1024
)

type MessageType uint16

const (
	MessageVoteRequest MessageType = iota + 1
	MessageAppendEntries
	MessageReplicationBatch
	MessageHeartbeat
	MessageBrokerJoin
	MessageReplicationStatus
)

var (
	ErrInvalidEnvelope = errors.New("interbroker: invalid envelope")
	ErrFrameTooLarge   = errors.New("interbroker: frame too large")
	ErrUnexpectedReply = errors.New("interbroker: unexpected response")
)

// Envelope is the common wrapper for controller RPCs, replication batches,
// heartbeats, and future broker-to-broker control messages.
type Envelope struct {
	Version    uint16
	Type       MessageType
	RequestID  uint64
	SourceID   string
	TargetID   string
	IsResponse bool
	Error      string
	Payload    []byte
}

func EncodeEnvelope(envelope Envelope) ([]byte, error) {
	if envelope.Version == 0 {
		envelope.Version = ProtocolVersion
	}
	if envelope.Version != ProtocolVersion || envelope.Type == 0 || envelope.RequestID == 0 ||
		envelope.SourceID == "" || envelope.TargetID == "" ||
		len(envelope.SourceID) > maxPeerIDLength || len(envelope.TargetID) > maxPeerIDLength ||
		len(envelope.Error) > maxPeerErrorLength || len(envelope.Payload) > MaxPeerPayload {
		return nil, ErrInvalidEnvelope
	}
	if envelope.IsResponse && envelope.Error != "" && len(envelope.Payload) != 0 {
		return nil, ErrInvalidEnvelope
	}
	if !envelope.IsResponse && envelope.Error != "" {
		return nil, ErrInvalidEnvelope
	}

	total := peerHeaderSize + len(envelope.SourceID) + len(envelope.TargetID) + len(envelope.Error) + len(envelope.Payload)
	if total > MaxPeerFrame {
		return nil, ErrFrameTooLarge
	}
	data := make([]byte, total)
	binary.BigEndian.PutUint16(data[0:2], envelope.Version)
	binary.BigEndian.PutUint16(data[2:4], uint16(envelope.Type))
	binary.BigEndian.PutUint64(data[4:12], envelope.RequestID)
	if envelope.IsResponse {
		data[12] = 1
	}
	binary.BigEndian.PutUint16(data[13:15], uint16(len(envelope.SourceID)))
	binary.BigEndian.PutUint16(data[15:17], uint16(len(envelope.TargetID)))
	binary.BigEndian.PutUint16(data[17:19], uint16(len(envelope.Error)))
	binary.BigEndian.PutUint32(data[19:23], uint32(len(envelope.Payload)))
	cursor := peerHeaderSize
	copy(data[cursor:], envelope.SourceID)
	cursor += len(envelope.SourceID)
	copy(data[cursor:], envelope.TargetID)
	cursor += len(envelope.TargetID)
	copy(data[cursor:], envelope.Error)
	cursor += len(envelope.Error)
	copy(data[cursor:], envelope.Payload)
	return data, nil
}

func DecodeEnvelope(data []byte) (Envelope, error) {
	if len(data) < peerHeaderSize || len(data) > MaxPeerFrame {
		return Envelope{}, ErrInvalidEnvelope
	}
	version := binary.BigEndian.Uint16(data[0:2])
	typeID := MessageType(binary.BigEndian.Uint16(data[2:4]))
	requestID := binary.BigEndian.Uint64(data[4:12])
	flags := data[12]
	sourceLength := int(binary.BigEndian.Uint16(data[13:15]))
	targetLength := int(binary.BigEndian.Uint16(data[15:17]))
	errorLength := int(binary.BigEndian.Uint16(data[17:19]))
	payloadLength := uint64(binary.BigEndian.Uint32(data[19:23]))
	if version != ProtocolVersion || typeID == 0 || requestID == 0 || flags > 1 ||
		sourceLength == 0 || sourceLength > maxPeerIDLength ||
		targetLength == 0 || targetLength > maxPeerIDLength ||
		errorLength > maxPeerErrorLength || payloadLength > MaxPeerPayload {
		return Envelope{}, ErrInvalidEnvelope
	}
	expected := uint64(peerHeaderSize+sourceLength+targetLength+errorLength) + payloadLength
	if expected != uint64(len(data)) {
		return Envelope{}, ErrInvalidEnvelope
	}
	cursor := peerHeaderSize
	source := string(data[cursor : cursor+sourceLength])
	cursor += sourceLength
	target := string(data[cursor : cursor+targetLength])
	cursor += targetLength
	remoteError := string(data[cursor : cursor+errorLength])
	cursor += errorLength
	payload := append([]byte(nil), data[cursor:]...)
	if source == "" || target == "" || (flags == 1 && remoteError != "" && len(payload) != 0) || (flags == 0 && remoteError != "") {
		return Envelope{}, fmt.Errorf("invalid envelope fields: %w", ErrInvalidEnvelope)
	}
	return Envelope{
		Version: version, Type: typeID, RequestID: requestID,
		SourceID: source, TargetID: target, IsResponse: flags == 1,
		Error: remoteError, Payload: payload,
	}, nil
}
